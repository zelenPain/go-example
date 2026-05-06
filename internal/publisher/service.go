package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"go-example/internal/db"
	"go-example/internal/model"
	"go-example/internal/queue"
	"go-example/internal/segment"
)

type Service struct {
	repo     *db.Repository
	segments segment.Store
	queue    queue.Client
}

func NewService(repo *db.Repository, segments segment.Store, queue queue.Client) Service {
	return Service{repo: repo, segments: segments, queue: queue}
}

func (s Service) PublishNextCampaign(ctx context.Context) error {
	return s.publishNextCampaign(ctx, 0)
}

func (s Service) PublishNextCampaignInBatches(ctx context.Context, batchSize int) error {
	if batchSize <= 0 {
		return fmt.Errorf("batch size must be greater than 0")
	}
	return s.publishNextCampaign(ctx, batchSize)
}

func (s Service) publishNextCampaign(ctx context.Context, batchSize int) error {
	// Each publisher run picks only the first pending campaign.
	// No pending campaign is treated as a successful no-op for demo usage.
	campaign, err := s.repo.NextPendingCampaign(ctx)
	if err != nil {
		return err
	}
	if campaign == nil {
		log.Println("publisher: no pending campaign")
		return nil
	}

	// TODO: Add a recovery path for campaigns stuck in processing if the publisher
	// crashes after NextPendingCampaign and before FailCampaign/CompleteCampaign.
	if err := s.publishCampaign(ctx, *campaign, batchSize); err != nil {
		_ = s.repo.FailCampaign(ctx, campaign.ID, err)
		return err
	}
	return s.repo.CompleteCampaign(ctx, campaign.ID)
}

func (s Service) publishCampaign(ctx context.Context, campaign db.Campaign, batchSize int) error {
	users, err := s.segments.GetUsers(ctx, campaign.SegmentFileKey)
	if err != nil {
		return fmt.Errorf("get segment users: %w", err)
	}

	published := 0
	// Batch mode only chunks active users for throughput/retry demos. The segment
	// is still read from S3 in one pass because the current file is a small JSON array.
	activeUsers := activeSegmentUsers(users)
	batches := segmentUserBatches(activeUsers, batchSize)
	for index, batch := range batches {
		batchPublished, err := s.publishBatch(ctx, campaign, batch)
		if err != nil {
			return err
		}
		published += batchPublished
		if batchSize > 0 {
			log.Printf("publisher: campaign_id=%d batch=%d/%d batch_size=%d published=%d", campaign.ID, index+1, len(batches), len(batch), batchPublished)
		}
	}

	log.Printf("publisher: campaign_id=%d published=%d", campaign.ID, published)
	return nil
}

func (s Service) publishBatch(ctx context.Context, campaign db.Campaign, users []model.SegmentUser) (int, error) {
	published := 0
	for _, user := range users {
		queueMessage := model.QueueMessage{
			CampaignID:     campaign.ID,
			UserID:         user.UserID,
			LineUserID:     user.LineUserID,
			Text:           fmt.Sprintf("Training message from campaign %s", campaign.Name),
			IdempotencyKey: fmt.Sprintf("campaign:%d:user:%s", campaign.ID, user.UserID),
		}
		payload, err := json.Marshal(queueMessage)
		if err != nil {
			return 0, err
		}

		outboxID, inserted, err := s.repo.InsertOutbox(ctx, db.OutboxMessage{
			CampaignID:     campaign.ID,
			UserID:         user.UserID,
			LineUserID:     user.LineUserID,
			IdempotencyKey: queueMessage.IdempotencyKey,
			Payload:        payload,
		})
		if err != nil {
			return 0, fmt.Errorf("insert outbox: %w", err)
		}
		if !inserted {
			status, err := s.repo.OutboxStatus(ctx, outboxID)
			if err != nil {
				return 0, fmt.Errorf("get outbox status: %w", err)
			}
			// On publisher retry, do not send messages that already reached SQS or
			// were consumed. Subscriber still has dedupe, but skipping here makes demos clearer.
			if status == "sent_to_sqs" || status == "consumed" {
				log.Printf("publisher: skip already published outbox_id=%d status=%s", outboxID, status)
				continue
			}
		}

		// TODO: Add an outbox replay/recovery job for rows left as pending if the
		// publisher crashes after InsertOutbox and before Send/MarkSentToSQS.
		queueMessage.OutboxID = outboxID
		sqsMessageID, err := s.queue.Send(ctx, queueMessage)
		if err != nil {
			return 0, fmt.Errorf("send sqs: %w", err)
		}
		if err := s.repo.MarkSentToSQS(ctx, outboxID, sqsMessageID); err != nil {
			return 0, fmt.Errorf("mark sent to sqs: %w", err)
		}
		published++
	}
	return published, nil
}

func activeSegmentUsers(users []model.SegmentUser) []model.SegmentUser {
	activeUsers := make([]model.SegmentUser, 0, len(users))
	for _, user := range users {
		if user.Active {
			activeUsers = append(activeUsers, user)
		}
	}
	return activeUsers
}

func segmentUserBatches(users []model.SegmentUser, batchSize int) [][]model.SegmentUser {
	if batchSize <= 0 || batchSize >= len(users) {
		return [][]model.SegmentUser{users}
	}

	batches := make([][]model.SegmentUser, 0, (len(users)+batchSize-1)/batchSize)
	for start := 0; start < len(users); start += batchSize {
		end := start + batchSize
		if end > len(users) {
			end = len(users)
		}
		batches = append(batches, users[start:end])
	}
	return batches
}
