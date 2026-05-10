package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"go-example/internal/db"
	"go-example/internal/model"
	"go-example/internal/queue"
	"go-example/internal/segment"
)

type Service struct {
	repo         *db.Repository
	segments     segment.Store
	queue        queue.Client
	pollInterval time.Duration
	workers      int
	claimLimit   int
}

func NewService(repo *db.Repository, segments segment.Store, queue queue.Client, pollInterval time.Duration, workers int, claimLimit int) Service {
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	if workers <= 0 {
		workers = 1
	}
	if claimLimit <= 0 {
		claimLimit = workers
	}
	return Service{
		repo:         repo,
		segments:     segments,
		queue:        queue,
		pollInterval: pollInterval,
		workers:      workers,
		claimLimit:   claimLimit,
	}
}

func (s Service) Run(ctx context.Context) error {
	// Run one poll immediately so the publisher does not wait for the first tick
	// when there are already pending campaigns.
	if err := s.claimAndProcess(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	// Poll forever until the process is stopped. Each tick picks a small batch
	// of pending campaigns and processes them before waiting for the next tick.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.claimAndProcess(ctx); err != nil {
				return err
			}
		}
	}
}

func (s Service) claimAndProcess(ctx context.Context) error {
	// Step 1: get at most claimLimit pending campaigns from MySQL.
	campaigns, err := s.repo.PendingCampaigns(ctx, s.claimLimit)
	if err != nil {
		return err
	}
	if len(campaigns) == 0 {
		log.Println("publisher: no pending campaigns")
		return nil
	}

	// Step 2: mark each campaign as processing before starting goroutines.
	// This makes the campaign state visible in DB while it is being published.
	processing := make([]db.Campaign, 0, len(campaigns))
	for _, campaign := range campaigns {
		if err := s.repo.MarkCampaignProcessing(ctx, campaign.ID); err != nil {
			log.Printf("publisher: mark campaign processing failed campaign_id=%d: %v", campaign.ID, err)
			continue
		}
		campaign.Status = "processing"
		processing = append(processing, campaign)
	}
	if len(processing) == 0 {
		return nil
	}

	// Step 3: process the marked campaigns with a bounded worker pool.
	log.Printf("publisher: processing=%d workers=%d", len(processing), s.workers)
	s.handleCampaigns(ctx, processing)
	return nil
}

func (s Service) handleCampaigns(ctx context.Context, campaigns []db.Campaign) {
	var wg sync.WaitGroup
	// sem limits concurrent campaign processing. For example, workers=4 means
	// only four campaigns can read S3 and send SQS messages at the same time.
	sem := make(chan struct{}, s.workers)

spawn:
	for _, campaign := range campaigns {
		select {
		case <-ctx.Done():
			break spawn
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(campaign db.Campaign) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := s.processCampaign(ctx, campaign); err != nil {
				log.Printf("publisher: campaign_id=%d failed: %v", campaign.ID, err)
			}
		}(campaign)
	}

	wg.Wait()
}

func (s Service) processCampaign(ctx context.Context, campaign db.Campaign) error {
	// TODO: Add a recovery path for campaigns stuck in processing if the publisher
	// crashes after MarkCampaignProcessing and before FailCampaign/CompleteCampaign.
	// Each campaign owns its final status. One failed campaign should not stop
	// other campaign goroutines from completing.
	if err := s.publishCampaign(ctx, campaign); err != nil {
		_ = s.repo.FailCampaign(ctx, campaign.ID, err)
		return err
	}
	return s.repo.CompleteCampaign(ctx, campaign.ID)
}

func (s Service) publishCampaign(ctx context.Context, campaign db.Campaign) error {
	// The segment file contains target users for this campaign.
	users, err := s.segments.GetUsers(ctx, campaign.SegmentFileKey)
	if err != nil {
		return fmt.Errorf("get segment users: %w", err)
	}

	// Inactive users are intentionally skipped before writing outbox rows.
	activeUsers := activeSegmentUsers(users)
	published, err := s.publishUsers(ctx, campaign, activeUsers)
	if err != nil {
		return err
	}

	log.Printf("publisher: campaign_id=%d published=%d", campaign.ID, published)
	return nil
}

func (s Service) publishUsers(ctx context.Context, campaign db.Campaign, users []model.SegmentUser) (int, error) {
	published := 0
	for _, user := range users {
		// Keep idempotency deterministic. Retrying the same campaign/user creates
		// the same key, so MySQL can prevent duplicate outbox rows.
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
		// Store the outbox id in the SQS body so the subscriber/audit path can
		// trace the queue message back to its database row.
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
