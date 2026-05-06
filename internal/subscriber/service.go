package subscriber

import (
	"context"
	"log"
	"sync"
	"time"

	"go-example/internal/audit"
	"go-example/internal/db"
	"go-example/internal/dedup"
	"go-example/internal/line"
	"go-example/internal/queue"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Service struct {
	repo       *db.Repository
	dedup      dedup.Store
	queue      queue.Client
	line       line.Client
	audit      audit.Logger
	maxRetry   int
	waitSecond int
	workers    int
}

func NewService(repo *db.Repository, dedup dedup.Store, queue queue.Client, line line.Client, audit audit.Logger, maxRetry int, waitSecond int, workers int) Service {
	if workers <= 0 {
		workers = 1
	}
	return Service{
		repo:       repo,
		dedup:      dedup,
		queue:      queue,
		line:       line,
		audit:      audit,
		maxRetry:   maxRetry,
		waitSecond: waitSecond,
		workers:    workers,
	}
}

func (s Service) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		messages, err := s.queue.Receive(ctx, s.waitSecond)
		if err != nil {
			return err
		}
		s.handleMessages(ctx, messages)
	}
}

func (s Service) handleMessages(ctx context.Context, messages []types.Message) {
	if len(messages) == 0 {
		return
	}

	// Process the current SQS receive batch concurrently, capped by workers.
	// SQS returns up to 10 messages per receive call, so this keeps concurrency bounded.
	var wg sync.WaitGroup
	sem := make(chan struct{}, s.workers)
	for _, raw := range messages {
		select {
		case <-ctx.Done():
			return
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(raw types.Message) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := s.handle(ctx, raw); err != nil {
				log.Printf("subscriber: handle failed: %v", err)
			}
		}(raw)
	}
	wg.Wait()
}

func (s Service) handle(ctx context.Context, raw types.Message) error {
	msg, err := queue.DecodeMessage(raw)
	if err != nil {
		return err
	}

	receiveCount := queue.ReceiveCount(raw)
	// When max retry is exceeded, do not delete the message. SQS redrive policy
	// will move it to the DLQ after the visibility timeout and receive count match.
	if receiveCount > s.maxRetry {
		_ = s.repo.MarkFailed(ctx, msg.IdempotencyKey, receiveCount, errMaxRetry(receiveCount))
		_ = s.audit.Log(ctx, msg, "max_retry_waiting_for_dlq", receiveCount)
		return nil
	}

	// Redis processed marker is the fast path for duplicate SQS deliveries.
	// If Redis is down, processing continues and falls back to MySQL.
	processed, err := s.dedup.IsProcessed(ctx, msg.IdempotencyKey)
	if err != nil {
		log.Printf("subscriber: redis processed check failed: %v", err)
	}
	if processed {
		return s.queue.Delete(ctx, aws.ToString(raw.ReceiptHandle))
	}

	// MySQL is the source of truth. This check prevents duplicate LINE calls even
	// when Redis lost data or is currently unavailable.
	consumed, err := s.repo.AlreadyConsumed(ctx, msg.IdempotencyKey)
	if err != nil {
		return err
	}
	if consumed {
		_ = s.dedup.MarkProcessed(ctx, msg.IdempotencyKey, 24*time.Hour)
		return s.queue.Delete(ctx, aws.ToString(raw.ReceiptHandle))
	}

	// Short-lived lock prevents two workers from processing the same idempotency key concurrently.
	locked, err := s.dedup.AcquireProcessingLock(ctx, msg.IdempotencyKey, 2*time.Minute)
	if err != nil {
		log.Printf("subscriber: redis lock failed: %v", err)
	} else if !locked {
		log.Printf("subscriber: duplicate in-flight message idempotency_key=%s", msg.IdempotencyKey)
		return nil
	} else {
		defer s.dedup.ReleaseProcessingLock(ctx, msg.IdempotencyKey)
	}

	if err := s.line.Send(ctx, line.Message{To: msg.LineUserID, Text: msg.Text}); err != nil {
		_ = s.repo.MarkFailed(ctx, msg.IdempotencyKey, receiveCount, err)
		_ = s.audit.Log(ctx, msg, "failed", receiveCount)
		return err
	}

	// TODO: Add stronger external idempotency or a send log for the case where
	// LINE succeeds but the subscriber crashes before MarkConsumed/DeleteMessage.
	if err := s.repo.MarkConsumed(ctx, msg.IdempotencyKey); err != nil {
		return err
	}
	if err := s.dedup.MarkProcessed(ctx, msg.IdempotencyKey, 24*time.Hour); err != nil {
		log.Printf("subscriber: redis mark processed failed: %v", err)
	}
	if err := s.audit.Log(ctx, msg, "consumed", receiveCount); err != nil {
		log.Printf("subscriber: dynamodb audit failed: %v", err)
	}
	return s.queue.Delete(ctx, aws.ToString(raw.ReceiptHandle))
}

type errMaxRetry int

func (e errMaxRetry) Error() string {
	return "max retry reached"
}
