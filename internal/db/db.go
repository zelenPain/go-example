package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Repository struct {
	db *sql.DB
}

type Campaign struct {
	ID             int64
	Name           string
	SegmentFileKey string
	Status         string
}

type OutboxMessage struct {
	ID             int64
	CampaignID     int64
	UserID         string
	LineUserID     string
	IdempotencyKey string
	Payload        []byte
	Status         string
	RetryCount     int
}

func Open(ctx context.Context, dsn string) (*Repository, error) {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(5 * time.Minute)
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &Repository{db: conn}, nil
}

func (r *Repository) Close() error {
	return r.db.Close()
}

func (r *Repository) PendingCampaigns(ctx context.Context, limit int) ([]Campaign, error) {
	if limit <= 0 {
		limit = 1
	}

	// Demo assumption: only one publisher process is running, so a simple
	// pending query is enough. No transaction or row lock is needed here.
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, segment_file_key, status
		FROM message_campaigns
		WHERE status = 'pending'
		ORDER BY id ASC
		LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var campaigns []Campaign
	for rows.Next() {
		var campaign Campaign
		if err := rows.Scan(&campaign.ID, &campaign.Name, &campaign.SegmentFileKey, &campaign.Status); err != nil {
			return nil, err
		}
		campaigns = append(campaigns, campaign)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(campaigns) == 0 {
		return nil, nil
	}
	return campaigns, nil
}

func (r *Repository) MarkCampaignProcessing(ctx context.Context, id int64) error {
	// Mark the campaign before handing it to a goroutine. The status condition
	// keeps the update narrow and avoids changing a campaign that was manually
	// moved out of pending.
	_, err := r.db.ExecContext(ctx, `
		UPDATE message_campaigns
		SET status = 'processing', started_at = COALESCE(started_at, NOW()), error_message = NULL
		WHERE id = ? AND status = 'pending'`, id)
	if err != nil {
		return err
	}
	return nil
}

func (r *Repository) CompleteCampaign(ctx context.Context, id int64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE message_campaigns
		SET status = 'completed', finished_at = NOW(), error_message = NULL
		WHERE id = ?`, id)
	return err
}

func (r *Repository) FailCampaign(ctx context.Context, id int64, cause error) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE message_campaigns
		SET status = 'failed', finished_at = NOW(), error_message = ?
		WHERE id = ?`, errorString(cause), id)
	return err
}

func (r *Repository) InsertOutbox(ctx context.Context, msg OutboxMessage) (int64, bool, error) {
	// idempotency_key is unique. If a retry sees the same campaign/user pair,
	// MySQL reuses the existing row instead of creating a duplicate.
	result, err := r.db.ExecContext(ctx, `
		INSERT INTO outbox_messages
			(campaign_id, user_id, line_user_id, idempotency_key, payload, status)
		VALUES (?, ?, ?, ?, ?, 'pending')
		ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP`,
		msg.CampaignID, msg.UserID, msg.LineUserID, msg.IdempotencyKey, msg.Payload)
	if err != nil {
		return 0, false, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, false, err
	}
	if id == 0 {
		// LastInsertId = 0 means the insert hit the duplicate key path. Fetch the
		// existing outbox id so the caller can decide whether to send SQS or skip.
		id, err = r.FindOutboxID(ctx, msg.IdempotencyKey)
		return id, false, err
	}
	return id, true, nil
}

func (r *Repository) FindOutboxID(ctx context.Context, key string) (int64, error) {
	var id int64
	err := r.db.QueryRowContext(ctx, `
		SELECT id FROM outbox_messages WHERE idempotency_key = ?`, key).Scan(&id)
	return id, err
}

func (r *Repository) OutboxStatus(ctx context.Context, id int64) (string, error) {
	var status string
	err := r.db.QueryRowContext(ctx, `
		SELECT status FROM outbox_messages WHERE id = ?`, id).Scan(&status)
	return status, err
}

func (r *Repository) MarkSentToSQS(ctx context.Context, id int64, sqsMessageID string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox_messages
		SET status = 'sent_to_sqs', sqs_message_id = ?, error_message = NULL
		WHERE id = ?`, sqsMessageID, id)
	return err
}

func (r *Repository) MarkConsumed(ctx context.Context, idempotencyKey string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox_messages
		SET status = 'consumed', error_message = NULL
		WHERE idempotency_key = ?`, idempotencyKey)
	return err
}

func (r *Repository) MarkFailed(ctx context.Context, idempotencyKey string, retryCount int, cause error) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox_messages
		SET status = 'failed', retry_count = ?, error_message = ?
		WHERE idempotency_key = ?`, retryCount, errorString(cause), idempotencyKey)
	return err
}

func (r *Repository) AlreadyConsumed(ctx context.Context, idempotencyKey string) (bool, error) {
	var status string
	err := r.db.QueryRowContext(ctx, `
		SELECT status FROM outbox_messages WHERE idempotency_key = ?`, idempotencyKey).Scan(&status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return status == "consumed", nil
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%v", err)
}
