package model

type SegmentUser struct {
	UserID     string `json:"user_id"`
	LineUserID string `json:"line_user_id"`
	Active     bool   `json:"active"`
}

type QueueMessage struct {
	CampaignID     int64  `json:"campaign_id"`
	OutboxID       int64  `json:"outbox_id"`
	UserID         string `json:"user_id"`
	LineUserID     string `json:"line_user_id"`
	Text           string `json:"text"`
	IdempotencyKey string `json:"idempotency_key"`
}
