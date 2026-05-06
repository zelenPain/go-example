CREATE DATABASE IF NOT EXISTS training_msg_queue;
USE training_msg_queue;

CREATE TABLE IF NOT EXISTS message_campaigns (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  segment_file_key VARCHAR(512) NOT NULL,
  status ENUM('pending','processing','completed','failed') NOT NULL DEFAULT 'pending',
  error_message TEXT NULL,
  started_at DATETIME NULL,
  finished_at DATETIME NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS outbox_messages (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  campaign_id BIGINT UNSIGNED NOT NULL,
  user_id VARCHAR(128) NOT NULL,
  line_user_id VARCHAR(128) NOT NULL,
  sqs_message_id VARCHAR(255) NULL,
  idempotency_key VARCHAR(255) NOT NULL,
  payload JSON NOT NULL,
  status ENUM('pending','sent_to_sqs','consumed','failed','dlq') NOT NULL DEFAULT 'pending',
  retry_count INT NOT NULL DEFAULT 0,
  error_message TEXT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_idempotency_key (idempotency_key),
  KEY idx_campaign_status (campaign_id, status),
  CONSTRAINT fk_outbox_campaign FOREIGN KEY (campaign_id) REFERENCES message_campaigns(id)
);
