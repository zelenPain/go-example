#!/usr/bin/env bash
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-ap-southeast-1}"
ACCOUNT_ID="000000000000"
QUEUE_NAME="message-events"
DLQ_NAME="message-events-dlq"
BUCKET_NAME="training-user-segments"
TABLE_NAME="message_process_logs"

awslocal sqs create-queue --queue-name "${DLQ_NAME}"

DLQ_ARN="arn:aws:sqs:${REGION}:${ACCOUNT_ID}:${DLQ_NAME}"
awslocal sqs create-queue \
  --queue-name "${QUEUE_NAME}" \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

awslocal s3 mb "s3://${BUCKET_NAME}" || true
cat >/tmp/segment-active-users.json <<'JSON'
[
  {"user_id":"u001","line_user_id":"line-u001","active":true},
  {"user_id":"u002","line_user_id":"line-u002","active":false},
  {"user_id":"u003","line_user_id":"line-u003","active":true}
]
JSON
awslocal s3 cp /tmp/segment-active-users.json "s3://${BUCKET_NAME}/segments/active-users.json"

awslocal dynamodb create-table \
  --table-name "${TABLE_NAME}" \
  --attribute-definitions AttributeName=message_id,AttributeType=S \
  --key-schema AttributeName=message_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST || true
