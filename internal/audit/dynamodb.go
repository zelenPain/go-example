package audit

import (
	"context"
	"fmt"
	"time"

	"go-example/internal/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Logger struct {
	client *dynamodb.Client
	table  string
}

func NewLogger(client *dynamodb.Client, table string) Logger {
	return Logger{client: client, table: table}
}

func (l Logger) Log(ctx context.Context, msg model.QueueMessage, status string, receiveCount int) error {
	// message_id uses idempotency_key so each logical message has one audit record to query.
	_, err := l.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(l.table),
		Item: map[string]types.AttributeValue{
			"message_id":       &types.AttributeValueMemberS{Value: msg.IdempotencyKey},
			"campaign_id":      &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", msg.CampaignID)},
			"outbox_id":        &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", msg.OutboxID)},
			"user_id":          &types.AttributeValueMemberS{Value: msg.UserID},
			"status":           &types.AttributeValueMemberS{Value: status},
			"receive_count":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", receiveCount)},
			"processed_at_utc": &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
		},
	})
	return err
}
