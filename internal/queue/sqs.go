package queue

import (
	"context"
	"encoding/json"
	"strconv"

	"go-example/internal/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Client struct {
	sqs      *sqs.Client
	queueURL string
}

func NewClient(client *sqs.Client, queueURL string) Client {
	return Client{sqs: client, queueURL: queueURL}
}

func (c Client) Send(ctx context.Context, msg model.QueueMessage) (string, error) {
	body, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}
	// idempotency_key is stored in both the body and message attributes for easier SQS inspection/debugging.
	output, err := c.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(c.queueURL),
		MessageBody: aws.String(string(body)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"idempotency_key": {
				DataType:    aws.String("String"),
				StringValue: aws.String(msg.IdempotencyKey),
			},
		},
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(output.MessageId), nil
}

func (c Client) Receive(ctx context.Context, waitSeconds int) ([]types.Message, error) {
	// Long polling keeps the worker from spinning aggressively when the queue is empty.
	output, err := c.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.queueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     int32(waitSeconds),
		AttributeNames:      []types.QueueAttributeName{types.QueueAttributeName("ApproximateReceiveCount")},
		MessageAttributeNames: []string{
			"All",
		},
	})
	if err != nil {
		return nil, err
	}
	return output.Messages, nil
}

func (c Client) Delete(ctx context.Context, receiptHandle string) error {
	_, err := c.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}

func DecodeMessage(msg types.Message) (model.QueueMessage, error) {
	var queueMessage model.QueueMessage
	err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &queueMessage)
	return queueMessage, err
}

func ReceiveCount(msg types.Message) int {
	// ApproximateReceiveCount is used for retry logs and to align with the SQS redrive policy.
	value := msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]
	count, err := strconv.Atoi(value)
	if err != nil {
		return 1
	}
	return count
}
