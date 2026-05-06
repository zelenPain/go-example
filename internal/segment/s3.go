package segment

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"

	"go-example/internal/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Store struct {
	client *s3.Client
	bucket string
}

func NewStore(client *s3.Client, bucket string) Store {
	return Store{client: client, bucket: bucket}
}

func (s Store) GetUsers(ctx context.Context, key string) ([]model.SegmentUser, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer output.Body.Close()

	body, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, err
	}

	// Support two formats for flexible demos:
	// 1. JSON array: [{"user_id":"u001",...}]
	// 2. JSON lines: one user object per line.
	var users []model.SegmentUser
	if err := json.Unmarshal(body, &users); err == nil {
		return users, nil
	}

	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		var user model.SegmentUser
		if err := json.Unmarshal(scanner.Bytes(), &user); err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, scanner.Err()
}
