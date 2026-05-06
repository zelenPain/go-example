package awsclient

import (
	"context"

	"go-example/internal/config"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Clients struct {
	SQS      *sqs.Client
	S3       *s3.Client
	DynamoDB *dynamodb.Client
}

func New(ctx context.Context, cfg config.Config) (Clients, error) {
	options := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.AWSRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretKey, "")),
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return Clients{}, err
	}

	return Clients{
		SQS: sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
			// AWSEndpoint is only set for LocalStack. When empty, the SDK resolves
			// the real AWS endpoint from the configured region.
			if cfg.AWSEndpoint != "" {
				o.BaseEndpoint = &cfg.AWSEndpoint
			}
		}),
		S3: s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			if cfg.AWSEndpoint != "" {
				o.BaseEndpoint = &cfg.AWSEndpoint
				// LocalStack needs path-style URLs: /bucket/key instead of virtual-hosted style.
				o.UsePathStyle = true
			}
		}),
		DynamoDB: dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
			if cfg.AWSEndpoint != "" {
				o.BaseEndpoint = &cfg.AWSEndpoint
			}
		}),
	}, nil
}
