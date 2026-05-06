package main

import (
	"context"
	"flag"
	"log"
	"time"

	"go-example/internal/awsclient"
	"go-example/internal/config"
	"go-example/internal/db"
	"go-example/internal/publisher"
	"go-example/internal/queue"
	"go-example/internal/segment"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cfg := config.Load()
	// Publisher has two demo modes:
	// single: publish all active users in one pass.
	// batch: split active users into smaller chunks for easier observation/retry.
	mode := flag.String("mode", "single", "publisher mode: single or batch")
	batchSize := flag.Int("batch-size", cfg.PublishBatchSize, "number of active users to publish per batch")
	flag.Parse()

	repo, err := db.Open(ctx, cfg.MySQLDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer repo.Close()

	awsClients, err := awsclient.New(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	service := publisher.NewService(
		repo,
		segment.NewStore(awsClients.S3, cfg.S3Bucket),
		queue.NewClient(awsClients.SQS, cfg.SQSQueueURL),
	)

	switch *mode {
	case "single":
		err = service.PublishNextCampaign(ctx)
	case "batch":
		err = service.PublishNextCampaignInBatches(ctx, *batchSize)
	default:
		log.Fatalf("unsupported publisher mode %q", *mode)
	}
	if err != nil {
		log.Fatal(err)
	}
}
