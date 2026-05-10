package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-example/internal/awsclient"
	"go-example/internal/config"
	"go-example/internal/db"
	"go-example/internal/publisher"
	"go-example/internal/queue"
	"go-example/internal/segment"
)

func main() {
	// Publisher runs as a long-lived worker. The context is canceled when the
	// process receives Ctrl+C or SIGTERM from Docker/Kubernetes.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()

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
		// These values control the demo polling flow:
		// how often to query campaigns, how many campaigns can run in parallel,
		// and how many pending campaigns to pick per poll.
		time.Duration(cfg.PublisherPollSeconds)*time.Second,
		cfg.PublisherWorkers,
		cfg.PublisherClaimLimit,
	)

	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
