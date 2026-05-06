package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go-example/internal/audit"
	"go-example/internal/awsclient"
	"go-example/internal/config"
	"go-example/internal/db"
	"go-example/internal/dedup"
	"go-example/internal/line"
	"go-example/internal/queue"
	"go-example/internal/subscriber"
)

func main() {
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

	dedupStore := dedup.New(cfg)
	defer dedupStore.Close()

	service := subscriber.NewService(
		repo,
		dedupStore,
		queue.NewClient(awsClients.SQS, cfg.SQSQueueURL),
		line.NewClient(cfg.LineEndpoint, cfg.LineChannelToken),
		audit.NewLogger(awsClients.DynamoDB, cfg.DynamoDBTable),
		cfg.MaxRetry,
		cfg.PollWaitSeconds,
		cfg.SubscriberWorkers,
	)
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
