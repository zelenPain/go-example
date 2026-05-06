package dedup

import (
	"context"
	"time"

	"go-example/internal/config"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	client *redis.Client
}

func New(cfg config.Config) Store {
	return Store{
		client: redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
		}),
	}
}

func (s Store) Close() error {
	return s.client.Close()
}

func (s Store) AcquireProcessingLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	// SET NX + TTL creates a short-lived distributed lock for the message being processed.
	return s.client.SetNX(ctx, "lock:"+key, "1", ttl).Result()
}

func (s Store) ReleaseProcessingLock(ctx context.Context, key string) error {
	return s.client.Del(ctx, "lock:"+key).Err()
}

func (s Store) MarkProcessed(ctx context.Context, key string, ttl time.Duration) error {
	// processed marker is a fast dedupe cache; MySQL remains the fallback/source of truth.
	return s.client.Set(ctx, "processed:"+key, "1", ttl).Err()
}

func (s Store) IsProcessed(ctx context.Context, key string) (bool, error) {
	result, err := s.client.Exists(ctx, "processed:"+key).Result()
	return result > 0, err
}
