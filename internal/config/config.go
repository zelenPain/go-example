package config

import (
	"os"
	"strconv"

	"github.com/go-sql-driver/mysql"
)

type Config struct {
	AppEnv               string
	MySQLDSN             string
	MySQLHost            string
	MySQLPort            string
	MySQLUser            string
	MySQLPassword        string
	MySQLDatabase        string
	MySQLTLS             string
	RedisAddr            string
	RedisPassword        string
	RedisDB              int
	AWSRegion            string
	AWSEndpoint          string
	AWSAccessKeyID       string
	AWSSecretKey         string
	SQSQueueURL          string
	SQSDLQURL            string
	S3Bucket             string
	DynamoDBTable        string
	MaxRetry             int
	PollWaitSeconds      int
	SubscriberWorkers    int
	PublisherPollSeconds int
	PublisherWorkers     int
	PublisherClaimLimit  int
	LineEndpoint         string
	LineChannelToken     string
}

func Load() Config {
	// MYSQL_DSN has the highest priority. When it is empty, the app builds
	// the DSN from separate fields so local Docker, local MySQL, and RDS are easy to switch.
	mysqlHost := env("MYSQL_HOST", "127.0.0.1")
	mysqlPort := env("MYSQL_PORT", "3306")
	mysqlUser := env("MYSQL_USER", "training")
	mysqlPassword := env("MYSQL_PASSWORD", "training")
	mysqlDatabase := env("MYSQL_DATABASE", "training_msg_queue")
	mysqlTLS := env("MYSQL_TLS", "")

	return Config{
		AppEnv:               env("APP_ENV", "local"),
		MySQLDSN:             mysqlDSN(mysqlHost, mysqlPort, mysqlUser, mysqlPassword, mysqlDatabase, mysqlTLS),
		MySQLHost:            mysqlHost,
		MySQLPort:            mysqlPort,
		MySQLUser:            mysqlUser,
		MySQLPassword:        mysqlPassword,
		MySQLDatabase:        mysqlDatabase,
		MySQLTLS:             mysqlTLS,
		RedisAddr:            env("REDIS_ADDR", "127.0.0.1:6379"),
		RedisPassword:        env("REDIS_PASSWORD", ""),
		RedisDB:              envInt("REDIS_DB", 0),
		AWSRegion:            env("AWS_REGION", "ap-southeast-1"),
		AWSEndpoint:          env("AWS_ENDPOINT", "http://localhost:4566"),
		AWSAccessKeyID:       env("AWS_ACCESS_KEY_ID", "test"),
		AWSSecretKey:         env("AWS_SECRET_ACCESS_KEY", "test"),
		SQSQueueURL:          env("SQS_QUEUE_URL", "http://localhost:4566/000000000000/message-events"),
		SQSDLQURL:            env("SQS_DLQ_URL", "http://localhost:4566/000000000000/message-events-dlq"),
		S3Bucket:             env("S3_BUCKET", "training-user-segments"),
		DynamoDBTable:        env("DYNAMODB_TABLE", "message_process_logs"),
		MaxRetry:             envInt("MAX_RETRY", 3),
		PollWaitSeconds:      envInt("POLL_WAIT_SECONDS", 10),
		SubscriberWorkers:    envInt("SUBSCRIBER_WORKERS", 4),
		PublisherPollSeconds: envInt("PUBLISHER_POLL_SECONDS", 5),
		PublisherWorkers:     envInt("PUBLISHER_WORKERS", 4),
		PublisherClaimLimit:  envInt("PUBLISHER_CLAIM_LIMIT", 10),
		LineEndpoint:         env("LINE_ENDPOINT", "http://localhost:8080/messages"),
		LineChannelToken:     env("LINE_CHANNEL_TOKEN", "local-token"),
	}
}

func mysqlDSN(host, port, user, password, database, tls string) string {
	if dsn := os.Getenv("MYSQL_DSN"); dsn != "" {
		return dsn
	}

	// Use mysql.Config to format and escape the DSN instead of hand-building a string.
	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = host + ":" + port
	cfg.DBName = database
	cfg.ParseTime = true
	cfg.Params = map[string]string{
		"multiStatements": "true",
	}
	if tls != "" {
		cfg.TLSConfig = tls
	}
	return cfg.FormatDSN()
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
