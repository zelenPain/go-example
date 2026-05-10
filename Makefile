.PHONY: deps infra-up infra-down migrate seed build publisher subscriber line-mock test

deps:
	go mod tidy

infra-up:
	docker compose up -d mysql redis localstack

infra-down:
	docker compose down

migrate:
	mysql -h "$${MYSQL_HOST:-127.0.0.1}" -P "$${MYSQL_PORT:-3307}" -u "$${MYSQL_USER:-training}" -p"$${MYSQL_PASSWORD:-training}" < migrations/001_init.sql

seed:
	mysql -h "$${MYSQL_HOST:-127.0.0.1}" -P "$${MYSQL_PORT:-3307}" -u "$${MYSQL_USER:-training}" -p"$${MYSQL_PASSWORD:-training}" < migrations/002_seed.sql

build:
	go build ./...

publisher:
	go run ./cmd/publisher

subscriber:
	go run ./cmd/subscriber

line-mock:
	go run ./cmd/line-mock

test:
	go test ./...
