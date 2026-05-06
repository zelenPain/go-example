# go-example

## Kiến Trúc Thư Mục

```text
cmd/
  publisher/       Chạy publisher thủ công với mode single hoặc batch
  subscriber/      Worker long-poll SQS
  line-mock/       HTTP server giả lập LINE API
internal/
  audit/           DynamoDB audit log
  awsclient/       AWS SDK clients cho SQS, S3, DynamoDB
  config/          Load config từ env
  db/              MySQL repository
  dedup/           Redis lock và processed marker
  line/            LINE HTTP client
  model/           Queue message và segment user model
  publisher/       Business logic publish campaign
  queue/           SQS wrapper
  segment/         S3 segment reader
  subscriber/      Business logic consume message
migrations/        MySQL schema và seed data
docker/            Script init LocalStack
```

## Vai Trò Các Service

- `MySQL`: source of truth chính, lưu campaign và outbox message.
- `S3`: lưu file user segment input.
- `SQS`: message queue giữa publisher và subscriber.
- `Redis`: lock và dedupe nhanh cho subscriber.
- `DynamoDB`: audit log quá trình subscriber xử lý message.
- `line-mock`: giả lập LINE endpoint để test mà không cần LINE API thật.
- `LocalStack`: giả lập SQS, S3, DynamoDB trong Docker.

## Luồng Xử Lý

### Happy Path

1. Chạy `publisher` thủ công để gọi `publisher.Service.PublishNextCampaign` hoặc `PublishNextCampaignInBatches`.
2. `internal/db.NextPendingCampaign` tìm campaign `pending`, lock bằng `SELECT ... FOR UPDATE`, rồi update sang `processing`.
3. `internal/segment.Store.GetUsers` đọc file JSON từ S3.
4. Publisher bỏ qua user có `active = false`.
5. Publisher build `model.QueueMessage`.
6. Publisher insert row vào `outbox_messages` với `idempotency_key` unique.
7. Publisher send message vào SQS qua `internal/queue.Client.Send`.
8. Publisher update `outbox_messages.status = sent_to_sqs`.
9. Subscriber long-poll SQS qua `internal/queue.Client.Receive`.
10. Subscriber xử lý các message nhận được song song, giới hạn bởi `SUBSCRIBER_WORKERS`.
11. Subscriber check Redis `processed:*`, MySQL `AlreadyConsumed`, và Redis lock `lock:*`.
12. Subscriber gọi `line.Client.Send` tới `line-mock`.
13. Subscriber update MySQL `consumed`.
14. Subscriber ghi audit vào DynamoDB `message_process_logs`.
15. Subscriber delete message khỏi SQS.

### Duplicate Message

1. SQS có thể deliver lại cùng một message.
2. Subscriber check Redis `processed:{idempotency_key}`.
3. Nếu Redis chưa có, subscriber check MySQL `outbox_messages.status = consumed`.
4. Nếu đã consumed, subscriber delete SQS message và không gọi LINE nữa.

### Retry Và DLQ

1. Nếu LINE fail, subscriber không delete SQS message.
2. SQS sẽ làm message visible lại sau visibility timeout.
3. Subscriber đọc `ApproximateReceiveCount`.
4. Khi vượt max receive count, LocalStack/SQS redrive message sang DLQ `message-events-dlq`.

## Database

### `message_campaigns`

Quản lý một đợt gửi message.

Field quan trọng:

- `name`: tên campaign.
- `segment_file_key`: key file segment trên S3, ví dụ `segments/active-users.json`.
- `status`: `pending`, `processing`, `completed`, `failed`.
- `error_message`: lưu lỗi khi publisher fail.

### `outbox_messages`

Quản lý từng message gửi cho từng user.

Field quan trọng:

- `campaign_id`: campaign cha.
- `user_id`, `line_user_id`: user nhận message.
- `payload`: JSON message.
- `sqs_message_id`: ID sau khi send vào SQS.
- `idempotency_key`: khóa chống gửi/xử lý trùng, ví dụ `campaign:1:user:u001`.
- `status`: `pending`, `sent_to_sqs`, `consumed`, `failed`, `dlq`.
- `retry_count`: số lần retry subscriber ghi nhận.

## Yêu Cầu Môi Trường

- Windows + Docker Desktop, hoặc Ubuntu WSL dùng Docker Desktop integration.
- Go 1.26+ nếu chạy bằng `go run`/`go test` ngoài Docker.
- MySQL container mặc định, hoặc AWS RDS MySQL khi đổi env.

Kiểm tra Go:

```powershell
go version
```

## Cấu Hình

Project load config từ environment variables. Khi chạy bằng Docker Compose, file `.env` được inject qua `env_file`. Khi chạy trực tiếp bằng `go run`, ứng dụng không tự đọc `.env`, nên cần export env trước hoặc dùng công cụ load `.env`.

### MySQL Container Mặc Định

Docker Compose chạy MySQL bằng image `mysql:8.4`. App trong container connect qua hostname nội bộ `mysql:3306`; máy host Windows/WSL connect qua port `3307`.

```text
MYSQL_DSN=
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_USER=training
MYSQL_PASSWORD=training
MYSQL_DATABASE=training_msg_queue
MYSQL_TLS=
```

Nếu `MYSQL_DSN` để trống, app tự build DSN từ các field `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`, `MYSQL_TLS`. Nếu set `MYSQL_DSN`, giá trị này sẽ override các field riêng lẻ.

### AWS RDS MySQL

Để dùng AWS RDS MySQL thật:

```text
MYSQL_DSN=
MYSQL_HOST=<your-rds-endpoint>
MYSQL_PORT=3306
MYSQL_USER=<rds-user>
MYSQL_PASSWORD=<rds-password>
MYSQL_DATABASE=training_msg_queue
MYSQL_TLS=true
```

`MYSQL_TLS=true` phù hợp khi RDS yêu cầu TLS. Nếu môi trường test cho phép non-TLS, có thể để trống.

### LocalStack

Giá trị LocalStack mặc định:

```text
AWS_REGION=ap-southeast-1
AWS_ENDPOINT=http://localstack:4566
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test

SQS_QUEUE_URL=http://localstack:4566/000000000000/message-events
SQS_DLQ_URL=http://localstack:4566/000000000000/message-events-dlq
S3_BUCKET=training-user-segments
DYNAMODB_TABLE=message_process_logs
```

Code AWS client hiện dùng `BaseEndpoint` theo từng service client, không dùng global endpoint resolver deprecated nữa. Nếu dùng AWS thật cho SQS/S3/DynamoDB, để trống `AWS_ENDPOINT`:

```text
AWS_ENDPOINT=
AWS_REGION=ap-southeast-1
AWS_ACCESS_KEY_ID=<real-access-key>
AWS_SECRET_ACCESS_KEY=<real-secret-key>
SQS_QUEUE_URL=<real-sqs-url>
S3_BUCKET=<real-bucket>
DYNAMODB_TABLE=<real-table>
```

### Publisher

Batch size mặc định khi chạy `--mode=batch`:

```text
PUBLISH_BATCH_SIZE=100
```

Có thể override trực tiếp bằng flag:

```powershell
docker compose --profile app run --rm publisher --mode=batch --batch-size=2
```

### Subscriber

Subscriber xử lý song song các message trong mỗi lần receive từ SQS. SQS đang receive tối đa 10 messages/lần; `SUBSCRIBER_WORKERS` giới hạn số goroutine xử lý đồng thời:

```text
SUBSCRIBER_WORKERS=4
POLL_WAIT_SECONDS=10
MAX_RETRY=3
```

## Connect MySQL Container

Project mặc định dùng database:

```text
Host: 127.0.0.1
Port: 3307
User: training
Password: training
Database: training_msg_queue
SSL: None
```

Lưu ý: port `3307` là port expose ra máy host. Bên trong Docker network, app vẫn dùng `mysql:3306`.

## Init Database

Khi volume `mysql-data` được tạo lần đầu, MySQL container tự chạy các file trong `docker-entrypoint-initdb.d`:

- `migrations/001_init.sql`
- `migrations/002_seed.sql`

Nếu muốn chạy migration thủ công từ host:

```powershell
Get-Content migrations\001_init.sql | mysql -h 127.0.0.1 -P 3307 -u training -ptraining
Get-Content migrations\002_seed.sql | mysql -h 127.0.0.1 -P 3307 -u training -ptraining
```

Kiểm tra:

```sql
USE training_msg_queue;
SHOW TABLES;
SELECT id, name, status, segment_file_key FROM message_campaigns;
```

## Chạy Bằng Docker

Start infra:

```powershell
docker compose up -d mysql redis localstack
```

Build image:

```powershell
docker compose --profile app build
```

Run app services:

```powershell
docker compose --profile app up subscriber line-mock
```

Sau đó chạy publisher thủ công bằng một terminal khác.

Single publisher:

```powershell
docker compose --profile app run --rm publisher
```

Batch publisher:

```powershell
docker compose --profile app run --rm publisher --mode=batch --batch-size=2
```

Log happy path kỳ vọng:

```text
publisher: campaign_id=1 published=2
line-mock: to=line-u001 text="Training message from campaign Training campaign 001"
line-mock: to=line-u003 text="Training message from campaign Training campaign 001"
AWS dynamodb.PutItem => 200
AWS sqs.DeleteMessage => 200
```

## Chạy Riêng Từng Component

Start line mock và subscriber:

```powershell
docker compose --profile app up subscriber line-mock
```

Chạy publisher một lần:

```powershell
docker compose --profile app run --rm publisher
```

Chạy batch publisher:

```powershell
docker compose --profile app run --rm publisher --mode=batch --batch-size=2
```

## Chạy Từ Ubuntu WSL

Nếu chạy source bằng `go run` trong Ubuntu WSL, các hostname Docker service như `redis`, `localstack`, `line-mock` thường không dùng trực tiếp được. Khi đó dùng port đã publish:

```text
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3307
REDIS_ADDR=127.0.0.1:6379
AWS_ENDPOINT=http://localhost:4566
SQS_QUEUE_URL=http://localhost:4566/000000000000/message-events
LINE_ENDPOINT=http://localhost:8080/messages
```

Nếu chạy app bằng Docker Compose thì giữ `MYSQL_HOST=mysql`, `MYSQL_PORT=3306` trong `.env`.

## Build/Test Local

Với PowerShell, nên đặt `GOCACHE` trong workspace để tránh lỗi quyền cache:

```powershell
$env:GOCACHE='E:\projects\exmaple-go-msg-queue\.gocache'
go mod tidy
go test ./...
```

## Quan Sát SQS

List queues:

```powershell
docker compose exec localstack awslocal sqs list-queues
```

Xem attributes queue chính:

```powershell
docker compose exec localstack awslocal sqs get-queue-attributes `
  --queue-url http://localhost:4566/000000000000/message-events `
  --attribute-names All
```

Field cần quan sát:

- `ApproximateNumberOfMessages`: message đang chờ consume.
- `ApproximateNumberOfMessagesNotVisible`: message đã receive nhưng chưa delete.
- `RedrivePolicy`: cấu hình đẩy sang DLQ.

Receive message thủ công:

```powershell
docker compose exec localstack awslocal sqs receive-message `
  --queue-url http://localhost:4566/000000000000/message-events `
  --attribute-names All `
  --message-attribute-names All `
  --max-number-of-messages 10
```

Xem DLQ:

```powershell
docker compose exec localstack awslocal sqs receive-message `
  --queue-url http://localhost:4566/000000000000/message-events-dlq `
  --attribute-names All `
  --message-attribute-names All `
  --max-number-of-messages 10
```

## Quan Sát DynamoDB

List tables:

```powershell
docker compose exec localstack awslocal dynamodb list-tables
```

Scan audit logs:

```powershell
docker compose exec localstack awslocal dynamodb scan `
  --table-name message_process_logs
```

Get audit log theo `idempotency_key`:

```powershell
docker compose exec localstack awslocal dynamodb get-item `
  --table-name message_process_logs `
  --key '{"message_id":{"S":"campaign:1:user:u001"}}'
```

## Quan Sát Redis

Mở Redis CLI:

```powershell
docker compose exec redis redis-cli
```

Trong Redis CLI:

```text
KEYS *
GET processed:campaign:1:user:u001
GET lock:campaign:1:user:u001
```

Lưu ý: `lock:*` có TTL ngắn, thường biến mất nhanh sau khi xử lý xong.

## Test Edge Cases

Nên test từng case riêng. Vì publisher chạy thủ công nên không có scheduler tự động pick campaign.

```powershell
docker compose up -d mysql redis localstack line-mock
docker compose --profile app up subscriber line-mock
```

### 1. Happy Path

Tạo campaign:

```sql
INSERT INTO message_campaigns (name, segment_file_key, status)
VALUES ('Happy path test', 'segments/active-users.json', 'pending');
```

Chạy publisher single:

```powershell
docker compose --profile app run --rm publisher
```

Hoặc chạy publisher batch:

```powershell
docker compose --profile app run --rm publisher --mode=batch --batch-size=2
```

Kiểm tra DB:

```sql
SELECT id, name, status FROM message_campaigns ORDER BY id DESC LIMIT 3;
SELECT id, user_id, line_user_id, status, sqs_message_id FROM outbox_messages ORDER BY id DESC LIMIT 5;
```

Kỳ vọng:

```text
campaign status = completed
2 outbox rows status = consumed
line-mock có log line-u001 và line-u003
```

### 2. Duplicate Message

Lấy một message đã consumed:

```sql
SELECT id, campaign_id, user_id, line_user_id, idempotency_key
FROM outbox_messages
LIMIT 1;
```

Gửi duplicate vào SQS:

```powershell
docker compose exec localstack awslocal sqs send-message `
  --queue-url http://localhost:4566/000000000000/message-events `
  --message-body '{"campaign_id":1,"outbox_id":1,"user_id":"u001","line_user_id":"line-u001","text":"DUPLICATE TEST","idempotency_key":"campaign:1:user:u001"}'
```

Xem log:

```powershell
docker compose logs -f subscriber line-mock localstack
```

Kỳ vọng:

```text
sqs.ReceiveMessage => 200
sqs.DeleteMessage => 200
Không có: line-mock: to=line-u001 text="DUPLICATE TEST"
```

### 3. Retry Và DLQ

Dừng LINE mock để subscriber fail:

```powershell
docker compose stop line-mock
```

Tạo campaign:

```sql
INSERT INTO message_campaigns (name, segment_file_key, status)
VALUES ('Retry DLQ test', 'segments/active-users.json', 'pending');
```

Chạy publisher:

```powershell
docker compose --profile app run --rm publisher
```

Chờ 40-60 giây, sau đó xem DLQ:

```powershell
docker compose exec localstack awslocal sqs receive-message `
  --queue-url http://localhost:4566/000000000000/message-events-dlq `
  --attribute-names All `
  --message-attribute-names All `
  --max-number-of-messages 10
```

Bật lại LINE mock:

```powershell
docker compose start line-mock
```

### 4. S3 Missing File

Tạo campaign trỏ tới segment không tồn tại:

```sql
INSERT INTO message_campaigns (name, segment_file_key, status)
VALUES ('Missing S3 test', 'segments/not-found.json', 'pending');
```

Chạy publisher:

```powershell
docker compose --profile app run --rm publisher
```

Kiểm tra DB:

```sql
SELECT id, name, status, error_message
FROM message_campaigns
ORDER BY id DESC
LIMIT 1;
```

Kỳ vọng:

```text
status = failed
error_message có lỗi GetObject hoặc NoSuchKey
```

### 5. Redis Down Fallback

Dừng Redis:

```powershell
docker compose stop redis
```

Tạo campaign:

```sql
INSERT INTO message_campaigns (name, segment_file_key, status)
VALUES ('Redis down fallback test', 'segments/active-users.json', 'pending');
```

Chạy publisher:

```powershell
docker compose --profile app run --rm publisher
```

Kỳ vọng:

```text
subscriber có log lỗi Redis
line-mock vẫn nhận message
outbox_messages.status = consumed
```

Bật lại Redis:

```powershell
docker compose start redis
```

## Reset Data Để Test Lại

Trong MySQL:

```sql
USE training_msg_queue;
DELETE FROM outbox_messages;
DELETE FROM message_campaigns;
INSERT INTO message_campaigns (name, segment_file_key, status)
VALUES ('Training campaign 001', 'segments/active-users.json', 'pending');
```

Restart containers:

```powershell
docker compose down
docker compose up -d mysql redis localstack
docker compose --profile app up --build subscriber line-mock
```

## Lưu Ý Khi Copy Sang Máy Khác

- Giữ file init LocalStack ở line ending LF, không phải CRLF:

```bash
dos2unix docker/init-localstack.sh
chmod +x docker/init-localstack.sh
```

- Nếu chạy app bằng Docker Compose, dùng hostname theo network Docker: `mysql`, `redis`, `localstack`, `line-mock`.
- Nếu chạy app bằng `go run` từ WSL, dùng `localhost` cho các service đã publish port.
- Không commit secret thật vào `.env`. Dùng `.env.example` làm template.

### Chuyển Sang MySQL Local Có Sẵn

Mặc định repo chạy MySQL bằng service `mysql` trong Docker Compose. Nếu copy code sang máy khác và muốn dùng MySQL local có sẵn thay vì MySQL container, không chỉ xóa service `mysql`; cần đổi cả Compose và `.env`.

Nếu app vẫn chạy bằng Docker Compose:

1. Xóa service `mysql` trong `docker-compose.yml`.
2. Xóa `mysql` khỏi `depends_on` của `publisher` và `subscriber`.
3. Đổi `.env`:

```text
MYSQL_DSN=
MYSQL_HOST=host.docker.internal
MYSQL_PORT=3306
MYSQL_USER=training
MYSQL_PASSWORD=training
MYSQL_DATABASE=training_msg_queue
MYSQL_TLS=
```

4. Tự chạy migration vào MySQL local:

```powershell
Get-Content migrations\001_init.sql | mysql -h 127.0.0.1 -P 3306 -u training -ptraining
Get-Content migrations\002_seed.sql | mysql -h 127.0.0.1 -P 3306 -u training -ptraining
```

5. Đảm bảo MySQL local đang listen port `3306` và cho phép Docker connect từ `host.docker.internal`.

Nếu chạy app bằng `go run` trực tiếp từ Windows/WSL, không cần sửa `docker-compose.yml`; chỉ cần set env trỏ tới MySQL local:

```text
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
```

## Troubleshooting

### `go.sum` missing khi Docker build

Chạy:

```powershell
go mod tidy
```

Sau đó build lại:

```powershell
docker compose --profile app build
```

### PowerShell lỗi JSON khi gửi SQS message

Dùng single quote bọc JSON:

```powershell
--message-body '{"campaign_id":1,"outbox_id":1}'
```

Hoặc tạo biến:

```powershell
$body = @{
  campaign_id = 1
  outbox_id = 1
  user_id = "u001"
  line_user_id = "line-u001"
  text = "DUPLICATE TEST"
  idempotency_key = "campaign:1:user:u001"
} | ConvertTo-Json -Compress
```

### Docker app không connect được MySQL

Kiểm tra MySQL container:

```powershell
docker compose ps mysql
mysql -h 127.0.0.1 -P 3307 -u training -ptraining training_msg_queue
```

Nếu app chạy trong Docker Compose, `.env` nên dùng:

```text
MYSQL_HOST=mysql
MYSQL_PORT=3306
```

Nếu chạy `go run` trực tiếp từ Windows/WSL, dùng port expose ra host:

```text
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3307
```

### Docker báo network not found

Nếu gặp lỗi dạng:

```text
failed to set up container networking: network <id> not found
```

Thường là container cũ còn reference tới Docker network đã bị xóa/recreate sau khi sửa `docker-compose.yml`. Chạy lại sạch Compose, không xóa volume MySQL:

```powershell
docker compose down --remove-orphans
docker compose up -d mysql redis localstack
docker compose --profile app up subscriber line-mock
```

Nếu vẫn lỗi, force recreate:

```powershell
docker compose --profile app up --force-recreate subscriber line-mock
```

Nếu vẫn còn lỗi network, prune network không còn dùng:

```powershell
docker network prune
docker compose up -d mysql redis localstack
docker compose --profile app up subscriber line-mock
```

Không dùng `docker compose down -v` trừ khi muốn reset sạch database, vì `-v` sẽ xóa volume `mysql-data`.

### Subscriber poll SQS liên tục

Đây là hành vi bình thường của long polling:

```text
AWS sqs.ReceiveMessage => 200
```

Nếu queue rỗng, subscriber vẫn poll tiếp mỗi `POLL_WAIT_SECONDS`.
