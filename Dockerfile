FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/publisher ./cmd/publisher \
    && go build -o /out/subscriber ./cmd/subscriber \
    && go build -o /out/line-mock ./cmd/line-mock

FROM alpine:3.20

RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/ /usr/local/bin/

CMD ["subscriber"]
