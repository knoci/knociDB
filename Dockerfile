# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/knocidb ./cmd/knocidb

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 10001 knocidb && \
    mkdir -p /data && chown knocidb:knocidb /data

USER knocidb
WORKDIR /data
COPY --from=builder /out/knocidb /usr/local/bin/knocidb

EXPOSE 9090 9091 63001 63101
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/knocidb"]
CMD ["--mode=standalone", "--data-dir=/data", "--grpc-addr=:9090", "--admin-addr=:9091"]
