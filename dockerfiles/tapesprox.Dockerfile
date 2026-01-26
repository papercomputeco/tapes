# syntax=docker/dockerfile:1

# -----------------------------------------------------------------------------
# Build stage
# -----------------------------------------------------------------------------
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .

ARG LDFLAGS="-s -w"
RUN CGO_ENABLED=0 go build \
    -ldflags="${LDFLAGS}" \
    -o /bin/tapesprox \
    ./cmd/proxy

# -----------------------------------------------------------------------------
# Runtime
# -----------------------------------------------------------------------------
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

# Create non-root user
RUN addgroup -g 1000 tapes && \
    adduser -u 1000 -G tapes -s /bin/sh -D tapes

WORKDIR /app

COPY --from=builder /bin/tapesprox /app/tapesprox

# Default data directory for SQLite
RUN mkdir -p /data && chown tapes:tapes /data
VOLUME ["/data"]

USER tapes

EXPOSE 8080

ENTRYPOINT ["/app/tapesprox"]
CMD ["-listen", ":8080"]
