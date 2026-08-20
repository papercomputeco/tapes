# syntax=docker/dockerfile:1

# -----------------------------------------------------------------------------
# Build stage
# -----------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM golang:1.26-bookworm AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .

ARG LDFLAGS="-s -w"
RUN CGO_ENABLED=0 \
    GOOS=${TARGETOS:-linux} \
    GOARCH=${TARGETARCH} \
    GOEXPERIMENT="jsonv2" \
    go build \
    -ldflags="${LDFLAGS} -extldflags '-static'" \
    -o /bin/tapes \
    ./cli/tapes

# -----------------------------------------------------------------------------
# Runtime
# -----------------------------------------------------------------------------
FROM alpine:3.20

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

WORKDIR /app

COPY --from=builder /bin/tapes /app/tapes

USER 1000:1000
EXPOSE 8080
ENTRYPOINT ["/app/tapes"]
