# syntax=docker/dockerfile:1
# check=skip=SecretsUsedInArgOrEnv

# -----------------------------------------------------------------------------
# Build stage
# -----------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS builder
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILDTIME=dev
ARG POSTHOG_API_KEY
ARG POSTHOG_ENDPOINT=https://us.i.posthog.com

WORKDIR /src

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .

RUN CGO_ENABLED=0 \
    GOOS=${TARGETOS:-linux} \
    GOARCH=${TARGETARCH} \
    GOEXPERIMENT="jsonv2" \
    go build \
    -ldflags="-s -w -X 'github.com/papercomputeco/tapes/pkg/utils.Version=${VERSION}' -X 'github.com/papercomputeco/tapes/pkg/utils.Sha=${COMMIT}' -X 'github.com/papercomputeco/tapes/pkg/utils.Buildtime=${BUILDTIME}' -X 'github.com/papercomputeco/tapes/pkg/telemetry.PostHogAPIKey=${POSTHOG_API_KEY}' -X 'github.com/papercomputeco/tapes/pkg/telemetry.PostHogEndpoint=${POSTHOG_ENDPOINT}' -extldflags '-static'" \
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
