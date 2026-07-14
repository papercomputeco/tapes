# Based around the auto-documented Makefile:
# http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html

VERSION ?= $(shell git describe --tags --always --dirty)
COMMIT  := $(shell git rev-parse HEAD)
BUILDTIME ?= $(shell date -u '+%Y-%m-%d %H:%M:%S')
REGISTRY ?= public.ecr.aws/g4e5l3z3/papercomputeco
IMAGE ?= tapes:dev

POSTHOG_API_KEY ?=
POSTHOG_ENDPOINT ?= https://us.i.posthog.com

LDFLAGS := -s -w \
	-X 'github.com/papercomputeco/tapes/pkg/utils.Version=$(VERSION)' \
	-X 'github.com/papercomputeco/tapes/pkg/utils.Sha=$(COMMIT)' \
	-X 'github.com/papercomputeco/tapes/pkg/utils.Buildtime=$(BUILDTIME)' \
	-X 'github.com/papercomputeco/tapes/pkg/telemetry.PostHogAPIKey=$(POSTHOG_API_KEY)' \
	-X 'github.com/papercomputeco/tapes/pkg/telemetry.PostHogEndpoint=$(POSTHOG_ENDPOINT)'

.PHONY: check
check: ## Runs all dagger checks. Auto-fixes are not automatically applied.
	$(call print-target)
	dagger check

.PHONY: format
format: ## Runs golangci-lint linters and formatters with auto-fixes applied.
	$(call print-target)
	dagger call fix-lint export --path .

.PHONY: swag
swag: ## Runs the swaggo/swag utility for generating the swagger yaml
	swag init \
		--parseDependency \
		--parseInternal \
		-g api/swagger.go \
		-o docs

.PHONY: swagfmt
swagfmt: ## Runs swaggo/swag for formatting swag godoc comments
	swag fmt

.PHONY: openapi
openapi: swag ## Regenerates the OpenAPI 3.0.3 contract (api/openapi.yaml) from the swag docs
	go run ./cmd/gen-openapi

.PHONY: generate
generate: ## Regenerates sqlc queries
	sqlc generate

.PHONY: build-local
build-local: ## Builds local artifacts with local toolchain
	$(call print-target)
	@mkdir -p ./build
	CGO_ENABLED=0 GOEXPERIMENT=jsonv2 go build -ldflags "$(LDFLAGS)" -o ./build/ ./cli/tapes

.PHONY: install
install: build-local ## Builds local artifacts and installs to configured $GOPATH
	$(call print-target)
	cp ./build/tapes $(shell go env GOBIN)

.PHONY: build
build: ## Builds all cross-platform artifacts - Warning! MacOS may fail cross compiling toolchain dependency
	dagger call \
		build-release \
			--version ${VERSION} \
			--commit ${COMMIT} \
			--post-hog-public-key="${POSTHOG_API_KEY}" \
		export \
			--path ./build

.PHONY: nightly
nightly: ## Builds and releases nightly tapes artifacts
	dagger call \
		nightly \
			--commit=${COMMIT} \
			--post-hog-public-key="${POSTHOG_API_KEY}" \
			--endpoint=env://BUCKET_ENDPOINT \
			--bucket=env://BUCKET_NAME \
			--access-key-id=env://BUCKET_ACCESS_KEY_ID \
			--secret-access-key=env://BUCKET_SECRET_ACCESS_KEY

.PHONY: upload-install-script
upload-install-script: ## Uploads the install script
	dagger call \
		upload-install-sh \
			--endpoint=env://BUCKET_ENDPOINT \
			--bucket=env://BUCKET_NAME \
			--access-key-id=env://BUCKET_ACCESS_KEY_ID \
			--secret-access-key=env://BUCKET_SECRET_ACCESS_KEY

.PHONY: release
release: ## Builds and releases tapes artifacts
	dagger call \
		release-latest \
			--version=${VERSION} \
			--commit=${COMMIT} \
			--post-hog-public-key=$(POSTHOG_API_KEY) \
			--endpoint=env://BUCKET_ENDPOINT \
			--bucket=env://BUCKET_NAME \
			--access-key-id=env://BUCKET_ACCESS_KEY_ID \
			--secret-access-key=env://BUCKET_SECRET_ACCESS_KEY

.PHONY: build-images
build-images: build-tapes-image ## Builds all container artifacts

.PHONY: build-local-image
build-local-image: ## Build a local Docker image for Kind/clearing (IMAGE=tapes:dev)
	$(call print-target)
	dagger call \
		build-tapes-image \
			--version=${VERSION} \
			--commit=${COMMIT} \
		export-image \
			--name=${IMAGE}

.PHONY: build-tapes-image
build-tapes-image: ## Builds, tags, and loads the tapes container artifact locally
	$(call print-target)
	dagger call \
		build-tapes-image \
			--version=${VERSION} \
			--commit=${COMMIT} \
		export-image \
			--name=${REGISTRY}/tapes:${VERSION}
	dagger call \
		build-tapes-image \
			--version=${VERSION} \
			--commit=${COMMIT} \
		export-image \
			--name=${REGISTRY}/tapes:latest

.PHONY: build-push-tapes-images
build-push-tapes-images: ## Builds and publishes the multi-arch tapes container images
	dagger call \
		build-push-tapes-images \
			--registry=${REGISTRY} \
			--tags=${VERSION} \
			--tags=latest \
			--version=${VERSION} \
			--commit=${COMMIT}

.PHONY: up
up:
	docker compose up --build

.PHONY: clean
clean: ## Removes the "build" directory with built artifacts
	$(call print-target)
	@rm -rf ./build

.PHONY: test
test: ## Runs tests via "go test" in the Dagger services environment
	$(call print-target)
	dagger call test

.PHONY: e2e-test
e2e-test: ## Runs end-to-end tests with Postgres and Ollama via Dagger
	$(call print-target)
	dagger call test-e-2-e

.PHONY: test-kafka-e2e
test-kafka-e2e: ## Runs Kafka e2e proxy publish test
	$(call print-target)
	dagger call test-kafka-e-2-e

.PHONY: help
.DEFAULT_GOAL := help
help: ## Prints this help message
	@grep -h -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

define print-target
    @printf "Executing target: \033[36m$@\033[0m\n"
endef
