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
	# install (not cp) writes a temp file and renames it into place: a fresh
	# inode each time. Overwriting the binary in place invalidates the running
	# Mach-O's code signature on macOS, which SIGKILLs the next invocation.
	install -m 0755 ./build/tapes $(shell go env GOBIN)/tapes

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

# --- Local DB-backed tests -------------------------------------------------
#
# The DB-backed suites read TEST_POSTGRES_DSN and fail without it, so `go test
# ./...` on a bare workstation reports a wall of BeforeEach failures that look
# like broken code. These targets stand up the SAME Postgres the Dagger
# pipeline uses, so a local run and a CI run agree.
#
# "the same" is load-bearing: the CI image ships pgvector and pg_duckdb, and a
# stock postgres image passes most of the suite while failing pkg/spanembed on
# a missing extension — a mystery failure that reads as a code bug. Matching
# the image is the difference between a local run you can trust and one that
# lies to you in a specific, hard-to-diagnose way.
#
# The image and credentials are derived from .dagger/postgres.go rather than
# restated here: that file is what CI actually runs, so it stays the single
# source of truth and this can't drift from it.
DAGGER_PG_SRC := .dagger/postgres.go
pg_const = $(shell sed -n 's/^[[:space:]]*$(1)[[:space:]]*=[[:space:]]*"\(.*\)".*/\1/p' $(DAGGER_PG_SRC))

TEST_PG_IMAGE := $(call pg_const,postgresImage)
TEST_PG_USER  := $(call pg_const,testPgUser)
TEST_PG_PASS  := $(call pg_const,testPgPass)
TEST_PG_DB    := $(call pg_const,testPgDB)

TEST_PG_CONTAINER ?= tapes-test-postgres
TEST_PG_HOST_PORT ?= 55432
TEST_POSTGRES_DSN ?= postgres://$(TEST_PG_USER):$(TEST_PG_PASS)@127.0.0.1:$(TEST_PG_HOST_PORT)/$(TEST_PG_DB)?sslmode=disable
PKG ?= ./...

.PHONY: test-db-up
test-db-up: ## Starts a local Postgres matching the Dagger CI service (idempotent)
	$(call print-target)
	@if [ -z "$(TEST_PG_IMAGE)" ]; then \
		echo "could not read postgresImage from $(DAGGER_PG_SRC)."; \
		echo "The constant moved or was renamed — update pg_const in the makefile"; \
		echo "rather than hardcoding an image here, or local and CI will diverge."; \
		exit 1; \
	fi
	@if [ -n "$$(docker ps -q -f name='^$(TEST_PG_CONTAINER)$$')" ]; then \
		echo "$(TEST_PG_CONTAINER) already running"; \
	else \
		docker rm -f $(TEST_PG_CONTAINER) >/dev/null 2>&1 || true; \
		echo "starting $(TEST_PG_IMAGE)"; \
		docker run -d --name $(TEST_PG_CONTAINER) \
			-e POSTGRES_USER=$(TEST_PG_USER) \
			-e POSTGRES_PASSWORD=$(TEST_PG_PASS) \
			-e POSTGRES_DB=$(TEST_PG_DB) \
			-p $(TEST_PG_HOST_PORT):5432 \
			$(TEST_PG_IMAGE) >/dev/null; \
	fi
	@printf 'waiting for postgres'; \
	for i in $$(seq 1 60); do \
		if docker exec $(TEST_PG_CONTAINER) pg_isready -U $(TEST_PG_USER) >/dev/null 2>&1; then \
			printf ' ready\n'; exit 0; \
		fi; \
		printf '.'; sleep 1; \
	done; \
	printf '\n'; echo "postgres did not become ready; see: docker logs $(TEST_PG_CONTAINER)"; exit 1

.PHONY: test-db-down
test-db-down: ## Stops and removes the local test Postgres
	$(call print-target)
	@docker rm -f $(TEST_PG_CONTAINER) >/dev/null 2>&1 && echo "removed $(TEST_PG_CONTAINER)" || echo "$(TEST_PG_CONTAINER) not running"

# GOEXPERIMENT is set explicitly below, as in build-local: the flake's dev
# shell provides it, but a bare shell does not, and pkg/derive|merkle|storage
# then fail to build with errors pointing at encoding/json/v2 rather than at
# the missing experiment.
.PHONY: test-local
test-local: test-db-up ## Runs the full suite locally against the CI Postgres (PKG=./pkg/... to scope)
	$(call print-target)
	@echo "TEST_POSTGRES_DSN=$(TEST_POSTGRES_DSN)"
	GOEXPERIMENT=jsonv2 TEST_POSTGRES_DSN="$(TEST_POSTGRES_DSN)" go test $(PKG)
	@echo "postgres left running for fast re-runs; 'make test-db-down' to stop it"

.PHONY: help
.DEFAULT_GOAL := help
help: ## Prints this help message
	@grep -h -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

define print-target
    @printf "Executing target: \033[36m$@\033[0m\n"
endef
