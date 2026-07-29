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
	dagger call fix-lint export --path . --quiet

# tapes publishes TWO contracts, because the read API and the ingest write
# surface are different servers with different trust models — see the header of
# ingest/swagger.go. swag scans the whole module for annotations regardless of
# which general-info file -g names, so each invocation excludes the other's
# handlers; without that, one document would absorb both surfaces and imply the
# ingest endpoints are reachable from wherever the read API is.
.PHONY: swag
swag: ## Runs the swaggo/swag utility for generating the swagger yaml
	swag init \
		--parseDependency \
		--parseInternal \
		--exclude ingest \
		-g api/swagger.go \
		-o docs
	swag init \
		--parseDependency \
		--parseInternal \
		--exclude api \
		--instanceName ingest \
		-g ingest/swagger.go \
		-o docs/ingest

.PHONY: swagfmt
swagfmt: ## Runs swaggo/swag for formatting swag godoc comments
	swag fmt

.PHONY: openapi
openapi: swag ## Regenerates both OpenAPI 3.0.3 contracts (api/ and ingest/) from the swag docs
	go run ./cmd/gen-openapi
	go run ./cmd/gen-openapi -in docs/ingest/ingest_swagger.json -out ingest/openapi.yaml

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

.PHONY: test-run-id
test-run-id: ## Runs tests via "go test" in the Dagger services environment
	$(call print-target)
	dagger call test --run-id="$$(date +%s)"

.PHONY: e2e-test
e2e-test: ## Runs end-to-end tests with Postgres and Ollama via Dagger
	$(call print-target)
	dagger call test-e-2-e

# --- Local DB-backed tests -------------------------------------------------
#
# The DB-backed suites read TEST_POSTGRES_DSN. In the nix dev shell that is
# exported for you (flake.nix) pointing at the docker-compose postgres service,
# so once that service is up a plain `go test ./...` works. These targets bring
# it up and down; `make test-local` does both in one step.
#
# The service is reused rather than a purpose-built container, because
# docker-compose.yaml already pins the same image the Dagger pipeline binds.
# That agreement is load-bearing: the image carries pgvector and pg_duckdb, and
# a stock postgres passes most of the suite while failing pkg/spanembed on a
# missing extension — an environment gap that reads as a code bug in a package
# you probably did not touch. test-db-up asserts the two pins still match
# rather than trusting that they do.
DAGGER_PG_SRC  := .dagger/postgres.go
COMPOSE_PG_SRC := docker-compose.yaml
PKG ?= ./...

.PHONY: test-db-check
test-db-check: ## Verifies docker-compose and the Dagger CI service pin the same Postgres image
	@dagger_img=$$(sed -n 's/^[[:space:]]*postgresImage[[:space:]]*=[[:space:]]*"\(.*\)".*/\1/p' $(DAGGER_PG_SRC)); \
	compose_img=$$(sed -n 's/^[[:space:]]*image:[[:space:]]*\(.*postgres[^[:space:]]*\).*/\1/p' $(COMPOSE_PG_SRC) | head -1); \
	if [ -z "$$dagger_img" ] || [ -z "$$compose_img" ]; then \
		echo "could not read the postgres image from $(DAGGER_PG_SRC) and/or $(COMPOSE_PG_SRC)."; \
		echo "One of them moved. Fix the extraction here rather than hardcoding an image,"; \
		echo "or the local database will quietly stop matching CI."; \
		exit 1; \
	fi; \
	if [ "$$dagger_img" != "$$compose_img" ]; then \
		echo "postgres image mismatch — local tests would not match CI:"; \
		echo "  $(DAGGER_PG_SRC):  $$dagger_img"; \
		echo "  $(COMPOSE_PG_SRC): $$compose_img"; \
		echo "Pin both to the same tag."; \
		exit 1; \
	fi; \
	echo "postgres image matches CI: $$dagger_img"

.PHONY: test-db-up
test-db-up: test-db-check ## Starts the docker-compose Postgres used by the DB-backed tests (idempotent)
	$(call print-target)
	@docker compose up -d postgres
	@printf 'waiting for postgres'; \
	for i in $$(seq 1 60); do \
		if docker compose exec -T postgres pg_isready -U tapes >/dev/null 2>&1; then \
			printf ' ready\n'; exit 0; \
		fi; \
		printf '.'; sleep 1; \
	done; \
	printf '\n'; echo "postgres did not become ready; see: docker compose logs postgres"; exit 1

.PHONY: test-db-down
test-db-down: ## Stops and removes the local test Postgres (keeps its data volume)
	$(call print-target)
	@docker compose rm -sf postgres
	@echo "the postgres-data volume is kept; 'docker compose down -v' also drops it"

# GOEXPERIMENT is set explicitly below, as in build-local: the flake's dev
# shell provides it, but a bare shell does not, and pkg/derive|merkle|storage
# then fail to build with errors pointing at encoding/json/v2 rather than at
# the missing experiment. TEST_POSTGRES_DSN is likewise defaulted here so the
# target works outside the dev shell, while deferring to an existing value.
TEST_POSTGRES_DSN ?= postgres://tapes:tapes@127.0.0.1:5432/tapes?sslmode=disable
GO_TEST_FLAGS ?= -count=1

.PHONY: test-local
test-local: test-db-up ## Runs the suite locally against the CI Postgres (PKG=./pkg/... to scope)
	$(call print-target)
	@echo "TEST_POSTGRES_DSN=$(TEST_POSTGRES_DSN)"
	GOEXPERIMENT=jsonv2 TEST_POSTGRES_DSN="$(TEST_POSTGRES_DSN)" go test $(GO_TEST_FLAGS) $(PKG)
	@echo "postgres left running for fast re-runs; 'make test-db-down' to stop it"

.PHONY: docs-build
docs-build: ## Builds the mdBook documentation
	mdbook build docs

.PHONY: docs-serve
docs-serve: ## Serves the mdBook documentation with live reload
	mdbook serve docs

.PHONY: help
.DEFAULT_GOAL := help
help: ## Prints this help message
	@grep -h -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

define print-target
    @printf "Executing target: \033[36m$@\033[0m\n"
endef
