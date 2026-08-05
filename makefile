# Based around the auto-documented Makefile:
# http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html

VERSION ?= $(shell git describe --tags --always --dirty)
COMMIT  := $(shell git rev-parse HEAD)
BUILDTIME ?= $(shell date -u '+%Y-%m-%d %H:%M:%S')
REGISTRY ?= public.ecr.aws/g4e5l3z3/papercomputeco
IMAGE ?= tapes:dev

# extproc does NOT ship from REGISTRY. tapes and postgres are published to ECR
# Public, whose repositories are enumerated in terraform and carry an anonymous
# pull policy; tapes-extproc is a private ECR repository in the Tooling account
# that the data plane pulls via the cross-account role. The two are different
# registries with different auth, so they get different variables — pointing
# extproc at REGISTRY produces a push the release role has no permission for.
EXTPROC_REGISTRY ?= 952121199601.dkr.ecr.us-east-1.amazonaws.com
EXTPROC_IMAGE ?= tapes-extproc:dev

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

# tapes publishes two contracts — the read API and the ingest write surface are
# different servers with different trust models, see the header of
# ingest/openapi.go — and each server compiles its own from the routes it
# registered and serves it at GET /openapi. Neither is generated into a checked-in
# file, so neither can be stale, so there is nothing to regenerate or verify.
#
# `contracts` below writes the documents themselves, with per-field prose folded
# in from a checkout, for consumers that want bytes on disk. Still not checked
# in, and still read by nothing here.

.PHONY: contracts
contracts: ## Compiles both published OpenAPI contracts into ./build/contracts
	$(call print-target)
	dagger call contracts export --path ./build/contracts

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
build-images: build-tapes-image build-extproc-image ## Builds all container artifacts

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

.PHONY: parity
parity: ## Runs the envelope contract fixture gates (no services needed)
	$(call print-target)
	dagger call check-parity

.PHONY: test-extproc
test-extproc: ## Runs the ext_proc adapter's test suite (no services needed)
	$(call print-target)
	dagger call check-extproc

.PHONY: check-extproc-image
check-extproc-image: ## Builds the tapes-extproc image without loading it (CI gate)
	$(call print-target)
	dagger call \
		build-extproc-image \
			--version=${VERSION} \
			--commit=${COMMIT} \
		sync

.PHONY: build-extproc-image
build-extproc-image: ## Builds and loads the tapes-extproc image locally (EXTPROC_IMAGE=tapes-extproc:dev)
	$(call print-target)
	dagger call \
		build-extproc-image \
			--version=${VERSION} \
			--commit=${COMMIT} \
		export-image \
			--name=${EXTPROC_IMAGE}

.PHONY: build-local-extproc-image
# Cross-repo clearing contract; keep the Dagger invocation owned by
# build-extproc-image.
build-local-extproc-image: ## Build a local tapes-extproc image for Kind/clearing
	$(MAKE) build-extproc-image EXTPROC_IMAGE="$(EXTPROC_IMAGE)"

.PHONY: build-push-extproc-images
build-push-extproc-images: ## Builds and publishes the multi-arch tapes-extproc container images
	dagger call \
		build-push-extproc-images \
			--registry=${EXTPROC_REGISTRY} \
			--tags=${VERSION} \
			--tags=latest \
			--version=${VERSION} \
			--commit=${COMMIT}

.PHONY: e2e-test
e2e-test: ## Runs end-to-end tests with Postgres and Ollama via Dagger
	$(call print-target)
	dagger call test-e-2-e

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
