# Based around the auto-documented Makefile:
# http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html

VERSION ?= $(shell git describe --tags --always --dirty)
COMMIT  := $(shell git rev-parse HEAD)
BUILDTIME ?= $(shell date -u '+%Y-%m-%d %H:%M:%S')

LDFLAGS := -s -w \
	-X 'github.com/papercomputeco/tapes/pkg/utils.Version=$(VERSION)' \
	-X 'github.com/papercomputeco/tapes/pkg/utils.Sha=$(COMMIT)' \
	-X 'github.com/papercomputeco/tapes/pkg/utils.Buildtime=$(BUILDTIME)'

.PHONY: format
format:
	find . -type f -name "*.go" -exec goimports -local github.com/papercomputeco/tapes -w {} \;

.PHONY: check-lint
check-lint: ## Runs golangci-lint check. Auto-fixes are not automatically applied.
	$(call print-target)
	dagger call check-lint

.PHONY: fix-lint
fix-lint: ## Runs golangci-lint lint with auto-fixes applied.
	$(call print-target)
	dagger call fix-lint export --path .

.PHONY: generate
generate: ## Regenerates ent code from schema
	go generate ./pkg/storage/ent/...

.PHONY: build-local
build-local: ## Builds local artifacts with local toolchain
	$(call print-target)
	@mkdir -p ./build
	CGO_ENABLED=1 GOEXPERIMENT=jsonv2 go build -ldflags "$(LDFLAGS)" -o ./build/ ./cli/tapes
	CGO_ENABLED=1 GOEXPERIMENT=jsonv2 go build -ldflags "$(LDFLAGS)" -o ./build/ ./cli/tapesprox
	CGO_ENABLED=1 GOEXPERIMENT=jsonv2 go build -ldflags "$(LDFLAGS)" -o ./build/ ./cli/tapesapi

.PHONY: install
install: build-local ## Builds local artifacts and installs to configured $GOPATH
	$(call print-target)
	cp ./build/tapes $(shell go env GOBIN)
	cp ./build/tapesprox $(shell go env GOBIN)
	cp ./build/tapesapi $(shell go env GOBIN)

.PHONY: build
build: ## Builds all cross-platform artifacts - Warning! MacOS may fail cross compiling toolchain dependency
	dagger call \
		build-release \
			--version ${VERSION} \
			--commit ${COMMIT} \
		export \
			--path ./build

.PHONY: nightly
nightly: ## Builds and releases nightly tapes artifacts
	dagger call \
		nightly \
			--commit=${COMMIT} \
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
			--endpoint=env://BUCKET_ENDPOINT \
			--bucket=env://BUCKET_NAME \
			--access-key-id=env://BUCKET_ACCESS_KEY_ID \
			--secret-access-key=env://BUCKET_SECRET_ACCESS_KEY

.PHONY: up
up:
	docker compose up --build

.PHONY: build-containers
build-containers: build-tapes-container build-api-container build-proxy-container ## Builds all container artifacts

.PHONY: build-tapes-container
build-tapes-container: ## Build the tapes container artifact
	$(call print-target)
	docker build -f dockerfiles/tapes.Dockerfile \
		-t papercomputeco/tapes:$(VERSION) \
		-t papercomputeco/tapes:latest \
		.

.PHONY: build-api-container
build-api-container: ## Build the tapesapi container artifact
	$(call print-target)
	docker build -f dockerfiles/tapesapi.Dockerfile \
		-t papercomputeco/api:$(VERSION) \
		-t papercomputeco/api:latest \
		.

.PHONY: build-proxy-container
build-proxy-container: ## Build the tapesprox container artifact
	$(call print-target)
	docker build -f dockerfiles/tapesprox.Dockerfile \
		-t papercomputeco/proxy:$(VERSION) \
		-t papercomputeco/proxy:latest \
		.

.PHONY: clean
clean: ## Removes the "build" directory with built artifacts
	$(call print-target)
	@rm -rf ./build

.PHONY: unit-test
unit-test: ## Runs unit tests via "go test"
	$(call print-target)
	dagger call test

.PHONY: help
.DEFAULT_GOAL := help
help: ## Prints this help message
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

define print-target
    @printf "Executing target: \033[36m$@\033[0m\n"
endef
