# Based around the auto-documented Makefile:
# http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html

VERSION ?= dev

GO_BUILD_FLAGS = -ldflags="-s -w"

.PHONY: format
format:
	find . -type f -name "*.go" -exec goimports -local github.com/papercompute/tapes -w {} \;

.PHONY: build-dir
build-dir:
	@mkdir -p build

.PHONY: build
build: build-proxy ## Builds all artifacts

.PHONY: build-containers
build-containers: build-proxy-container ## Builds all container artifacts

.PHONY: build-proxy
build-proxy: | build-dir ## Build proxy artifact
	$(call print-target)
	go build -o build/tapesprox ${GO_BUILD_FLAGS} ./cmd/proxy

.PHONY: build-proxy-container
build-proxy-container: ## Build the tapesprox container artifact
	$(call print-target)
	docker build -f dockerfiles/tapesprox.Dockerfile \
		-t tapes/proxy:$(VERSION) \
		-t tapes/proxy:latest \
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
