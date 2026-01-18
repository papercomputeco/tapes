# Based around the auto-documented Makefile:
# http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html

GO_BUILD_FLAGS = -ldflags="-s -w"

.PHONY: build-dir
build-dir:
	@mkdir -p build

.PHONY: build
build: build-proxy ## Builds all artifacts

.PHONY: build-proxy
build-proxy: | build-dir ## Build proxy artifact
	$(call print-target)
	go build -o build/tapesprox ${GO_BUILD_FLAGS} ./cmd/proxy

.PHONY: clean
clean: ## Removes the "build" directory with built artifacts
	$(call print-target)
	@rm -rf ./build

.PHONY: unit-test
unit-test: ## Runs unit tests via "go test"
	$(call print-target)
	go test -v ./...

.PHONY: test-integration
test-integration: build ## Runs integration tests against Ollama
	$(call print-target)
	./scripts/test-ollama.sh

.PHONY: demo
demo: build ## Runs interactive demo showcasing tamper-proof audit trails
	$(call print-target)
	./scripts/demo.sh

.PHONY: run
run: build ## Runs the proxy with debug logging (in-memory storage)
	$(call print-target)
	./build/tapesprox -debug

.PHONY: run-persist
run-persist: build ## Runs the proxy with SQLite persistence
	$(call print-target)
	./build/tapesprox -debug -db tapes.db

.PHONY: help
.DEFAULT_GOAL := help
help: ## Prints this help message
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

define print-target
    @printf "Executing target: \033[36m$@\033[0m\n"
endef
