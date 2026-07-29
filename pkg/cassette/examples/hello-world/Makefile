IMAGE ?= tapes/hello-world-cassette:0.0.1

.PHONY: build
build: ## Builds the cassette binary
	go build -o build/hello-world .

.PHONY: image
image: ## Builds the cassette container image
	docker build -t $(IMAGE) .

.PHONY: test
test: ## Vets and type-checks
	go vet ./...

.PHONY: clean
clean: ## Removes built artifacts
	rm -rf build/

.PHONY: help
.DEFAULT_GOAL := help
help: ## Prints this help message
	@grep -h -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
