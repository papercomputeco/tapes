.PHONY: run test format deps

run: ## Runs the cassette on 127.0.0.1:9999
	go run .

test: ## Runs the example tests
	go test ./...

deps: ## Updates module dependencies
	go mod tidy

format: ## Formats the example
	gofmt -w .

.PHONY: help
.DEFAULT_GOAL := help
help: ## Prints this help message
	@grep -h -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
