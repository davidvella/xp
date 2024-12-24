#@ Helpers
# from https://www.thapaliya.com/en/writings/well-documented-makefiles/
help:  ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Tools
tools: ## Installs required binaries locally
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/actgardner/gogen-avro/v10/cmd/...@latest

##@ Tests
test: check ## Run unit tests
	@echo "== unit test"
	go test -short -cover ./...

##@ Run static checks
check: tools ## Runs lint, fmt and vet checks against the codebase
	golangci-lint run
	go fmt ./...
	go vet ./...

##@ Generate
generate: tools ## Generates the avro in the /avro folder
	go generate ./...

run-docker:
	docker-compose up --build --force-recreate --no-deps