#@ Helpers
# from https://www.thapaliya.com/en/writings/well-documented-makefiles/
help:  ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Tools
tools: ## Installs required binaries locally
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/actgardner/gogen-avro/v10/cmd/...@latest
	go install github.com/jstemmer/go-junit-report@latest

##@ Tests
test: ## Run unit tests
	@echo "== unit test"
	go test -cover -v ./...

##@ Tests
test-ci: tools ## Run unit tests
	@echo "== unit test"
	go test -cover 2>&1 ./... | go-junit-report -set-exit-code > junit.xml

##@ Run static checks
check: tools ## Runs lint, fmt and vet checks against the codebase
	golangci-lint run
	go fmt ./...
	go vet ./...

##@ Coverage
coverage: ## Runs code coverage
	go test -v -coverprofile=cover.out -covermode=atomic ./...
	go tool cover -html=cover.out -o cover.html