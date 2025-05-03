# Format all files

goimports:
	@echo "==> Formatting imports"
	@goimports -e -l -w -local github.com/hamba/avro $(shell find . -type f -name '*.go' -not -path "./vendor/*")
	@echo "==> Done"

.PHONY: goimports

fmt: goimports
	@echo "==> Formatting source"
	@gofumpt -l -extra -w $(shell find . -type f -name '*.go' -not -path "./vendor/*")
	@echo "==> Done"
.PHONY: fmt

# Tidy the go.mod file
tidy:
	@echo "==> Cleaning go.mod"
	@go mod tidy
	@echo "==> Done"
.PHONY: tidy

# Run all tests
test:
	@go test -cover -race ./...
.PHONY: test

# Lint the project
lint:
	@golangci-lint run ./...
.PHONY: lint

# Run CI tasks
ci: lint test
.PHONY: ci
