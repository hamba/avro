# Format all files
fmt:
	@echo "==> Formatting source"
	@golangci-lint fmt ./...
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
