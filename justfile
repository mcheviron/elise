set shell := ["bash", "-lc"]

install:
    @echo "Installing development tools..."
    @if ! command -v goimports >/dev/null 2>&1; then \
        echo "Installing goimports..."; \
        go install golang.org/x/tools/cmd/goimports@latest; \
    fi
    @if ! command -v air >/dev/null 2>&1; then \
        echo "Installing air..."; \
        go install github.com/air-verse/air@latest; \
    fi
    @if ! command -v deadcode >/dev/null 2>&1; then \
        echo "Installing deadcode..."; \
        go install golang.org/x/tools/cmd/deadcode@latest; \
    fi
    @if ! command -v staticcheck >/dev/null 2>&1; then \
        echo "Installing staticcheck..."; \
        go install honnef.co/go/tools/cmd/staticcheck@latest; \
    fi
    @echo "✓ All tools installed"

fmt:
    @echo "Formatting..."
    @goimports -w .
    @echo "✓ Formatting complete"

vet:
    @echo "Vetting..."
    @go vet ./...
    @echo "✓ Go vet passed"

build:
    @echo "Building..."
    @go build ./...
    @echo "✓ Build successful"

test:
    @echo "Running tests..."
    @go test ./...
    @echo "✓ Tests passed"

run *args:
    @if ! command -v air >/dev/null 2>&1; then \
        echo "Installing air..."; \
        go install github.com/air-verse/air@latest; \
    fi
    @echo "Starting server with live reload..."
    @air {{args}}

check:
    @echo "Running code checks..."
    @echo ""
    @echo "Running goimports..."
    @goimports -w .
    @echo "✓ goimports complete"
    @echo ""
    @echo "Running go vet..."
    @go vet ./...
    @echo "✓ Go vet passed"
    @echo ""
    @echo "Running go build..."
    @go build ./...
    @echo "✓ Go build passed"
    @echo ""
    @echo "Running go mod tidy..."
    @go mod tidy
    @echo "✓ Go mod tidy complete"
    @echo ""
    @echo "Running deadcode..."
    @deadcode ./...
    @echo "✓ Deadcode check complete"
    @echo ""
    @echo "Running staticcheck..."
    @staticcheck ./...
    @echo "✓ Staticcheck passed"
    @echo ""
    @echo "✓ All checks complete"
