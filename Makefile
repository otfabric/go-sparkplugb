.DEFAULT_GOAL := help

.PHONY: help all build fmt fmt-check lint lint-ci vet vuln test stress coverage coverage-pkg-check cover coverage-gate fuzz bench e2e generate generate-check cross check clean test-interop interop

help: ## This help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

export GOWORK := off

GO_MOD_VERSION := $(shell sed -nE 's/^go ([0-9]+\.[0-9]+).*/\1/p' go.mod | head -n1)
GOFMT_CI = $(shell GOTOOLCHAIN=go$(GO_MOD_VERSION).0 go env GOROOT)/bin/gofmt

BIN_DIR := bin
# Library packages: exclude examples and proto (generated Protobuf code).
TEST_PKGS := $(shell go list ./... | grep -vE '/examples|/proto$$')
COVER_PKGS := $(shell go list ./... | grep -vE '/examples|/proto$$' | paste -sd, -)
COVER_MIN ?= 90
# Per-package floor (TEST-012), over the packages of COVER_PKGS.
COVER_PKG_MIN := 80

all: build ## Default target: build the examples

build: ## Build the examples into ./bin
	@echo "Building examples"
	@mkdir -p $(BIN_DIR)
	@for dir in examples/*/; do \
		name="$$(basename "$$dir")"; \
		go build -o "$(BIN_DIR)/example-$$name" "./$$dir"; \
	done

GO_FILES = $(shell git ls-files --cached --others --exclude-standard -- '*.go')

fmt: ## Format Go code with the gofmt of the Go version in go.mod
	@echo "Running gofmt (go$(GO_MOD_VERSION))"
	@$(GOFMT_CI) -w $(GO_FILES)

fmt-check: ## Fail if gofmt would change any file
	@echo "Checking formatting (gofmt go$(GO_MOD_VERSION) and local $$(go env GOVERSION))"
	@ci="$$($(GOFMT_CI) -l $(GO_FILES))"; loc="$$(gofmt -l $(GO_FILES))"; \
	if [ -n "$$ci$$loc" ]; then \
		[ -z "$$ci" ] || { echo "Not formatted for gofmt go$(GO_MOD_VERSION) (CI):"; echo "$$ci"; }; \
		[ -z "$$loc" ] || { echo "Not formatted for the local gofmt:"; echo "$$loc"; }; \
		echo "Run 'make fmt'."; \
		exit 1; \
	fi

lint: ## Run staticcheck
	@echo "Running staticcheck"
	@staticcheck ./...

lint-ci: ## Run golangci-lint (see .golangci.yaml)
	@echo "Running golangci-lint"
	@golangci-lint run ./...

vet: ## Run go vet
	@echo "Running go vet"
	@go vet ./...

vuln: ## Run govulncheck
	@echo "Running govulncheck"
	@govulncheck ./...

test: ## Run the library tests with the race detector
	@echo "Running tests"
	@go test -count=1 -race $(TEST_PKGS)

stress: ## Run the library tests shuffled, twice, with the race detector
	@echo "Running the tests shuffled, twice, with the race detector"
	@go test -count=2 -race -shuffle=on -timeout=30m $(TEST_PKGS)

coverage: ## Run tests with coverage (writes coverage.out)
	@echo "Running coverage"
	@go test -count=1 -race -coverprofile=coverage.out -covermode=atomic -coverpkg=$(COVER_PKGS) $(TEST_PKGS)
	@go tool cover -func=coverage.out | tail -n1

coverage-pkg-check: coverage ## Fail when a package of COVER_PKGS is below COVER_PKG_MIN (reads coverage.out)
	@awk -v m="$(COVER_PKG_MIN)" -v pkgs="$(COVER_PKGS)" 'BEGIN { split(pkgs, l, ","); for (i in l) want[l[i]] = 1 } \
	NR > 1 { n[$$1] = $$2; if ($$3 > 0) h[$$1] = 1 } \
	END { for (k in n) { p = k; sub(/\/[^\/]*$$/, "", p); if (p in want) { s[p] += n[k]; if (k in h) c[p] += n[k] } } \
	for (p in s) { v = 100 * c[p] / s[p]; if (v < m) { printf "%s: %.1f%% < %s%%\n", p, v, m; bad = 1 } } exit bad }' coverage.out

coverage-gate: coverage ## Fail if coverage is below COVER_MIN percent
	@total=$$(go tool cover -func=coverage.out | awk '/^total:/ {sub("%", "", $$3); print $$3}'); \
	if awk -v t="$$total" -v m="$(COVER_MIN)" 'BEGIN { exit !(t + 0 >= m + 0) }'; then \
		echo "Coverage $$total% meets the gate of $(COVER_MIN)%"; \
	else \
		echo "Coverage $$total% is below the gate of $(COVER_MIN)%"; exit 1; \
	fi

cover: coverage ## Open coverage report in browser
	@echo "Opening coverage report"
	@go tool cover -html=coverage.out

FUZZTIME ?= 30s
FUZZ_TARGETS := ./payload:FuzzDecode ./payload:FuzzDecodeBooleanArray ./payload:FuzzDecodeStringArray ./topic:FuzzParse

fuzz: ## Run the fuzz targets for FUZZTIME each (default 30s)
	@echo "Fuzzing (FUZZTIME=$(FUZZTIME))"
	@for t in $(FUZZ_TARGETS); do \
		pkg=$${t%%:*}; name=$${t##*:}; \
		echo "$$pkg $$name"; \
		go test -run '^$$' -fuzz "^$$name$$" -fuzztime $(FUZZTIME) "$$pkg" || exit 1; \
	done

bench: ## Run the benchmarks
	@go test -run '^$$' -bench . -benchmem $(TEST_PKGS)

e2e: ## The runtimes over Paho against a fake broker on loopback TCP
	@go test -count=1 -race -run '^TestE2E' ./mqtt

generate: ## Regenerate Protobuf code
	@echo "Generating Protobuf code"
	@go generate ./proto/...

generate-check: generate ## Fail if regenerating changes the committed code
	@git diff --exit-code --stat -- proto/ || { echo "Generated code is out of date: run make generate and commit the result."; exit 1; }

test-interop: ## Run the edge/host/app against the sparkplugb-interop references and brokers pinned in interop/images.go (needs Docker; INTEROP_TESTFLAGS=-v)
	@echo "Running interop tests"
	@cd interop && go vet -tags interop ./... && golangci-lint run --build-tags interop ./... && \
		SPARKPLUGB_REQUIRE_INTEROP=1 go test -tags interop -race -count=1 -timeout=30m $(INTEROP_TESTFLAGS) ./...

interop: test-interop ## Alias for test-interop

cross: ## Cross-compile for common platforms
	GOOS=linux   GOARCH=amd64 go build ./...
	GOOS=linux   GOARCH=arm64 go build ./...
	GOOS=darwin  GOARCH=amd64 go build ./...
	GOOS=darwin  GOARCH=arm64 go build ./...
	GOOS=windows GOARCH=amd64 go build ./...

check: fmt fmt-check lint lint-ci vet vuln test coverage-gate coverage-pkg-check ## Run format + lint + vet + vuln + test + coverage gate

clean: ## Remove generated binaries and coverage artifacts
	@echo "Cleaning up"
	@rm -rf $(BIN_DIR) coverage.out coverage.html
