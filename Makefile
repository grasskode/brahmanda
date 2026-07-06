GO          ?= go
BIN         ?= bin
INSTALL_DIR ?= $(HOME)/.local/bin

.PHONY: build brahma srishti chitra install vet test tidy fmt clean help

# brahma — a config-driven pipeline orchestrator. Three binaries:
#   * brahma  — the creator: long-running daemon. Loads config.yaml and
#               lazy-fills each pipeline's worker pool by exec'ing srishti.
#   * srishti — the runner: short-lived, one per worker. Execs the pipeline
#               command, enforces the timeout, routes NDJSON stdout into
#               the journal.
#   * chitra  — the record-keeper: read-only monitor over the state layout.
build: brahma srishti chitra

brahma:
	$(GO) build -o $(BIN)/brahma ./cmd/brahma

srishti:
	$(GO) build -o $(BIN)/srishti ./cmd/srishti

chitra:
	$(GO) build -o $(BIN)/chitra ./cmd/chitra

install: build
	mkdir -p $(INSTALL_DIR)
	install -m 0755 $(BIN)/brahma  $(INSTALL_DIR)/brahma
	install -m 0755 $(BIN)/srishti $(INSTALL_DIR)/srishti
	install -m 0755 $(BIN)/chitra  $(INSTALL_DIR)/chitra
	@echo "Installed to $(INSTALL_DIR). Ensure it is on PATH."

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

tidy:
	$(GO) mod tidy

fmt:
	$(GO) fmt ./...

clean:
	rm -rf $(BIN)

help:
	@echo "Targets:"
	@echo "  build    build all three binaries into ./$(BIN) (default)"
	@echo "  brahma   build only ./$(BIN)/brahma (the creator — orchestrator daemon)"
	@echo "  srishti  build only ./$(BIN)/srishti (the runner — per-worker subprocess)"
	@echo "  chitra   build only ./$(BIN)/chitra (the record-keeper — monitor)"
	@echo "  install  copy binaries to $(INSTALL_DIR)"
	@echo "  vet      go vet ./..."
	@echo "  test     go test ./..."
	@echo "  tidy     go mod tidy"
	@echo "  fmt      go fmt ./..."
	@echo "  clean    rm -rf $(BIN)"
