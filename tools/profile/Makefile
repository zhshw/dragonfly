PROFILE_ROOT ?= /data/dragonfly-observability
PROFILE_LISTEN ?= 0.0.0.0:8787
WORKFLOW_CONFIG ?= $(PROFILE_ROOT)/config/profile.json
PROFILE_EXECUTOR_DIR ?= $(PROFILE_ROOT)/config/executor
BIN_DIR ?= $(HOME)/.local/bin
WEB_DIST ?= $(CURDIR)/web/dist
GO ?= go
NPM ?= npm
COMPOSE ?= docker compose
STACK = $(COMPOSE) --env-file "$(PROFILE_ROOT)/config/langfuse.env" -f compose.yaml

.PHONY: up restart web executor langfuse build
up: web
	$(MAKE) executor

restart: PROFILE_UP_FLAGS = --no-deps --force-recreate
restart: web

web:
	@set -eu; \
		export PROFILE_UID=$$(id -u) PROFILE_GID=$$(id -g); \
		export PROFILE_BINARY="$(abspath $(BIN_DIR))/profile-web"; \
		export PROFILE_ROOT="$(PROFILE_ROOT)" PROFILE_LISTEN="$(PROFILE_LISTEN)" WEB_DIST="$(WEB_DIST)"; \
		export PROFILE_EXECUTOR_DIR="$(PROFILE_EXECUTOR_DIR)"; \
		test -x "$$PROFILE_BINARY" || { echo "Missing executable $$PROFILE_BINARY" >&2; exit 1; }; \
		test -r "$$WEB_DIST/index.html" || { echo "Missing $$WEB_DIST/index.html" >&2; exit 1; }; \
		install -d -m 0700 "$$PROFILE_EXECUTOR_DIR"; \
		$(STACK) up -d --wait --wait-timeout 180 $(PROFILE_UP_FLAGS) profile-web

executor:
	@set -eu; \
		socket="$(PROFILE_EXECUTOR_DIR)/executor.sock"; \
		if ! systemctl --user is-active --quiet profile-executor.service; then \
			bin="$(abspath $(BIN_DIR))/profile"; \
			test -x "$$bin" || { echo "Missing executable $$bin" >&2; exit 1; }; \
			test -r "$(WORKFLOW_CONFIG)" || { echo 'Missing workflow config $(WORKFLOW_CONFIG)' >&2; exit 1; }; \
			install -d -m 0700 "$(PROFILE_EXECUTOR_DIR)"; \
			storage=$$($(STACK) config --environment | sed -n 's/^LANGFUSE_STORAGE_ROOT=//p'); \
			test -n "$$storage" || { echo 'Missing LANGFUSE_STORAGE_ROOT' >&2; exit 1; }; \
			systemd-run --user --collect --unit=profile-executor --property=Type=exec \
			  --property=Restart=on-failure --property=RestartSec=2s --property=KillMode=mixed \
			  --working-directory="$(CURDIR)" --setenv=PATH="$$PATH" \
			  -- "$$bin" executor -socket "$$socket" -workflow-config "$(WORKFLOW_CONFIG)" \
			  -dsn-file "$(PROFILE_ROOT)/config/raft-workflow.dsn" \
			  -data "$$storage/raft-workflow/experiments"; \
		fi; \
		curl --noproxy '*' --unix-socket "$$socket" --fail --silent --retry 20 \
		  --retry-all-errors --retry-delay 1 --max-time 2 http://profile-executor/health || \
		  { echo 'Host executor did not become ready. Check journalctl --user -u profile-executor' >&2; exit 1; }

langfuse:
	$(STACK) up -d --wait --wait-timeout 180

build:
	mkdir -p "$(BIN_DIR)"
	GOWORK=off $(GO) build -o "$(BIN_DIR)/profile" ./cmd/cli
	GOWORK=off $(GO) build -o "$(BIN_DIR)/profile-web" ./cmd/web
	$(NPM) --prefix web ci
	$(NPM) --prefix web run build
