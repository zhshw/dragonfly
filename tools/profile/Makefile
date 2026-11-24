PROFILE_ROOT ?= /data/dragonfly-observability
PROFILE_LISTEN ?= 127.0.0.1:8787
WORKFLOW_CONFIG ?= $(PROFILE_ROOT)/config/profile.json
BIN_DIR ?= $(HOME)/.local/bin
WEB_DIST ?= $(CURDIR)/web/dist
GO ?= go
NPM ?= npm

.PHONY: up langfuse build
up: langfuse
	@test -x "$(BIN_DIR)/profile-web" && test -r "$(WEB_DIST)/index.html" || { echo 'Run make build first' >&2; exit 1; }
	@test -r "$(WORKFLOW_CONFIG)" || { echo 'Missing workflow config $(WORKFLOW_CONFIG)' >&2; exit 1; }
	@set -eu; \
		dsn="$(PROFILE_ROOT)/config/raft-workflow.dsn"; \
		previous=$$(cksum 2>/dev/null < "$$dsn" || true); \
		sudo bash ops/setup-postgres.sh "$(PROFILE_ROOT)"; \
		if ! systemctl is-active --quiet profile-web.service; then \
			sudo systemd-run --unit=profile-web --collect --uid="$$(id -un)" \
				--property=Type=exec --property=Restart=on-failure --working-directory="$(CURDIR)" \
				"$(BIN_DIR)/profile-web" -dsn-file "$$dsn" -listen "$(PROFILE_LISTEN)" \
				-data "$(PROFILE_ROOT)/state/raft-workflow/experiments" -web "$(WEB_DIST)" \
				-workflow-config "$(WORKFLOW_CONFIG)"; \
		elif [ "$$previous" != "$$(cksum < "$$dsn")" ]; then \
			sudo systemctl restart profile-web.service; \
		fi
	@curl --fail --silent --retry 10 --retry-connrefused --retry-delay 1 --max-time 5 \
		"http://$(PROFILE_LISTEN)/api/v1/experiments" >/dev/null || { echo 'profile-web did not become ready' >&2; exit 1; }
	@echo "profile http://$(PROFILE_LISTEN)/"

langfuse:
	sudo docker compose --progress plain --env-file "$(PROFILE_ROOT)/config/langfuse.env" -f compose.yaml up -d --wait --wait-timeout 180

build:
	mkdir -p "$(BIN_DIR)"
	GOWORK=off $(GO) build -o "$(BIN_DIR)/profile" ./cmd/cli
	GOWORK=off $(GO) build -o "$(BIN_DIR)/profile-web" ./cmd/web
	$(NPM) --prefix web ci
	$(NPM) --prefix web run build
