.PHONY: test test-cover test-integration test-python test-all broker-up broker-down \
	fmt-check lint build cover cover-python tidy vuln sast

# Los tests de integración se saltan solos si no hay un broker alcanzable, así
# que `make test` anda igual sin Docker levantado.
test:
	go test ./...

# Cobertura con reporte por función; el umbral del 70% (DoD) se valida en CI
# (INF-07) con el broker levantado, que es como se mide de verdad: sin él, los
# tests de integración se saltan y la cobertura baja.
#
# Sin -race: en algunos entornos Windows con mingw/gcc desalineado el race
# detector rompe con exit status 0xc0000139 sin relación con el código. Necesita
# CGO + un gcc alineado con la imagen de Go, así que corre en CI, que es Linux.
test-cover:
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out

# Suite de Go contra el RabbitMQ de docker-compose.
test-integration: broker-up
	go test ./... -count=1 -race

# Suite de Python en un container, para no pedir un intérprete ni un venv en la
# máquina de nadie.
test-python:
	docker compose run --rm python-tests

test-all: test-integration test-python

broker-up:
	docker compose up -d --wait rabbitmq

broker-down:
	docker compose down

# --- Targets run by this repo's CI (.github/workflows/ci.yml) ---

# Security tooling is pinned: an analyser that updates itself turns an unrelated
# push red, and CI must run exactly what a developer can run locally.
# gosec v2.21.4 (the version in chat-and-real-time) does not compile with Go 1.25.
GOSEC_VERSION       := v2.22.4
GOVULNCHECK_VERSION := v1.1.4

# The assignment requires at least 70% coverage on backend services.
# CI overrides it with `make cover COVER_MIN=<n>`.
# := rather than ?=: with ?= a stray `export COVER_MIN=0` in someone's shell
# would silently lower the gate. A command-line variable still wins over :=.
COVER_MIN := 70
# The race detector needs CGO + a gcc matching the Go toolchain, which breaks on
# some Windows hosts (see test-cover). CI runs on Linux; turn it off locally
# with `make cover RACE=`. := for the same reason as COVER_MIN.
RACE := -race

fmt-check:
	@unformatted=$$(gofmt -l . | grep -v '^$$' || true); \
		if [ -n "$$unformatted" ]; then echo "not gofmt'd:"; echo "$$unformatted"; exit 1; fi

lint: fmt-check
	go vet ./...

# A library has no binary: this only proves that every package compiles.
build:
	go build ./...

# -p 1 serialises the package test binaries: with -coverpkg and packages running
# in parallel the merged profile comes out incomplete and the total swings
# between runs on identical code.
# -count=1 bypasses the test result cache: its key includes the env vars a test
# reads but not whether the broker is up, so a cached run full of skips could be
# replayed on a run that does have RabbitMQ.
# pubsubtest is measured too: it is public API that the services test against.
cover:
	go test -p 1 -count=1 $(RACE) -coverpkg=./... -coverprofile=coverage.out ./...
	@total=$$(go tool cover -func=coverage.out | awk '/^total:/ {gsub(/%/,"",$$3); print $$3}'); \
		echo "total coverage: $$total% (minimum $(COVER_MIN)%)"; \
		awk -v got="$$total" -v min="$(COVER_MIN)" 'BEGIN { exit (got+0 >= min+0) ? 0 : 1 }' || \
		(echo "coverage below the required minimum" && exit 1)

# The Python client, gated on the same COVER_MIN. Expects the package and its
# dev extras installed (pip install -e "python[dev]"), which CI does; locally,
# test-python runs the same suite in a container instead.
cover-python:
	cd python && python -m pytest --cov=discordia_pubsub --cov-report=term-missing \
		--cov-fail-under=$(COVER_MIN)

tidy:
	go mod tidy

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

sast:
	go run github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION) \
		-exclude-dir=.github -severity medium ./...
