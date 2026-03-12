# MQTT Broker

A lightweight MQTT broker implementation in Go.

## Requirements

- Go 1.25+

## Run

```bash
go run ./cmd/mqtt
```

## Test

```bash
go test ./...
```

## Build With GoReleaser

Build Linux binaries (`amd64`, `arm64`) locally:

```bash
goreleaser build --snapshot --clean
```

Build and publish container images to `ghcr.io/jsirianni/mqtt`:

```bash
goreleaser release --clean
```

The GoReleaser container image uses `scratch` and runs as a non-root user.

## Docs

- Configuration: `docs/config.md`
- Metrics: `docs/metrics.md`

## Lint and Security

This repository uses Go tool dependencies for static analysis in CI:

- `revive`
- `gosec`
