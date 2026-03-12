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

## Docs

- Configuration: `docs/config.md`
- Metrics: `docs/metrics.md`

## Lint and Security

This repository uses Go tool dependencies for static analysis in CI:

- `revive`
- `gosec`
