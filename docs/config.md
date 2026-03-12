# Configuration

The `mqtt` command supports configuration through:

1. CLI flags
2. Environment variables (`MQTT_` prefix)
3. A config file passed with `--config`

Viper key mapping is used, with dots replaced by underscores for env vars.
For example, `server.listen_addr` maps to `MQTT_SERVER_LISTEN_ADDR`.

## Precedence

When the same option is set in multiple places, higher-priority values win:

1. CLI flags
2. Environment variables
3. Config file values
4. Built-in defaults

## Loading a Config File

Pass a file path with:

```bash
mqtt --config ./config.yaml
```

The file format is inferred from extension by Viper (for example: YAML, JSON, TOML).

## Options

| Viper Key | CLI Flag | Env Var | Type | Default | Notes |
| --- | --- | --- | --- | --- | --- |
| `server.listen_addr` | `--listen-addr` | `MQTT_SERVER_LISTEN_ADDR` | string | `:1883` | Address the broker listens on. |
| `server.max_packet_size` | `--max-packet-size` | `MQTT_SERVER_MAX_PACKET_SIZE` | uint32 | `1048576` | Maximum packet size in bytes (1 MiB default). |
| `server.receive_maximum` | `--receive-maximum` | `MQTT_SERVER_RECEIVE_MAXIMUM` | uint16 | `32` | Values above `65535` are clamped to `65535`. |
| `server.max_outbound_queue` | `--max-outbound-queue` | `MQTT_SERVER_MAX_OUTBOUND_QUEUE` | int | `1024` | Max queued outbound messages per client. |
| `server.max_session_queue` | `--max-session-queue` | `MQTT_SERVER_MAX_SESSION_QUEUE` | int | `1024` | Max queued session messages per client. |
| `server.write_timeout` | `--write-timeout` | `MQTT_SERVER_WRITE_TIMEOUT` | duration | `10s` | Go duration format (`500ms`, `2s`, `1m`). |
| `server.read_timeout` | `--read-timeout` | `MQTT_SERVER_READ_TIMEOUT` | duration | `0` | `0` means no read timeout. |
| `server.session_sweep_interval` | `--session-sweep-interval` | `MQTT_SERVER_SESSION_SWEEP_INTERVAL` | duration | `30s` | Interval for session cleanup work. |
| `log.level` | `--log-level` | `MQTT_LOG_LEVEL` | string | `info` | Supported: `debug`, `info`, `warn`, `error`. Unrecognized values fall back to `info`. |
| `log.encoding` | `--log-encoding` | `MQTT_LOG_ENCODING` | string | `json` | Currently forced to JSON in code. |

## Example `config.yaml`

```yaml
server:
  listen_addr: ":1883"
  max_packet_size: 1048576
  receive_maximum: 32
  max_outbound_queue: 1024
  max_session_queue: 1024
  write_timeout: 10s
  read_timeout: 0s
  session_sweep_interval: 30s

log:
  level: info
  encoding: json
```
