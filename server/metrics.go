package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// MetricsCollector records broker operational metrics.
type MetricsCollector interface {
	SetListenerUp(up bool)
	IncListenerError()
	IncConnectionsAccepted()
	AddConnectionsActive(delta int64)
	IncConnectionsClosed(mode string)
	ObserveConnectionDuration(d time.Duration)

	IncPacketsIn(packetType string)
	IncPacketsOut(packetType string)
	ObservePacketSize(direction string, size int64)
	IncDecodeError(stage, reason string)
	IncEncodeError(packetType, reason string)

	IncConnectAttempt()
	IncConnectRejection(reasonCode byte)
	IncPublishIn(qos byte, retain bool)
	IncPublishForwarded(qos byte)
	AddPublishMatches(n int)
	AddPublishOfflineQueued(n int)
	IncSubscribe()
	IncUnsubscribe()
	IncSubscribeRejection(reasonCode byte)

	IncOutboundQueueDrop(path string)
	AddInflight(delta int64)
	AddPending(delta int64)
	ObservePubackLatency(d time.Duration)

	SetSessionCounts(active, persistent, clean int64)
	SetBuildInfo(version, revision string)
	Shutdown(context.Context) error
}

type noopMetricsCollector struct{}

func (noopMetricsCollector) SetListenerUp(bool)                      {}
func (noopMetricsCollector) IncListenerError()                       {}
func (noopMetricsCollector) IncConnectionsAccepted()                 {}
func (noopMetricsCollector) AddConnectionsActive(int64)              {}
func (noopMetricsCollector) IncConnectionsClosed(string)             {}
func (noopMetricsCollector) ObserveConnectionDuration(time.Duration) {}
func (noopMetricsCollector) IncPacketsIn(string)                     {}
func (noopMetricsCollector) IncPacketsOut(string)                    {}
func (noopMetricsCollector) ObservePacketSize(string, int64)         {}
func (noopMetricsCollector) IncDecodeError(string, string)           {}
func (noopMetricsCollector) IncEncodeError(string, string)           {}
func (noopMetricsCollector) IncConnectAttempt()                      {}
func (noopMetricsCollector) IncConnectRejection(byte)                {}
func (noopMetricsCollector) IncPublishIn(byte, bool)                 {}
func (noopMetricsCollector) IncPublishForwarded(byte)                {}
func (noopMetricsCollector) AddPublishMatches(int)                   {}
func (noopMetricsCollector) AddPublishOfflineQueued(int)             {}
func (noopMetricsCollector) IncSubscribe()                           {}
func (noopMetricsCollector) IncUnsubscribe()                         {}
func (noopMetricsCollector) IncSubscribeRejection(byte)              {}
func (noopMetricsCollector) IncOutboundQueueDrop(string)             {}
func (noopMetricsCollector) AddInflight(int64)                       {}
func (noopMetricsCollector) AddPending(int64)                        {}
func (noopMetricsCollector) ObservePubackLatency(time.Duration)      {}
func (noopMetricsCollector) SetSessionCounts(int64, int64, int64)    {}
func (noopMetricsCollector) SetBuildInfo(string, string)             {}
func (noopMetricsCollector) Shutdown(context.Context) error          { return nil }

// NewNoopMetricsCollector returns a metrics collector that records nothing.
func NewNoopMetricsCollector() MetricsCollector {
	return noopMetricsCollector{}
}

// OTelMetricsCollector records metrics via OpenTelemetry instruments.
type OTelMetricsCollector struct {
	listenerUp      atomic.Int64
	sessionsActive  atomic.Int64
	sessionsPersist atomic.Int64
	sessionsClean   atomic.Int64
	inflight        atomic.Int64
	pending         atomic.Int64

	processStartUnix float64
	processStartTime time.Time

	buildMu       sync.RWMutex
	buildVersion  string
	buildRevision string

	registration metric.Registration

	listenerErrors      metric.Int64Counter
	connectionsAccepted metric.Int64Counter
	connectionsActive   metric.Int64UpDownCounter
	connectionsClosed   metric.Int64Counter
	connectionDuration  metric.Float64Histogram

	packetsIn     metric.Int64Counter
	packetsOut    metric.Int64Counter
	packetSize    metric.Int64Histogram
	decodeErrors  metric.Int64Counter
	encodeErrors  metric.Int64Counter
	connects      metric.Int64Counter
	connectReject metric.Int64Counter
	publishIn     metric.Int64Counter
	publishFwd    metric.Int64Counter
	publishMatch  metric.Int64Counter
	publishQueued metric.Int64Counter
	subscribe     metric.Int64Counter
	unsubscribe   metric.Int64Counter
	subReject     metric.Int64Counter

	queueDrops    metric.Int64Counter
	inflightGauge metric.Int64UpDownCounter
	pendingGauge  metric.Int64UpDownCounter
	pubackLatency metric.Float64Histogram

	processStartGauge metric.Float64ObservableGauge
	uptimeGauge       metric.Float64ObservableGauge
	listenerUpGauge   metric.Int64ObservableGauge
	buildInfoGauge    metric.Int64ObservableGauge
	sessionActiveObs  metric.Int64ObservableGauge
	sessionPersistObs metric.Int64ObservableGauge
	sessionCleanObs   metric.Int64ObservableGauge
}

// NewOTelMetricsCollector constructs OTel instruments used by the server.
func NewOTelMetricsCollector(meter metric.Meter) (*OTelMetricsCollector, error) {
	now := time.Now()
	c := &OTelMetricsCollector{
		processStartUnix: float64(now.Unix()),
		processStartTime: now,
	}

	var err error
	if c.listenerErrors, err = meter.Int64Counter(
		"mqtt_listener_errors_total",
		metric.WithDescription("Total listener bind/accept errors"),
	); err != nil {
		return nil, err
	}
	if c.connectionsAccepted, err = meter.Int64Counter(
		"mqtt_connections_accepted_total",
		metric.WithDescription("Total accepted TCP connections"),
	); err != nil {
		return nil, err
	}
	if c.connectionsActive, err = meter.Int64UpDownCounter(
		"mqtt_connections_active",
		metric.WithDescription("Current number of active client connections"),
	); err != nil {
		return nil, err
	}
	if c.connectionsClosed, err = meter.Int64Counter(
		"mqtt_connections_closed_total",
		metric.WithDescription("Total connection closes by close mode"),
	); err != nil {
		return nil, err
	}
	if c.connectionDuration, err = meter.Float64Histogram(
		"mqtt_connection_duration_seconds",
		metric.WithDescription("Connection lifetime in seconds"),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if c.packetsIn, err = meter.Int64Counter(
		"mqtt_packets_in_total",
		metric.WithDescription("Inbound packets by packet type"),
	); err != nil {
		return nil, err
	}
	if c.packetsOut, err = meter.Int64Counter(
		"mqtt_packets_out_total",
		metric.WithDescription("Outbound packets by packet type"),
	); err != nil {
		return nil, err
	}
	if c.packetSize, err = meter.Int64Histogram(
		"mqtt_packet_size_bytes",
		metric.WithDescription("Packet size by direction"),
		metric.WithUnit("By"),
	); err != nil {
		return nil, err
	}
	if c.decodeErrors, err = meter.Int64Counter(
		"mqtt_decode_errors_total",
		metric.WithDescription("Packet decode errors"),
	); err != nil {
		return nil, err
	}
	if c.encodeErrors, err = meter.Int64Counter(
		"mqtt_encode_errors_total",
		metric.WithDescription("Packet encode/write errors"),
	); err != nil {
		return nil, err
	}
	if c.connects, err = meter.Int64Counter(
		"mqtt_connect_attempts_total",
		metric.WithDescription("CONNECT handling attempts"),
	); err != nil {
		return nil, err
	}
	if c.connectReject, err = meter.Int64Counter(
		"mqtt_connect_rejections_total",
		metric.WithDescription("Rejected CONNECT attempts by reason code"),
	); err != nil {
		return nil, err
	}
	if c.publishIn, err = meter.Int64Counter(
		"mqtt_publish_in_total",
		metric.WithDescription("Inbound publish packets by qos/retain"),
	); err != nil {
		return nil, err
	}
	if c.publishFwd, err = meter.Int64Counter(
		"mqtt_publish_forwarded_total",
		metric.WithDescription("Forwarded deliveries by effective qos"),
	); err != nil {
		return nil, err
	}
	if c.publishMatch, err = meter.Int64Counter(
		"mqtt_publish_matches_total",
		metric.WithDescription("Matched subscriptions for publish routing"),
	); err != nil {
		return nil, err
	}
	if c.publishQueued, err = meter.Int64Counter(
		"mqtt_publish_offline_queued_total",
		metric.WithDescription("Messages queued due to offline/flow-limited sessions"),
	); err != nil {
		return nil, err
	}
	if c.subscribe, err = meter.Int64Counter(
		"mqtt_subscribe_total",
		metric.WithDescription("Subscribe requests processed"),
	); err != nil {
		return nil, err
	}
	if c.unsubscribe, err = meter.Int64Counter(
		"mqtt_unsubscribe_total",
		metric.WithDescription("Unsubscribe requests processed"),
	); err != nil {
		return nil, err
	}
	if c.subReject, err = meter.Int64Counter(
		"mqtt_subscribe_rejections_total",
		metric.WithDescription("Rejected subscribe filters by reason code"),
	); err != nil {
		return nil, err
	}
	if c.queueDrops, err = meter.Int64Counter(
		"mqtt_outbound_queue_drops_total",
		metric.WithDescription("Dropped/deferred outbound sends in non-blocking paths"),
	); err != nil {
		return nil, err
	}
	if c.inflightGauge, err = meter.Int64UpDownCounter(
		"mqtt_inflight_messages",
		metric.WithDescription("Current aggregate QoS1 inflight messages"),
	); err != nil {
		return nil, err
	}
	if c.pendingGauge, err = meter.Int64UpDownCounter(
		"mqtt_pending_messages",
		metric.WithDescription("Current aggregate QoS1 pending messages"),
	); err != nil {
		return nil, err
	}
	if c.pubackLatency, err = meter.Float64Histogram(
		"mqtt_puback_latency_seconds",
		metric.WithDescription("QoS1 PUBACK latency"),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}

	if c.processStartGauge, err = meter.Float64ObservableGauge(
		"mqtt_process_start_time_seconds",
		metric.WithDescription("Unix start timestamp of the process"),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if c.uptimeGauge, err = meter.Float64ObservableGauge(
		"mqtt_server_uptime_seconds",
		metric.WithDescription("Process uptime in seconds"),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if c.listenerUpGauge, err = meter.Int64ObservableGauge(
		"mqtt_listener_up",
		metric.WithDescription("Listener state: 1 up, 0 down"),
	); err != nil {
		return nil, err
	}
	if c.buildInfoGauge, err = meter.Int64ObservableGauge(
		"mqtt_build_info",
		metric.WithDescription("Build metadata as constant-1 gauge"),
	); err != nil {
		return nil, err
	}
	if c.sessionActiveObs, err = meter.Int64ObservableGauge(
		"mqtt_sessions_active",
		metric.WithDescription("Current number of tracked sessions"),
	); err != nil {
		return nil, err
	}
	if c.sessionPersistObs, err = meter.Int64ObservableGauge(
		"mqtt_sessions_persistent",
		metric.WithDescription("Current number of persistent sessions"),
	); err != nil {
		return nil, err
	}
	if c.sessionCleanObs, err = meter.Int64ObservableGauge(
		"mqtt_sessions_clean",
		metric.WithDescription("Current number of clean sessions"),
	); err != nil {
		return nil, err
	}

	c.registration, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveFloat64(c.processStartGauge, c.processStartUnix)
		o.ObserveFloat64(c.uptimeGauge, time.Since(c.processStartTime).Seconds())
		o.ObserveInt64(c.listenerUpGauge, c.listenerUp.Load())
		o.ObserveInt64(c.sessionActiveObs, c.sessionsActive.Load())
		o.ObserveInt64(c.sessionPersistObs, c.sessionsPersist.Load())
		o.ObserveInt64(c.sessionCleanObs, c.sessionsClean.Load())

		c.buildMu.RLock()
		version := c.buildVersion
		revision := c.buildRevision
		c.buildMu.RUnlock()
		o.ObserveInt64(
			c.buildInfoGauge,
			1,
			metric.WithAttributes(
				attribute.String("version", version),
				attribute.String("revision", revision),
			),
		)
		return nil
	}, c.processStartGauge, c.uptimeGauge, c.listenerUpGauge, c.buildInfoGauge, c.sessionActiveObs, c.sessionPersistObs, c.sessionCleanObs)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// SetListenerUp records listener availability as a binary gauge.
func (c *OTelMetricsCollector) SetListenerUp(up bool) {
	if up {
		c.listenerUp.Store(1)
		return
	}
	c.listenerUp.Store(0)
}

// IncListenerError increments the listener error counter.
func (c *OTelMetricsCollector) IncListenerError() {
	c.listenerErrors.Add(context.Background(), 1)
}

// IncConnectionsAccepted increments accepted connection count.
func (c *OTelMetricsCollector) IncConnectionsAccepted() {
	c.connectionsAccepted.Add(context.Background(), 1)
}

// AddConnectionsActive adjusts the active connections gauge by delta.
func (c *OTelMetricsCollector) AddConnectionsActive(delta int64) {
	c.connectionsActive.Add(context.Background(), delta)
}

// IncConnectionsClosed increments closed connections with close mode.
func (c *OTelMetricsCollector) IncConnectionsClosed(mode string) {
	c.connectionsClosed.Add(context.Background(), 1, metric.WithAttributes(attribute.String("mode", mode)))
}

// ObserveConnectionDuration records a connection lifetime sample.
func (c *OTelMetricsCollector) ObserveConnectionDuration(d time.Duration) {
	c.connectionDuration.Record(context.Background(), d.Seconds())
}

// IncPacketsIn increments inbound packet count for packet type.
func (c *OTelMetricsCollector) IncPacketsIn(packetType string) {
	c.packetsIn.Add(context.Background(), 1, metric.WithAttributes(attribute.String("packet_type", packetType)))
}

// IncPacketsOut increments outbound packet count for packet type.
func (c *OTelMetricsCollector) IncPacketsOut(packetType string) {
	c.packetsOut.Add(context.Background(), 1, metric.WithAttributes(attribute.String("packet_type", packetType)))
}

// ObservePacketSize records packet size by traffic direction.
func (c *OTelMetricsCollector) ObservePacketSize(direction string, size int64) {
	c.packetSize.Record(context.Background(), size, metric.WithAttributes(attribute.String("direction", direction)))
}

// IncDecodeError increments decode errors with stage and reason labels.
func (c *OTelMetricsCollector) IncDecodeError(stage, reason string) {
	c.decodeErrors.Add(
		context.Background(),
		1,
		metric.WithAttributes(
			attribute.String("stage", stage),
			attribute.String("reason", reason),
		),
	)
}

// IncEncodeError increments encode errors for packet type and reason.
func (c *OTelMetricsCollector) IncEncodeError(packetType, reason string) {
	c.encodeErrors.Add(
		context.Background(),
		1,
		metric.WithAttributes(
			attribute.String("packet_type", packetType),
			attribute.String("reason", reason),
		),
	)
}

// IncConnectAttempt increments CONNECT attempt count.
func (c *OTelMetricsCollector) IncConnectAttempt() {
	c.connects.Add(context.Background(), 1)
}

// IncConnectRejection increments rejected CONNECT attempts by reason.
func (c *OTelMetricsCollector) IncConnectRejection(reasonCode byte) {
	c.connectReject.Add(
		context.Background(),
		1,
		metric.WithAttributes(attribute.String("reason_code", reasonCodeLabel(reasonCode))),
	)
}

// IncPublishIn increments inbound publish count by qos/retain labels.
func (c *OTelMetricsCollector) IncPublishIn(qos byte, retain bool) {
	c.publishIn.Add(
		context.Background(),
		1,
		metric.WithAttributes(
			attribute.Int("qos", int(qos)),
			attribute.Bool("retain", retain),
		),
	)
}

// IncPublishForwarded increments forwarded publish count by qos.
func (c *OTelMetricsCollector) IncPublishForwarded(qos byte) {
	c.publishFwd.Add(context.Background(), 1, metric.WithAttributes(attribute.Int("qos", int(qos))))
}

// AddPublishMatches adds matched subscription count for routing.
func (c *OTelMetricsCollector) AddPublishMatches(n int) {
	if n <= 0 {
		return
	}
	c.publishMatch.Add(context.Background(), int64(n))
}

// AddPublishOfflineQueued adds count of publishes queued offline.
func (c *OTelMetricsCollector) AddPublishOfflineQueued(n int) {
	if n <= 0 {
		return
	}
	c.publishQueued.Add(context.Background(), int64(n))
}

// IncSubscribe increments subscribe request count.
func (c *OTelMetricsCollector) IncSubscribe() {
	c.subscribe.Add(context.Background(), 1)
}

// IncUnsubscribe increments unsubscribe request count.
func (c *OTelMetricsCollector) IncUnsubscribe() {
	c.unsubscribe.Add(context.Background(), 1)
}

// IncSubscribeRejection increments rejected subscribe count by reason.
func (c *OTelMetricsCollector) IncSubscribeRejection(reasonCode byte) {
	c.subReject.Add(
		context.Background(),
		1,
		metric.WithAttributes(attribute.String("reason_code", reasonCodeLabel(reasonCode))),
	)
}

// IncOutboundQueueDrop increments outbound queue drop counters by path.
func (c *OTelMetricsCollector) IncOutboundQueueDrop(path string) {
	c.queueDrops.Add(context.Background(), 1, metric.WithAttributes(attribute.String("path", path)))
}

// AddInflight adjusts inflight message gauge while clamping at zero.
func (c *OTelMetricsCollector) AddInflight(delta int64) {
	if delta == 0 {
		return
	}
	next := c.inflight.Add(delta)
	applied := delta
	if next < 0 {
		applied = delta - next
		c.inflight.Store(0)
	}
	if applied != 0 {
		c.inflightGauge.Add(context.Background(), applied)
	}
}

// AddPending adjusts pending message gauge while clamping at zero.
func (c *OTelMetricsCollector) AddPending(delta int64) {
	if delta == 0 {
		return
	}
	next := c.pending.Add(delta)
	applied := delta
	if next < 0 {
		applied = delta - next
		c.pending.Store(0)
	}
	if applied != 0 {
		c.pendingGauge.Add(context.Background(), applied)
	}
}

// ObservePubackLatency records PUBACK latency sample in seconds.
func (c *OTelMetricsCollector) ObservePubackLatency(d time.Duration) {
	c.pubackLatency.Record(context.Background(), d.Seconds())
}

// SetSessionCounts updates current active, persistent, and clean sessions.
func (c *OTelMetricsCollector) SetSessionCounts(active, persistent, clean int64) {
	c.sessionsActive.Store(active)
	c.sessionsPersist.Store(persistent)
	c.sessionsClean.Store(clean)
}

// SetBuildInfo stores version and revision labels for build info gauge.
func (c *OTelMetricsCollector) SetBuildInfo(version, revision string) {
	c.buildMu.Lock()
	defer c.buildMu.Unlock()
	c.buildVersion = version
	c.buildRevision = revision
}

// Shutdown unregisters OTel callbacks used by this collector.
func (c *OTelMetricsCollector) Shutdown(_ context.Context) error {
	if c.registration == nil {
		return nil
	}
	return c.registration.Unregister()
}

func reasonCodeLabel(code byte) string {
	return fmt.Sprintf("0x%02x", code)
}

func packetTypeLabel(packetType byte) string {
	switch packetType {
	case PacketCONNECT:
		return "connect"
	case PacketCONNACK:
		return "connack"
	case PacketPUBLISH:
		return "publish"
	case PacketPUBACK:
		return "puback"
	case PacketSUBSCRIBE:
		return "subscribe"
	case PacketSUBACK:
		return "suback"
	case PacketUNSUBSCRIBE:
		return "unsubscribe"
	case PacketUNSUBACK:
		return "unsuback"
	case PacketPINGREQ:
		return "pingreq"
	case PacketPINGRESP:
		return "pingresp"
	case PacketDISCONNECT:
		return "disconnect"
	default:
		return "unknown"
	}
}

func errorReasonLabel(err error) string {
	if err == nil {
		return "none"
	}
	switch {
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, net.ErrClosed):
		return "net_closed"
	case errors.Is(err, ErrMalformedPacket):
		return "malformed_packet"
	case errors.Is(err, ErrProtocolError):
		return "protocol_error"
	case errors.Is(err, ErrUnsupportedProtocolVersion):
		return "unsupported_protocol_version"
	case errors.Is(err, ErrClientIDInvalid):
		return "client_id_invalid"
	case errors.Is(err, ErrBadAuthenticationMethod):
		return "bad_authentication_method"
	case errors.Is(err, ErrQoSNotSupported):
		return "qos_not_supported"
	case errors.Is(err, ErrPacketTooLarge):
		return "packet_too_large"
	case errors.Is(err, ErrTopicAliasNotSupported):
		return "topic_alias_not_supported"
	case errors.Is(err, ErrSubscriptionIDNotSupported):
		return "subscription_id_not_supported"
	case errors.Is(err, ErrSharedSubscriptionNotSupported):
		return "shared_subscription_not_supported"
	case errors.Is(err, ErrFirstPacketMustBeConnect):
		return "first_packet_not_connect"
	case errors.Is(err, ErrInvalidUTF8):
		return "invalid_utf8"
	default:
		return "other"
	}
}
