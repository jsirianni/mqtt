package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jsirianni/mqtt/storage/contracts"
	"go.uber.org/zap"
)

type testMetricsCollector struct {
	mu sync.Mutex

	connectAttempts   int
	connectRejections int
	publishIn         int
	publishForwarded  int
	publishMatches    int
	publishQueued     int
	subscribeCalls    int
	unsubscribeCalls  int
	subscribeRejects  int
}

func (t *testMetricsCollector) SetListenerUp(bool)                      {}
func (t *testMetricsCollector) IncListenerError()                       {}
func (t *testMetricsCollector) IncConnectionsAccepted()                 {}
func (t *testMetricsCollector) AddConnectionsActive(int64)              {}
func (t *testMetricsCollector) IncConnectionsClosed(string)             {}
func (t *testMetricsCollector) ObserveConnectionDuration(time.Duration) {}
func (t *testMetricsCollector) IncPacketsIn(string)                     {}
func (t *testMetricsCollector) IncPacketsOut(string)                    {}
func (t *testMetricsCollector) ObservePacketSize(string, int64)         {}
func (t *testMetricsCollector) IncDecodeError(string, string)           {}
func (t *testMetricsCollector) IncEncodeError(string, string)           {}
func (t *testMetricsCollector) IncOutboundQueueDrop(string)             {}
func (t *testMetricsCollector) AddInflight(int64)                       {}
func (t *testMetricsCollector) AddPending(int64)                        {}
func (t *testMetricsCollector) ObservePubackLatency(time.Duration)      {}
func (t *testMetricsCollector) SetSessionCounts(int64, int64, int64)    {}
func (t *testMetricsCollector) SetBuildInfo(string, string)             {}
func (t *testMetricsCollector) Shutdown(context.Context) error          { return nil }

func (t *testMetricsCollector) IncConnectAttempt() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.connectAttempts++
}

func (t *testMetricsCollector) IncConnectRejection(byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.connectRejections++
}

func (t *testMetricsCollector) IncPublishIn(byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.publishIn++
}

func (t *testMetricsCollector) IncPublishForwarded(byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.publishForwarded++
}

func (t *testMetricsCollector) AddPublishMatches(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.publishMatches += n
}

func (t *testMetricsCollector) AddPublishOfflineQueued(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.publishQueued += n
}

func (t *testMetricsCollector) IncSubscribe() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.subscribeCalls++
}

func (t *testMetricsCollector) IncUnsubscribe() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.unsubscribeCalls++
}

func (t *testMetricsCollector) IncSubscribeRejection(byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.subscribeRejects++
}

func TestBrokerRecordsMetricsForConnectPublishSubscribe(t *testing.T) {
	metrics := &testMetricsCollector{}
	b := NewBroker(defaultConfig(), zap.NewNop(), contracts.BrokerStores{}, metrics)

	subscriber := newTestConn()
	connectClient(t, b, subscriber, "sub", false, false)
	subscribeClient(t, b, subscriber, "sensors/+/temp", 1)

	publisher := newTestConn()
	connectClient(t, b, publisher, "pub", true, false)

	_, _, err := b.HandlePublish(publisher, &PublishPacket{
		Topic:   "sensors/room1/temp",
		Payload: []byte("12"),
		QoS:     1,
	})
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}

	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	if metrics.connectAttempts != 2 {
		t.Fatalf("expected 2 connect attempts, got %d", metrics.connectAttempts)
	}
	if metrics.publishIn != 1 {
		t.Fatalf("expected publish_in metric increment once, got %d", metrics.publishIn)
	}
	if metrics.publishForwarded == 0 {
		t.Fatalf("expected publish_forwarded metric increments, got %d", metrics.publishForwarded)
	}
	if metrics.publishMatches == 0 {
		t.Fatalf("expected publish_matches to be > 0, got %d", metrics.publishMatches)
	}
	if metrics.subscribeCalls == 0 {
		t.Fatalf("expected subscribe metric increment, got %d", metrics.subscribeCalls)
	}
}

func TestBrokerRecordsSubscribeRejections(t *testing.T) {
	metrics := &testMetricsCollector{}
	b := NewBroker(defaultConfig(), zap.NewNop(), contracts.BrokerStores{}, metrics)

	c := newTestConn()
	connectClient(t, b, c, "client", true, false)
	_, _, err := b.HandleSubscribe(c, &SubscribePacket{
		PacketID: 3,
		Filters: []SubscribeFilter{
			{Filter: "$share/g/topic/#", QoS: 0},
		},
	})
	if err != nil {
		t.Fatalf("HandleSubscribe returned unexpected error: %v", err)
	}

	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if metrics.subscribeRejects == 0 {
		t.Fatalf("expected subscribe rejection metric increment, got %d", metrics.subscribeRejects)
	}
}
