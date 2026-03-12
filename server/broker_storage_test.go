package server

import (
	"testing"

	"github.com/jsirianni/mqtt/storage/contracts"
	"go.uber.org/zap"
)

func newTestBroker() *Broker {
	return NewBroker(defaultConfig(), zap.NewNop(), contracts.BrokerStores{}, NewNoopMetricsCollector())
}

func newTestConn() *Conn {
	return &Conn{
		outbound: make(chan Packet, 32),
		closed:   make(chan struct{}),
	}
}

func connectClient(t *testing.T, b *Broker, conn *Conn, clientID string, clean bool, withWill bool) *ConnackPacket {
	t.Helper()

	pkt := &ConnectPacket{
		ClientID:     clientID,
		CleanSession: clean,
		WillFlag:     withWill,
		WillTopic:    "will/topic",
		WillPayload:  []byte("bye"),
		WillQoS:      0,
		WillRetain:   false,
	}

	connack, err := b.HandleConnect(conn, pkt)
	if err != nil {
		t.Fatalf("HandleConnect failed: %v", err)
	}
	return connack
}

func subscribeClient(t *testing.T, b *Broker, conn *Conn, filter string, qos byte) {
	t.Helper()
	_, _, err := b.HandleSubscribe(conn, &SubscribePacket{
		PacketID: 1,
		Filters: []SubscribeFilter{
			{
				Filter: filter,
				QoS:    qos,
			},
		},
	})
	if err != nil {
		t.Fatalf("HandleSubscribe failed: %v", err)
	}
}

func TestBrokerSessionSemantics(t *testing.T) {
	b := newTestBroker()

	c1 := newTestConn()
	connack := connectClient(t, b, c1, "clientA", false, false)
	if connack.SessionPresent {
		t.Fatalf("expected first connect to have SessionPresent=false")
	}

	if err := b.HandleDisconnect(c1, &DisconnectPacket{}); err != nil {
		t.Fatalf("HandleDisconnect failed: %v", err)
	}

	c2 := newTestConn()
	connack = connectClient(t, b, c2, "clientA", false, false)
	if !connack.SessionPresent {
		t.Fatalf("expected persistent reconnect to have SessionPresent=true")
	}

	if err := b.HandleDisconnect(c2, &DisconnectPacket{}); err != nil {
		t.Fatalf("HandleDisconnect failed: %v", err)
	}

	c3 := newTestConn()
	connack = connectClient(t, b, c3, "clientA", true, false)
	if connack.SessionPresent {
		t.Fatalf("expected clean-session connect to have SessionPresent=false")
	}
}

func TestBrokerRetainedSemantics(t *testing.T) {
	b := newTestBroker()

	pub := newTestConn()
	connectClient(t, b, pub, "publisher", true, false)

	_, _, err := b.HandlePublish(pub, &PublishPacket{
		Topic:   "a/b",
		Payload: []byte("hello"),
		QoS:     0,
		Retain:  true,
	})
	if err != nil {
		t.Fatalf("initial retained publish failed: %v", err)
	}

	sub1 := newTestConn()
	connectClient(t, b, sub1, "sub1", true, false)
	_, deliveries, err := b.HandleSubscribe(sub1, &SubscribePacket{
		PacketID: 10,
		Filters: []SubscribeFilter{
			{Filter: "a/#", QoS: 0},
		},
	})
	if err != nil {
		t.Fatalf("subscribe with retained failed: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 retained delivery, got %d", len(deliveries))
	}

	_, _, err = b.HandlePublish(pub, &PublishPacket{
		Topic:   "a/b",
		Payload: nil,
		QoS:     0,
		Retain:  true,
	})
	if err != nil {
		t.Fatalf("retained delete publish failed: %v", err)
	}

	sub2 := newTestConn()
	connectClient(t, b, sub2, "sub2", true, false)
	_, deliveries, err = b.HandleSubscribe(sub2, &SubscribePacket{
		PacketID: 11,
		Filters: []SubscribeFilter{
			{Filter: "a/#", QoS: 0},
		},
	})
	if err != nil {
		t.Fatalf("subscribe after retained delete failed: %v", err)
	}
	if len(deliveries) != 0 {
		t.Fatalf("expected 0 retained deliveries after delete, got %d", len(deliveries))
	}
}

func TestBrokerQoS1InflightPendingSemantics(t *testing.T) {
	b := newTestBroker()

	sub := newTestConn()
	connectClient(t, b, sub, "subscriber", false, false)
	subscribeClient(t, b, sub, "qos/1", 1)

	// Simulate subscriber going offline while session persists.
	sub.session.Conn = nil

	pub := newTestConn()
	connectClient(t, b, pub, "publisher", true, false)
	_, _, err := b.HandlePublish(pub, &PublishPacket{
		Topic:   "qos/1",
		Payload: []byte("data"),
		QoS:     1,
	})
	if err != nil {
		t.Fatalf("publish qos1 failed: %v", err)
	}

	sub.session.Conn = sub
	b.mu.Lock()
	b.drainPendingLocked(sub.session)
	b.mu.Unlock()

	select {
	case pkt := <-sub.outbound:
		p, ok := pkt.(*PublishPacket)
		if !ok {
			t.Fatalf("expected PublishPacket in outbound")
		}
		if p.PacketID == 0 {
			t.Fatalf("expected non-zero packet id for qos1 delivery")
		}
		inflightCount, err := b.stores.Delivery.InflightCount(sub.session.ClientID)
		if err != nil {
			t.Fatalf("InflightCount failed: %v", err)
		}
		if inflightCount != 1 {
			t.Fatalf("expected inflight outbound to contain 1 message")
		}
		if err := b.HandlePuback(sub, &PubackPacket{PacketID: p.PacketID}); err != nil {
			t.Fatalf("HandlePuback failed: %v", err)
		}
		inflightCount, err = b.stores.Delivery.InflightCount(sub.session.ClientID)
		if err != nil {
			t.Fatalf("InflightCount failed: %v", err)
		}
		if inflightCount != 0 {
			t.Fatalf("expected inflight outbound to be empty after PUBACK")
		}
	default:
		t.Fatalf("expected drained pending message to be sent outbound")
	}
}

func TestBrokerWillSemantics(t *testing.T) {
	b := newTestBroker()

	observer := newTestConn()
	connectClient(t, b, observer, "observer", true, false)
	subscribeClient(t, b, observer, "will/#", 0)

	willGraceful := newTestConn()
	connectClient(t, b, willGraceful, "willGraceful", false, true)
	if err := b.HandleDisconnect(willGraceful, &DisconnectPacket{}); err != nil {
		t.Fatalf("HandleDisconnect failed: %v", err)
	}
	select {
	case <-observer.outbound:
		t.Fatalf("did not expect will publish on graceful disconnect")
	default:
	}

	willUngraceful := newTestConn()
	connectClient(t, b, willUngraceful, "willUngraceful", false, true)
	if err := b.HandleConnectionLost(willUngraceful); err != nil {
		t.Fatalf("HandleConnectionLost failed: %v", err)
	}

	select {
	case pkt := <-observer.outbound:
		p, ok := pkt.(*PublishPacket)
		if !ok {
			t.Fatalf("expected will publish packet")
		}
		if p.Topic != "will/topic" {
			t.Fatalf("expected will topic, got %q", p.Topic)
		}
	default:
		t.Fatalf("expected will publish on ungraceful disconnect")
	}
}
