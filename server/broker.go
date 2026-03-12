// Package server implements the in-memory MQTT broker.
package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/jsirianni/mqtt/storage/contracts"
	memstore "github.com/jsirianni/mqtt/storage/memory"
	"go.uber.org/zap"
)

// Delivery is an internal routing instruction: send Packet to Target session.
type Delivery struct {
	Target *Session
	Packet Packet
}

// Broker holds shared in-memory state.
type Broker struct {
	mu      sync.RWMutex
	cfg     Config
	stores  contracts.BrokerStores
	logger  *zap.Logger
	metrics MetricsCollector
	// qos1SentAt tracks send timestamps for PUBACK latency observations.
	qos1SentAt    map[string]map[uint16]time.Time
	pendingCounts map[string]int
}

// NewBroker creates a broker with the given config.
func NewBroker(cfg Config, logger *zap.Logger, stores contracts.BrokerStores, metrics MetricsCollector) *Broker {
	if metrics == nil {
		metrics = NewNoopMetricsCollector()
	}
	mem := memstore.NewStores()
	if stores.Sessions == nil {
		stores.Sessions = mem
	}
	if stores.Retained == nil {
		stores.Retained = mem
	}
	if stores.Delivery == nil {
		stores.Delivery = mem
	}

	return &Broker{
		cfg:           cfg,
		stores:        stores,
		logger:        logger,
		metrics:       metrics,
		qos1SentAt:    make(map[string]map[uint16]time.Time),
		pendingCounts: make(map[string]int),
	}
}

func asSession(v any) *Session {
	sess, _ := v.(*Session)
	return sess
}

func asSubscription(v any) *Subscription {
	sub, _ := v.(*Subscription)
	return sub
}

func asRetainedMessage(v any) *RetainedMessage {
	rm, _ := v.(*RetainedMessage)
	return rm
}

func asPublishMessage(v any) *PublishMessage {
	msg, _ := v.(*PublishMessage)
	return msg
}

func (b *Broker) canSendMore(clientID string, sess *Session) bool {
	if sess == nil {
		return false
	}
	count, err := b.stores.Delivery.InflightCount(clientID)
	if err != nil {
		return false
	}
	return count < int(sess.ClientReceiveMaximum)
}

func (b *Broker) recordInflightAdded(clientID string, packetID uint16) {
	if clientID == "" || packetID == 0 {
		return
	}
	perClient := b.qos1SentAt[clientID]
	if perClient == nil {
		perClient = make(map[uint16]time.Time)
		b.qos1SentAt[clientID] = perClient
	}
	perClient[packetID] = time.Now()
	b.metrics.AddInflight(1)
}

func (b *Broker) recordInflightRemoved(clientID string, packetID uint16, observeLatency bool) {
	if clientID == "" || packetID == 0 {
		return
	}
	perClient := b.qos1SentAt[clientID]
	if perClient == nil {
		return
	}
	sentAt, ok := perClient[packetID]
	if !ok {
		return
	}
	delete(perClient, packetID)
	if len(perClient) == 0 {
		delete(b.qos1SentAt, clientID)
	}
	b.metrics.AddInflight(-1)
	if observeLatency {
		b.metrics.ObservePubackLatency(time.Since(sentAt))
	}
}

func (b *Broker) clearClientInflightMetrics(clientID string) {
	perClient := b.qos1SentAt[clientID]
	if perClient == nil {
		return
	}
	b.metrics.AddInflight(-int64(len(perClient)))
	delete(b.qos1SentAt, clientID)
}

func (b *Broker) recordPendingAdded(clientID string) {
	if clientID == "" {
		return
	}
	b.pendingCounts[clientID]++
	b.metrics.AddPending(1)
}

func (b *Broker) recordPendingRemoved(clientID string) {
	if clientID == "" {
		return
	}
	n := b.pendingCounts[clientID]
	if n <= 0 {
		return
	}
	if n == 1 {
		delete(b.pendingCounts, clientID)
	} else {
		b.pendingCounts[clientID] = n - 1
	}
	b.metrics.AddPending(-1)
}

func (b *Broker) clearClientPendingMetrics(clientID string) {
	n := b.pendingCounts[clientID]
	if n <= 0 {
		return
	}
	delete(b.pendingCounts, clientID)
	b.metrics.AddPending(-int64(n))
}

func (b *Broker) recountSessionMetricsLocked() {
	var active int64
	var persistent int64
	var clean int64
	b.stores.Sessions.ForEachSession(func(_ string, raw any) bool {
		sess := asSession(raw)
		if sess == nil {
			return true
		}
		active++
		if sess.CleanStartAtConnect {
			clean++
		} else {
			persistent++
		}
		return true
	})
	b.metrics.SetSessionCounts(active, persistent, clean)
}

// HandleConnect processes CONNECT and returns CONNACK or error.
func (b *Broker) HandleConnect(c *Conn, pkt *ConnectPacket) (*ConnackPacket, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.metrics.IncConnectAttempt()

	clientID := pkt.ClientID
	assigned := false
	if clientID == "" {
		clientID = fmt.Sprintf("gen-%d", b.stores.Sessions.NextGeneratedClientID())
		assigned = true
	}

	sessionPresent := false
	existing := asSession(func() any {
		v, _ := b.stores.Sessions.GetSession(clientID)
		return v
	}())

	if pkt.CleanSession {
		if existing != nil {
			b.removeSessionLocked(existing, false)
		}
		existing = nil
	}

	var sess *Session
	if existing != nil {
		sessionPresent = true
		sess = existing
		// Takeover: disconnect old connection if any
		if sess.Conn != nil && sess.Conn != c {
			oldConn := sess.Conn
			sess.Conn = nil
			oldConn.closeWithContext(false, ReasonUnspecifiedError, false, true)
		}
	} else {
		sess = NewSession(clientID)
		b.stores.Sessions.UpsertSession(clientID, sess)
	}

	// MQTT 3.1.1 uses Clean Session semantics.
	// Clean session means no session persistence after disconnect.
	sess.SessionExpiryInterval = 0
	if !pkt.CleanSession {
		// Non-zero means "persist while process lives" for existing broker internals.
		sess.SessionExpiryInterval = 0xFFFFFFFF
	}
	sess.ExpiresAt = time.Time{}

	sess.Conn = c
	sess.CleanStartAtConnect = pkt.CleanSession
	sess.Username = pkt.Username
	sess.ClientReceiveMaximum = 65535
	sess.ClientMaxPacketSize = 268435455
	sess.RequestProblemInfo = false

	// Store will
	if pkt.WillFlag {
		willMsg := pktToPublishMessage(pkt.WillTopic, pkt.WillPayload, pkt.WillQoS, pkt.WillRetain, nil, clientID)
		sess.Will = &WillMessage{
			Msg:           willMsg,
			DelayInterval: 0,
			CreatedAt:     time.Now(),
		}
	} else {
		sess.Will = nil
	}

	c.session = sess
	c.clientID = clientID
	c.connected = true

	connack := &ConnackPacket{
		SessionPresent: sessionPresent,
		ReasonCode:     ConnackAccepted,
	}
	_ = assigned // v3.1.1 CONNACK cannot include assigned client identifier.
	b.recountSessionMetricsLocked()
	return connack, nil
}

func pktToPublishMessage(topic string, payload []byte, qos byte, retain bool, props *WillProperties, publisherID string) *PublishMessage {
	msg := &PublishMessage{
		Topic:             topic,
		Payload:           payload,
		QoS:               qos,
		Retain:            retain,
		ReceivedAt:        time.Now(),
		PublisherClientID: publisherID,
	}
	if props != nil {
		msg.PayloadFormatIndicator = props.PayloadFormatIndicator
		msg.MessageExpiryInterval = props.MessageExpiryInterval
		msg.ContentType = props.ContentType
		msg.ResponseTopic = props.ResponseTopic
		msg.CorrelationData = props.CorrelationData
		msg.UserProperties = props.UserProperty
	}
	return msg
}

// removeSessionLocked removes session from broker and publishes will if ungraceful. Call with b.mu held.
func (b *Broker) removeSessionLocked(sess *Session, publishWill bool) {
	b.stores.Sessions.DeleteSession(sess.ClientID)
	b.clearClientInflightMetrics(sess.ClientID)
	b.clearClientPendingMetrics(sess.ClientID)
	if publishWill && sess.Will != nil {
		b.publishWillLocked(sess.Will)
	}
	b.recountSessionMetricsLocked()
}

func (b *Broker) publishWillLocked(will *WillMessage) {
	msg := will.Msg
	_ = will.DelayInterval // v1 simplification: publish immediately.
	b.routeLocked(msg, nil)
}

// routeLocked distributes msg to matching sessions. Call with b.mu held. skipSession optionally skips one (e.g. no-local).
func (b *Broker) routeLocked(msg *PublishMessage, skipSession *Session) {
	b.stores.Sessions.ForEachSession(func(_ string, raw any) bool {
		sess := asSession(raw)
		if sess == nil {
			return true
		}
		if sess == skipSession {
			return true
		}
		for _, sub := range sess.Subs {
			if !MatchTopic(sub.Filter, msg.Topic) {
				continue
			}
			if sub.NoLocal && sess.ClientID == msg.PublisherClientID {
				continue
			}
			grantedQoS := msg.QoS
			if sub.MaxQoS < grantedQoS {
				grantedQoS = sub.MaxQoS
			}
			if grantedQoS > 1 {
				grantedQoS = 1
			}
			delivery := b.makeDelivery(sess, msg, grantedQoS, sub.RetainAsPublished)
			if delivery != nil {
				b.sendDeliveryLocked(delivery)
			}
		}
		return true
	})
}

// makeDelivery creates one delivery for a session or nil. retainFlag: use RetainAsPublished for forwarded retain.
func (b *Broker) makeDelivery(sess *Session, msg *PublishMessage, grantedQoS byte, retainAsPublished bool) *Delivery {
	forwardRetain := msg.Retain && retainAsPublished
	pkt := messageToPublishPacket(msg, grantedQoS, forwardRetain)
	if sess.Conn != nil {
		if grantedQoS == 0 {
			return &Delivery{Target: sess, Packet: pkt}
		}
		// QoS 1: allocate packet ID and track inflight
		pid, err := b.stores.Delivery.ReservePacketID(sess.ClientID)
		if err != nil {
			return nil
		}
		pkt.PacketID = pid
		pkt.Dup = false
		if err := b.stores.Delivery.AddInflight(sess.ClientID, pid, msg); err != nil {
			return nil
		}
		b.recordInflightAdded(sess.ClientID, pid)
		return &Delivery{Target: sess, Packet: pkt}
	}
	// Offline: queue QoS 1 only
	if grantedQoS == 1 {
		if err := b.stores.Delivery.EnqueuePending(sess.ClientID, msg); err == nil {
			b.recordPendingAdded(sess.ClientID)
		}
	}
	return nil
}

func (b *Broker) sendDeliveryLocked(d *Delivery) {
	if d.Target.Conn == nil {
		return
	}
	select {
	case d.Target.Conn.outbound <- d.Packet:
	default:
		b.metrics.IncOutboundQueueDrop("broker_send_delivery")
		// Queue full; could add to session pending (v1: drop or queue)
		if pub, ok := d.Packet.(*PublishPacket); ok && pub.QoS == 1 {
			if err := b.stores.Delivery.RemoveInflight(d.Target.ClientID, pub.PacketID); err == nil {
				b.recordInflightRemoved(d.Target.ClientID, pub.PacketID, false)
			}
			if err := b.stores.Delivery.EnqueuePending(d.Target.ClientID, packetToMessage(pub)); err == nil {
				b.recordPendingAdded(d.Target.ClientID)
			}
		}
	}
}

func messageToPublishPacket(msg *PublishMessage, qos byte, retain bool) *PublishPacket {
	pkt := &PublishPacket{
		Topic:   msg.Topic,
		Payload: msg.Payload,
		QoS:     qos,
		Retain:  retain,
		Props:   copyPublishProps(messageProps(msg)),
	}
	return pkt
}

func messageProps(m *PublishMessage) *PublishProperties {
	if m == nil {
		return nil
	}
	return &PublishProperties{
		PayloadFormatIndicator: m.PayloadFormatIndicator,
		MessageExpiryInterval:  m.MessageExpiryInterval,
		ContentType:            m.ContentType,
		ResponseTopic:          m.ResponseTopic,
		CorrelationData:        m.CorrelationData,
		UserProperty:           m.UserProperties,
	}
}

func packetToMessage(p *PublishPacket) *PublishMessage {
	m := &PublishMessage{
		Topic:   p.Topic,
		Payload: p.Payload,
		QoS:     p.QoS,
		Retain:  p.Retain,
		Dup:     p.Dup,
	}
	if p.Props != nil {
		m.PayloadFormatIndicator = p.Props.PayloadFormatIndicator
		m.MessageExpiryInterval = p.Props.MessageExpiryInterval
		m.ContentType = p.Props.ContentType
		m.ResponseTopic = p.Props.ResponseTopic
		m.CorrelationData = p.Props.CorrelationData
		m.UserProperties = p.Props.UserProperty
	}
	return m
}

// HandlePublish processes PUBLISH from client. Returns deliveries, optional PUBACK, error.
func (b *Broker) HandlePublish(c *Conn, pkt *PublishPacket) ([]Delivery, *PubackPacket, error) {
	if err := ValidateTopicName(pkt.Topic); err != nil {
		return nil, nil, err
	}
	if pkt.QoS > 1 {
		return nil, nil, ErrQoSNotSupported
	}
	msg := packetToMessage(pkt)
	msg.PublisherClientID = c.clientID
	msg.ReceivedAt = time.Now()
	b.metrics.IncPublishIn(pkt.QoS, pkt.Retain)
	b.logger.Info("publish received",
		zap.String("client_id", c.clientID),
		zap.String("topic", pkt.Topic),
		zap.Uint8("qos", pkt.QoS),
		zap.Bool("retain", pkt.Retain),
	)

	b.mu.Lock()
	defer b.mu.Unlock()

	// Retained
	if pkt.Retain {
		if len(pkt.Payload) == 0 {
			b.stores.Retained.DeleteRetained(pkt.Topic)
		} else {
			b.stores.Retained.SetRetained(pkt.Topic, &RetainedMessage{Msg: msg})
		}
	}

	// Route to subscribers
	var deliveries []Delivery
	matchedSubscriptions := 0
	offlineQueued := 0
	b.stores.Sessions.ForEachSession(func(_ string, raw any) bool {
		sess := asSession(raw)
		if sess == nil {
			return true
		}
		for _, sub := range sess.Subs {
			if !MatchTopic(sub.Filter, msg.Topic) {
				continue
			}
			if sub.NoLocal && sess.ClientID == msg.PublisherClientID {
				continue
			}
			matchedSubscriptions++
			grantedQoS := pkt.QoS
			if sub.MaxQoS < grantedQoS {
				grantedQoS = sub.MaxQoS
			}
			if grantedQoS > 1 {
				grantedQoS = 1
			}
			forwardRetain := pkt.Retain && sub.RetainAsPublished
			p := messageToPublishPacket(msg, grantedQoS, forwardRetain)
			if sess.Conn != nil {
				if grantedQoS == 0 {
					deliveries = append(deliveries, Delivery{Target: sess, Packet: p})
				} else {
					if !b.canSendMore(sess.ClientID, sess) {
						if err := b.stores.Delivery.EnqueuePending(sess.ClientID, msg); err == nil {
							b.recordPendingAdded(sess.ClientID)
						}
						offlineQueued++
						continue
					}
					pid, err := b.stores.Delivery.ReservePacketID(sess.ClientID)
					if err != nil {
						continue
					}
					p.PacketID = pid
					if err := b.stores.Delivery.AddInflight(sess.ClientID, pid, msg); err != nil {
						continue
					}
					b.recordInflightAdded(sess.ClientID, pid)
					deliveries = append(deliveries, Delivery{Target: sess, Packet: p})
				}
			} else if grantedQoS == 1 {
				if err := b.stores.Delivery.EnqueuePending(sess.ClientID, msg); err == nil {
					b.recordPendingAdded(sess.ClientID)
				}
				offlineQueued++
			}
		}
		return true
	})

	var puback *PubackPacket
	if pkt.QoS == 1 {
		puback = &PubackPacket{PacketID: pkt.PacketID, ReasonCode: ReasonSuccess}
	}
	for _, d := range deliveries {
		pub, ok := d.Packet.(*PublishPacket)
		if !ok {
			continue
		}
		b.metrics.IncPublishForwarded(pub.QoS)
	}
	b.metrics.AddPublishMatches(matchedSubscriptions)
	b.metrics.AddPublishOfflineQueued(offlineQueued)
	b.logger.Info("publish forwarded",
		zap.String("client_id", c.clientID),
		zap.String("topic", pkt.Topic),
		zap.Uint8("qos", pkt.QoS),
		zap.Int("matched_subscriptions", matchedSubscriptions),
		zap.Int("forwarded_deliveries", len(deliveries)),
		zap.Int("offline_queued", offlineQueued),
	)
	return deliveries, puback, nil
}

// HandlePuback processes PUBACK from client.
func (b *Broker) HandlePuback(c *Conn, pkt *PubackPacket) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	sess := c.session
	if sess == nil {
		return nil
	}
	if err := b.stores.Delivery.RemoveInflight(sess.ClientID, pkt.PacketID); err == nil {
		b.recordInflightRemoved(sess.ClientID, pkt.PacketID, true)
	}
	// Drain pending queue into outbound
	b.drainPendingLocked(sess)
	return nil
}

// drainPendingLocked sends queued QoS1 messages to session if conn is set and receive max allows. Call with b.mu held.
func (b *Broker) drainPendingLocked(sess *Session) {
	if sess == nil || sess.Conn == nil {
		return
	}
	for b.canSendMore(sess.ClientID, sess) {
		raw, ok, err := b.stores.Delivery.DequeuePending(sess.ClientID)
		if err != nil || !ok {
			return
		}
		b.recordPendingRemoved(sess.ClientID)
		msg := asPublishMessage(raw)
		if msg == nil {
			continue
		}
		pid, err := b.stores.Delivery.ReservePacketID(sess.ClientID)
		if err != nil {
			return
		}
		p := messageToPublishPacket(msg, 1, msg.Retain)
		p.PacketID = pid
		p.Dup = true
		if err := b.stores.Delivery.AddInflight(sess.ClientID, pid, msg); err != nil {
			return
		}
		b.recordInflightAdded(sess.ClientID, pid)
		select {
		case sess.Conn.outbound <- p:
		default:
			b.metrics.IncOutboundQueueDrop("broker_drain_pending")
			if err := b.stores.Delivery.EnqueuePending(sess.ClientID, msg); err == nil {
				b.recordPendingAdded(sess.ClientID)
			}
			return
		}
	}
}

// HandleSubscribe processes SUBSCRIBE. Returns SUBACK and deliveries (retained + future live).
func (b *Broker) HandleSubscribe(c *Conn, pkt *SubscribePacket) (*SubackPacket, []Delivery, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.metrics.IncSubscribe()
	sess := c.session
	if sess == nil {
		return nil, nil, ErrProtocolError
	}

	reasonCodes := make([]byte, len(pkt.Filters))
	var deliveries []Delivery

	for i, f := range pkt.Filters {
		if IsSharedFilter(f.Filter) {
			reasonCodes[i] = ReasonWildcardSubscriptionNotSupported
			b.metrics.IncSubscribeRejection(reasonCodes[i])
			continue
		}
		if err := ValidateTopicFilter(f.Filter, false); err != nil {
			reasonCodes[i] = ReasonProtocolError
			b.metrics.IncSubscribeRejection(reasonCodes[i])
			continue
		}
		grantedQoS := f.QoS
		if grantedQoS > 1 {
			grantedQoS = 1
		}
		reasonCodes[i] = grantedQoS

		sub := &Subscription{
			Filter:            f.Filter,
			MaxQoS:            grantedQoS,
			NoLocal:           f.NoLocal,
			RetainAsPublished: f.RetainAsPublished,
			RetainHandling:    f.RetainHandling,
		}
		existing := sess.Subs[f.Filter]
		if err := b.stores.Sessions.UpsertSubscription(sess.ClientID, sub.Filter, sub); err != nil {
			reasonCodes[i] = ReasonUnspecifiedError
			b.metrics.IncSubscribeRejection(reasonCodes[i])
			continue
		}
		sess.Subs[f.Filter] = sub

		// Retained: per Retain Handling
		sendRetained := false
		switch sub.RetainHandling {
		case 0:
			sendRetained = true
		case 1:
			sendRetained = (existing == nil)
		case 2:
			sendRetained = false
		}
		if sendRetained {
			b.stores.Retained.ForEachRetained(func(topic string, raw any) bool {
				rm := asRetainedMessage(raw)
				if rm == nil {
					return true
				}
				if !MatchTopic(f.Filter, topic) {
					return true
				}
				if rm.Msg == nil {
					return true
				}
				gq := grantedQoS
				if rm.Msg.QoS < gq {
					gq = rm.Msg.QoS
				}
				retainFlag := rm.Msg.Retain && sub.RetainAsPublished
				p := messageToPublishPacket(rm.Msg, gq, retainFlag)
				p.Retain = true
				if sess.Conn != nil {
					if gq == 0 {
						deliveries = append(deliveries, Delivery{Target: sess, Packet: p})
					} else {
						if !b.canSendMore(sess.ClientID, sess) {
							if err := b.stores.Delivery.EnqueuePending(sess.ClientID, rm.Msg); err == nil {
								b.recordPendingAdded(sess.ClientID)
							}
							return true
						}
						pid, err := b.stores.Delivery.ReservePacketID(sess.ClientID)
						if err != nil {
							return true
						}
						p.PacketID = pid
						if err := b.stores.Delivery.AddInflight(sess.ClientID, pid, rm.Msg); err != nil {
							return true
						}
						b.recordInflightAdded(sess.ClientID, pid)
						deliveries = append(deliveries, Delivery{Target: sess, Packet: p})
					}
				}
				return true
			})
		}
	}

	suback := &SubackPacket{PacketID: pkt.PacketID, ReasonCodes: reasonCodes}
	return suback, deliveries, nil
}

// HandleUnsubscribe processes UNSUBSCRIBE.
func (b *Broker) HandleUnsubscribe(c *Conn, pkt *UnsubscribePacket) (*UnsubackPacket, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.metrics.IncUnsubscribe()
	sess := c.session
	if sess == nil {
		return nil, ErrProtocolError
	}
	reasonCodes := make([]byte, len(pkt.Filters))
	for i, filter := range pkt.Filters {
		_ = b.stores.Sessions.DeleteSubscription(sess.ClientID, filter)
		delete(sess.Subs, filter)
		reasonCodes[i] = ReasonSuccess
	}
	return &UnsubackPacket{PacketID: pkt.PacketID, ReasonCodes: reasonCodes}, nil
}

// HandleDisconnect processes graceful DISCONNECT from client.
func (b *Broker) HandleDisconnect(c *Conn, _ *DisconnectPacket) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	sess := c.session
	if sess == nil {
		return nil
	}
	// DISCONNECT is graceful in MQTT 3.1.1; suppress will.
	suppressWill := true
	sess.Conn = nil
	c.session = nil
	c.connected = false
	if suppressWill {
		sess.Will = nil
	}
	// Clean sessions are removed immediately.
	if sess.CleanStartAtConnect {
		b.removeSessionLocked(sess, false)
		return nil
	}
	b.recountSessionMetricsLocked()
	return nil
}

// HandleConnectionLost is called when connection drops without DISCONNECT (ungraceful).
func (b *Broker) HandleConnectionLost(c *Conn) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	sess := c.session
	if sess == nil {
		return nil
	}
	sess.Conn = nil
	c.session = nil
	c.connected = false
	if sess.Will != nil {
		b.publishWillLocked(sess.Will)
		sess.Will = nil
	}
	if sess.CleanStartAtConnect {
		b.removeSessionLocked(sess, false)
		return nil
	}
	b.recountSessionMetricsLocked()
	return nil
}
