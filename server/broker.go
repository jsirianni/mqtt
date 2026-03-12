package server

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Delivery is an internal routing instruction: send Packet to Target session.
type Delivery struct {
	Target *Session
	Packet Packet
}

// Broker holds shared in-memory state.
type Broker struct {
	mu             sync.RWMutex
	cfg            Config
	sessions       map[string]*Session
	retained       map[string]*RetainedMessage
	generatedIDCtr uint64
	logger         *zap.Logger
}

// NewBroker creates a broker with the given config.
func NewBroker(cfg Config, logger *zap.Logger) *Broker {
	return &Broker{
		cfg:      cfg,
		sessions: make(map[string]*Session),
		retained: make(map[string]*RetainedMessage),
		logger:   logger,
	}
}

// HandleConnect processes CONNECT and returns CONNACK or error.
func (b *Broker) HandleConnect(c *Conn, pkt *ConnectPacket) (*ConnackPacket, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	clientID := pkt.ClientID
	assigned := false
	if clientID == "" {
		b.generatedIDCtr++
		clientID = fmt.Sprintf("gen-%d", b.generatedIDCtr)
		assigned = true
	}

	sessionPresent := false
	existing := b.sessions[clientID]

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
		b.sessions[clientID] = sess
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
	delete(b.sessions, sess.ClientID)
	if publishWill && sess.Will != nil {
		b.publishWillLocked(sess.Will)
	}
}

func (b *Broker) publishWillLocked(will *WillMessage) {
	msg := will.Msg
	// Apply delay: if delay > 0 we would schedule; v1 simplification: publish immediately if delay 0
	if will.DelayInterval > 0 {
		// In full impl we'd schedule; for v1 we still publish (design says "maintain one timer per disconnected session")
		// Simplification: publish now
	}
	b.routeLocked(msg, nil)
}

// routeLocked distributes msg to matching sessions. Call with b.mu held. skipSession optionally skips one (e.g. no-local).
func (b *Broker) routeLocked(msg *PublishMessage, skipSession *Session) {
	for _, sess := range b.sessions {
		if sess == skipSession {
			continue
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
	}
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
		pid := sess.NextPacketID
		sess.NextPacketID++
		if sess.NextPacketID == 0 {
			sess.NextPacketID++
		}
		pkt.PacketID = pid
		pkt.Dup = false
		sess.InflightOutbound[pid] = &InflightPublish{PacketID: pid, Message: msg}
		return &Delivery{Target: sess, Packet: pkt}
	}
	// Offline: queue QoS 1 only
	if grantedQoS == 1 {
		sess.PendingQueue = append(sess.PendingQueue, msg)
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
		// Queue full; could add to session pending (v1: drop or queue)
		if pub, ok := d.Packet.(*PublishPacket); ok && pub.QoS == 1 {
			if _, ok := d.Target.InflightOutbound[pub.PacketID]; ok {
				delete(d.Target.InflightOutbound, pub.PacketID)
			}
			d.Target.PendingQueue = append(d.Target.PendingQueue, packetToMessage(pub))
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
			delete(b.retained, pkt.Topic)
		} else {
			b.retained[pkt.Topic] = &RetainedMessage{Msg: msg}
		}
	}

	// Route to subscribers
	var deliveries []Delivery
	matchedSubscriptions := 0
	offlineQueued := 0
	for _, sess := range b.sessions {
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
					if !sess.CanSendMore() {
						sess.PendingQueue = append(sess.PendingQueue, msg)
						offlineQueued++
						continue
					}
					pid := sess.NextPacketID
					sess.NextPacketID++
					if sess.NextPacketID == 0 {
						sess.NextPacketID++
					}
					p.PacketID = pid
					sess.InflightOutbound[pid] = &InflightPublish{PacketID: pid, Message: msg}
					deliveries = append(deliveries, Delivery{Target: sess, Packet: p})
				}
			} else if grantedQoS == 1 {
				sess.PendingQueue = append(sess.PendingQueue, msg)
				offlineQueued++
			}
		}
	}

	var puback *PubackPacket
	if pkt.QoS == 1 {
		puback = &PubackPacket{PacketID: pkt.PacketID, ReasonCode: ReasonSuccess}
	}
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
	delete(sess.InflightOutbound, pkt.PacketID)
	// Drain pending queue into outbound
	b.drainPendingLocked(sess)
	return nil
}

// drainPendingLocked sends queued QoS1 messages to session if conn is set and receive max allows. Call with b.mu held.
func (b *Broker) drainPendingLocked(sess *Session) {
	if sess.Conn == nil || len(sess.PendingQueue) == 0 {
		return
	}
	for len(sess.PendingQueue) > 0 && sess.CanSendMore() {
		msg := sess.PendingQueue[0]
		sess.PendingQueue = sess.PendingQueue[1:]
		pid := sess.NextPacketID
		sess.NextPacketID++
		if sess.NextPacketID == 0 {
			sess.NextPacketID++
		}
		p := messageToPublishPacket(msg, 1, msg.Retain)
		p.PacketID = pid
		p.Dup = true
		sess.InflightOutbound[pid] = &InflightPublish{PacketID: pid, Message: msg}
		select {
		case sess.Conn.outbound <- p:
		default:
			sess.PendingQueue = append([]*PublishMessage{msg}, sess.PendingQueue...)
			return
		}
	}
}

// HandleSubscribe processes SUBSCRIBE. Returns SUBACK and deliveries (retained + future live).
func (b *Broker) HandleSubscribe(c *Conn, pkt *SubscribePacket) (*SubackPacket, []Delivery, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sess := c.session
	if sess == nil {
		return nil, nil, ErrProtocolError
	}

	reasonCodes := make([]byte, len(pkt.Filters))
	var deliveries []Delivery

	for i, f := range pkt.Filters {
		if IsSharedFilter(f.Filter) {
			reasonCodes[i] = ReasonWildcardSubscriptionNotSupported
			continue
		}
		if err := ValidateTopicFilter(f.Filter, false); err != nil {
			reasonCodes[i] = ReasonProtocolError
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
			for topic, rm := range b.retained {
				if !MatchTopic(f.Filter, topic) {
					continue
				}
				if rm.Msg == nil {
					continue
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
						if !sess.CanSendMore() {
							sess.PendingQueue = append(sess.PendingQueue, rm.Msg)
							continue
						}
						pid := sess.NextPacketID
						sess.NextPacketID++
						if sess.NextPacketID == 0 {
							sess.NextPacketID++
						}
						p.PacketID = pid
						sess.InflightOutbound[pid] = &InflightPublish{PacketID: pid, Message: rm.Msg}
						deliveries = append(deliveries, Delivery{Target: sess, Packet: p})
					}
				}
			}
		}
	}

	suback := &SubackPacket{PacketID: pkt.PacketID, ReasonCodes: reasonCodes}
	return suback, deliveries, nil
}

// HandleUnsubscribe processes UNSUBSCRIBE.
func (b *Broker) HandleUnsubscribe(c *Conn, pkt *UnsubscribePacket) (*UnsubackPacket, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sess := c.session
	if sess == nil {
		return nil, ErrProtocolError
	}
	reasonCodes := make([]byte, len(pkt.Filters))
	for i, filter := range pkt.Filters {
		delete(sess.Subs, filter)
		reasonCodes[i] = ReasonSuccess
	}
	return &UnsubackPacket{PacketID: pkt.PacketID, ReasonCodes: reasonCodes}, nil
}

// HandleDisconnect processes graceful DISCONNECT from client.
func (b *Broker) HandleDisconnect(c *Conn, pkt *DisconnectPacket) error {
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
	}
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
	}
	return nil
}
