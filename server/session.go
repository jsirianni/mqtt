package server

import "time"

// Subscription is one subscription in a session.
type Subscription struct {
	Filter            string
	MaxQoS            byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
	IsShared          bool
}

// InflightPublish tracks a server->client QoS1 publish awaiting PUBACK.
type InflightPublish struct {
	PacketID uint16
	Message  *PublishMessage
}

// Session holds client session state (in-memory only in v1).
type Session struct {
	ClientID              string
	Conn                  *Conn
	CleanStartAtConnect   bool
	SessionExpiryInterval uint32
	ExpiresAt             time.Time

	Username             string
	ClientReceiveMaximum uint16
	ClientMaxPacketSize  uint32
	RequestProblemInfo   bool

	Subs map[string]*Subscription
	Will *WillMessage

	InflightOutbound map[uint16]*InflightPublish
	NextPacketID     uint16
	PendingQueue     []*PublishMessage
}

// NewSession creates a new session with initialized maps/slices.
func NewSession(clientID string) *Session {
	return &Session{
		ClientID:             clientID,
		Subs:                 make(map[string]*Subscription),
		InflightOutbound:     make(map[uint16]*InflightPublish),
		ClientReceiveMaximum: 65535,
	}
}

// InflightCount returns the number of unacknowledged outbound QoS1 publishes.
func (s *Session) InflightCount() int {
	return len(s.InflightOutbound)
}

// CanSendMore returns true if we can send another QoS1 to this client (receive max not reached).
func (s *Session) CanSendMore() bool {
	return s.InflightCount() < int(s.ClientReceiveMaximum)
}
