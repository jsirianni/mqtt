package server

import "time"

// PublishMessage is the internal message representation (not the wire PUBLISH packet).
type PublishMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
	Dup     bool

	PayloadFormatIndicator *byte
	MessageExpiryInterval  *uint32
	ContentType            *string
	ResponseTopic          *string
	CorrelationData        []byte
	UserProperties         [][2]string

	ReceivedAt        time.Time
	PublisherClientID string
}

// RetainedMessage stores one retained message and optional expiry.
type RetainedMessage struct {
	Msg       *PublishMessage
	ExpiresAt *time.Time
}
