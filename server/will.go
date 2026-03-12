package server

import "time"

// WillMessage holds a stored will for a session.
type WillMessage struct {
	Msg           *PublishMessage
	DelayInterval uint32
	CreatedAt     time.Time
}
