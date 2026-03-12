// Package contracts defines storage contracts used by the MQTT broker.
package contracts

// SessionStore manages session identity and subscription indexing.
type SessionStore interface {
	NextGeneratedClientID() uint64
	GetSession(clientID string) (any, bool)
	UpsertSession(clientID string, session any)
	DeleteSession(clientID string)
	ForEachSession(fn func(clientID string, session any) bool)

	GetSubscription(clientID, filter string) (any, bool)
	UpsertSubscription(clientID, filter string, sub any) error
	DeleteSubscription(clientID, filter string) error
	ForEachSubscription(fn func(clientID string, session any, sub any) bool)
}

// RetainedStore manages retained topic state.
type RetainedStore interface {
	SetRetained(topic string, msg any)
	DeleteRetained(topic string)
	ForEachRetained(fn func(topic string, msg any) bool)
}

// DeliveryStateStore manages QoS1 inflight and pending message state.
type DeliveryStateStore interface {
	ReservePacketID(clientID string) (uint16, error)
	AddInflight(clientID string, pktID uint16, msg any) error
	RemoveInflight(clientID string, pktID uint16) error
	InflightCount(clientID string) (int, error)
	EnqueuePending(clientID string, msg any) error
	DequeuePending(clientID string) (any, bool, error)
}

// BrokerStores composes the storage dependencies used by the broker.
type BrokerStores struct {
	Sessions SessionStore
	Retained RetainedStore
	Delivery DeliveryStateStore
}
