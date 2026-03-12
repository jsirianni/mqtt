// Package memory provides in-memory implementations of storage contracts.
package memory

import (
	"fmt"
	"sync"

	"github.com/jsirianni/mqtt/storage/contracts"
)

// Stores is a unified in-memory implementation of all storage contracts.
type Stores struct {
	mu sync.RWMutex

	sessions       map[string]any
	subs           map[string]map[string]any
	retained       map[string]any
	generatedIDCtr uint64

	nextPacketID map[string]uint16
	inflight     map[string]map[uint16]any
	pending      map[string][]any
}

var _ contracts.SessionStore = (*Stores)(nil)
var _ contracts.RetainedStore = (*Stores)(nil)
var _ contracts.DeliveryStateStore = (*Stores)(nil)

// NewStores creates an empty in-memory store set.
func NewStores() *Stores {
	return &Stores{
		sessions:     make(map[string]any),
		subs:         make(map[string]map[string]any),
		retained:     make(map[string]any),
		nextPacketID: make(map[string]uint16),
		inflight:     make(map[string]map[uint16]any),
		pending:      make(map[string][]any),
	}
}

func (m *Stores) ensureClientLocked(clientID string) {
	if _, ok := m.subs[clientID]; !ok {
		m.subs[clientID] = make(map[string]any)
	}
	if _, ok := m.inflight[clientID]; !ok {
		m.inflight[clientID] = make(map[uint16]any)
	}
	if _, ok := m.pending[clientID]; !ok {
		m.pending[clientID] = nil
	}
	if _, ok := m.nextPacketID[clientID]; !ok {
		m.nextPacketID[clientID] = 1
	}
}

// NextGeneratedClientID returns the next generated client ID counter.
func (m *Stores) NextGeneratedClientID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.generatedIDCtr++
	return m.generatedIDCtr
}

// GetSession returns a stored session by client ID.
func (m *Stores) GetSession(clientID string) (any, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[clientID]
	return s, ok
}

// UpsertSession stores or replaces a session for a client ID.
func (m *Stores) UpsertSession(clientID string, session any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[clientID] = session
	m.ensureClientLocked(clientID)
}

// DeleteSession removes all state for a client ID.
func (m *Stores) DeleteSession(clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, clientID)
	delete(m.subs, clientID)
	delete(m.nextPacketID, clientID)
	delete(m.inflight, clientID)
	delete(m.pending, clientID)
}

// ForEachSession iterates stored sessions until callback returns false.
func (m *Stores) ForEachSession(fn func(clientID string, session any) bool) {
	m.mu.RLock()
	items := make([]struct {
		clientID string
		session  any
	}, 0, len(m.sessions))
	for clientID, session := range m.sessions {
		items = append(items, struct {
			clientID string
			session  any
		}{clientID: clientID, session: session})
	}
	m.mu.RUnlock()

	for _, it := range items {
		if !fn(it.clientID, it.session) {
			return
		}
	}
}

// GetSubscription returns a subscription by client ID and filter.
func (m *Stores) GetSubscription(clientID, filter string) (any, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	subs, ok := m.subs[clientID]
	if !ok {
		return nil, false
	}
	sub, ok := subs[filter]
	return sub, ok
}

// UpsertSubscription stores or replaces a subscription for a filter.
func (m *Stores) UpsertSubscription(clientID, filter string, sub any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[clientID]; !ok {
		return fmt.Errorf("session not found: %s", clientID)
	}
	m.ensureClientLocked(clientID)
	m.subs[clientID][filter] = sub
	return nil
}

// DeleteSubscription removes a subscription for a filter.
func (m *Stores) DeleteSubscription(clientID, filter string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[clientID]; !ok {
		return fmt.Errorf("session not found: %s", clientID)
	}
	delete(m.subs[clientID], filter)
	return nil
}

// ForEachSubscription iterates subscriptions until callback returns false.
func (m *Stores) ForEachSubscription(fn func(clientID string, session any, sub any) bool) {
	type item struct {
		clientID string
		session  any
		sub      any
	}
	m.mu.RLock()
	items := make([]item, 0)
	for clientID, session := range m.sessions {
		for _, sub := range m.subs[clientID] {
			items = append(items, item{
				clientID: clientID,
				session:  session,
				sub:      sub,
			})
		}
	}
	m.mu.RUnlock()
	for _, it := range items {
		if !fn(it.clientID, it.session, it.sub) {
			return
		}
	}
}

// SetRetained stores retained state for a topic.
func (m *Stores) SetRetained(topic string, msg any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retained[topic] = msg
}

// DeleteRetained deletes retained state for a topic.
func (m *Stores) DeleteRetained(topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.retained, topic)
}

// ForEachRetained iterates retained state until callback returns false.
func (m *Stores) ForEachRetained(fn func(topic string, msg any) bool) {
	type item struct {
		topic string
		msg   any
	}
	m.mu.RLock()
	items := make([]item, 0, len(m.retained))
	for topic, msg := range m.retained {
		items = append(items, item{topic: topic, msg: msg})
	}
	m.mu.RUnlock()
	for _, it := range items {
		if !fn(it.topic, it.msg) {
			return
		}
	}
}

// ReservePacketID reserves the next packet ID for a client.
func (m *Stores) ReservePacketID(clientID string) (uint16, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[clientID]; !ok {
		return 0, fmt.Errorf("session not found: %s", clientID)
	}
	m.ensureClientLocked(clientID)
	pid := m.nextPacketID[clientID]
	if pid == 0 {
		pid = 1
	}
	m.nextPacketID[clientID] = pid + 1
	if m.nextPacketID[clientID] == 0 {
		m.nextPacketID[clientID] = 1
	}
	return pid, nil
}

// AddInflight tracks one inflight QoS1 message.
func (m *Stores) AddInflight(clientID string, pktID uint16, msg any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[clientID]; !ok {
		return fmt.Errorf("session not found: %s", clientID)
	}
	m.ensureClientLocked(clientID)
	m.inflight[clientID][pktID] = msg
	return nil
}

// RemoveInflight removes one inflight QoS1 message by packet ID.
func (m *Stores) RemoveInflight(clientID string, pktID uint16) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[clientID]; !ok {
		return fmt.Errorf("session not found: %s", clientID)
	}
	delete(m.inflight[clientID], pktID)
	return nil
}

// InflightCount reports the inflight QoS1 count for a client.
func (m *Stores) InflightCount(clientID string) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.sessions[clientID]; !ok {
		return 0, fmt.Errorf("session not found: %s", clientID)
	}
	return len(m.inflight[clientID]), nil
}

// EnqueuePending appends one pending message for a client.
func (m *Stores) EnqueuePending(clientID string, msg any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[clientID]; !ok {
		return fmt.Errorf("session not found: %s", clientID)
	}
	m.ensureClientLocked(clientID)
	m.pending[clientID] = append(m.pending[clientID], msg)
	return nil
}

// DequeuePending pops the oldest pending message for a client.
func (m *Stores) DequeuePending(clientID string) (any, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[clientID]; !ok {
		return nil, false, fmt.Errorf("session not found: %s", clientID)
	}
	q := m.pending[clientID]
	if len(q) == 0 {
		return nil, false, nil
	}
	msg := q[0]
	m.pending[clientID] = q[1:]
	return msg, true, nil
}
