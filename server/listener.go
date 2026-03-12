package server

import (
	"errors"
	"net"
	"sync"

	"go.uber.org/zap"
)

// Listener accepts TCP connections and spawns a Conn per client.
type Listener struct {
	addr    string
	broker  *Broker
	logger  *zap.Logger
	metrics MetricsCollector
	ln      net.Listener
	wg      sync.WaitGroup

	mu    sync.Mutex
	conns map[*Conn]struct{}
}

// NewListener creates a listener bound to an address and broker.
func NewListener(addr string, broker *Broker, logger *zap.Logger, metrics MetricsCollector) *Listener {
	if metrics == nil {
		metrics = NewNoopMetricsCollector()
	}
	return &Listener{
		addr:    addr,
		broker:  broker,
		logger:  logger,
		metrics: metrics,
		conns:   make(map[*Conn]struct{}),
	}
}

// Start begins accepting connections. Blocks until Listen fails.
func (l *Listener) Start() error {
	ln, err := net.Listen("tcp", l.addr)
	if err != nil {
		l.metrics.IncListenerError()
		return err
	}
	l.ln = ln
	l.metrics.SetListenerUp(true)
	l.logger.Info("server started", zap.String("listen_addr", l.addr))
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			l.metrics.IncListenerError()
			return err
		}
		l.logger.Info("client connection accepted", zap.String("remote_addr", conn.RemoteAddr().String()))
		l.metrics.IncConnectionsAccepted()
		l.wg.Add(1)
		c := NewConn(conn, l.broker, l.logger.Named("conn"))
		l.trackConn(c, true)
		go func() {
			defer l.wg.Done()
			defer l.trackConn(c, false)
			c.Run()
		}()
	}
}

// Close stops the listener.
func (l *Listener) Close() error {
	l.logger.Info("stopping listener")
	l.metrics.SetListenerUp(false)
	if l.ln != nil {
		err := l.ln.Close()
		for _, c := range l.snapshotConns() {
			c.closeWithContext(true, ReasonSuccess, false, false)
		}
		l.wg.Wait()
		l.logger.Info("listener stopped")
		return err
	}
	return nil
}

func (l *Listener) trackConn(c *Conn, add bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if add {
		l.conns[c] = struct{}{}
		l.metrics.AddConnectionsActive(1)
		return
	}
	delete(l.conns, c)
	l.metrics.AddConnectionsActive(-1)
}

func (l *Listener) snapshotConns() []*Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*Conn, 0, len(l.conns))
	for c := range l.conns {
		out = append(out, c)
	}
	return out
}
