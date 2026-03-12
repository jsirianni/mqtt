package server

import (
	"net"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Conn owns one client socket and runs read/write loops.
type Conn struct {
	netConn   net.Conn
	broker    *Broker
	logger    *zap.Logger
	outbound  chan Packet
	closed    chan struct{}
	closeOnce sync.Once

	session   *Session
	clientID  string
	connected bool

	lastRead  time.Time
	keepAlive uint16
	openedAt  time.Time
}

// NewConn constructs a connection wrapper around a net.Conn.
func NewConn(netConn net.Conn, broker *Broker, logger *zap.Logger) *Conn {
	return &Conn{
		netConn:  netConn,
		broker:   broker,
		logger:   logger,
		outbound: make(chan Packet, broker.cfg.MaxOutboundQueue),
		closed:   make(chan struct{}),
		openedAt: time.Now(),
	}
}

// closeWithContext closes the connection.
func (c *Conn) closeWithContext(graceful bool, reasonCode byte, sendDisconnect bool, isTakeover bool) {
	c.closeOnce.Do(func() {
		close(c.closed)
		if !isTakeover && !graceful {
			_ = c.broker.HandleConnectionLost(c)
		}
		_ = reasonCode
		_ = sendDisconnect
		mode := "ungraceful"
		if isTakeover {
			mode = "takeover"
		} else if graceful {
			mode = "graceful"
		}
		c.broker.metrics.IncConnectionsClosed(mode)
		c.broker.metrics.ObserveConnectionDuration(time.Since(c.openedAt))
		_ = c.netConn.Close()
	})
}

// Run starts read/write loops and blocks until read loop exits.
func (c *Conn) Run() {
	go c.writeLoop()
	c.readLoop()
}

func (c *Conn) readLoop() {
	maxSize := c.broker.cfg.MaxPacketSize
	for {
		pkt, packetSize, err := ReadPacket(c.netConn, maxSize)
		if err != nil {
			c.broker.metrics.IncDecodeError("read_packet", errorReasonLabel(err))
			c.logger.Warn("connection read failed before/within session",
				zap.String("remote_addr", c.netConn.RemoteAddr().String()),
				zap.Error(err),
			)
			c.closeWithContext(false, ReasonUnspecifiedError, true, false)
			return
		}
		if packetSize > 0 {
			c.broker.metrics.ObservePacketSize("in", int64(packetSize))
		}
		if inbound, ok := pkt.(Packet); ok {
			c.broker.metrics.IncPacketsIn(packetTypeLabel(inbound.PacketType()))
		}
		if !c.connected {
			if _, ok := pkt.(*ConnectPacket); !ok {
				c.logger.Warn("first packet is not CONNECT",
					zap.String("remote_addr", c.netConn.RemoteAddr().String()),
				)
				c.closeWithContext(false, ReasonProtocolError, true, false)
				return
			}
		}
		c.lastRead = time.Now()

		switch p := pkt.(type) {
		case *ConnectPacket:
			connack, err := c.broker.HandleConnect(c, p)
			if err != nil {
				reason := ReasonCodeFor(err)
				c.broker.metrics.IncConnectRejection(reason)
				c.logger.Warn("client connect rejected",
					zap.String("remote_addr", c.netConn.RemoteAddr().String()),
					zap.Error(err),
					zap.Uint8("reason_code", reason),
				)
				c.closeWithContext(false, reason, true, false)
				return
			}
			c.logger.Info("client connected",
				zap.String("client_id", c.clientID),
				zap.String("remote_addr", c.netConn.RemoteAddr().String()),
			)
			c.keepAlive = p.KeepAlive
			select {
			case c.outbound <- connack:
			case <-c.closed:
				return
			}
		case *PublishPacket:
			deliveries, puback, err := c.broker.HandlePublish(c, p)
			if err != nil {
				c.closeWithContext(false, ReasonCodeFor(err), true, false)
				return
			}
			for _, d := range deliveries {
				if d.Target != nil && d.Target.Conn != nil {
					select {
					case d.Target.Conn.outbound <- d.Packet:
					case <-d.Target.Conn.closed:
					default:
						c.broker.metrics.IncOutboundQueueDrop("conn_publish_fanout")
					}
				}
			}
			if puback != nil {
				select {
				case c.outbound <- puback:
				case <-c.closed:
					return
				}
			}
		case *PubackPacket:
			if err := c.broker.HandlePuback(c, p); err != nil {
				c.closeWithContext(false, ReasonCodeFor(err), true, false)
				return
			}
		case *SubscribePacket:
			suback, deliveries, err := c.broker.HandleSubscribe(c, p)
			if err != nil {
				c.closeWithContext(false, ReasonCodeFor(err), true, false)
				return
			}
			select {
			case c.outbound <- suback:
			case <-c.closed:
				return
			}
			for _, d := range deliveries {
				if d.Target != nil && d.Target.Conn != nil {
					select {
					case d.Target.Conn.outbound <- d.Packet:
					case <-d.Target.Conn.closed:
					default:
						c.broker.metrics.IncOutboundQueueDrop("conn_subscribe_retained_fanout")
					}
				}
			}
		case *UnsubscribePacket:
			unsuback, err := c.broker.HandleUnsubscribe(c, p)
			if err != nil {
				c.closeWithContext(false, ReasonCodeFor(err), true, false)
				return
			}
			select {
			case c.outbound <- unsuback:
			case <-c.closed:
				return
			}
		case *PingreqPacket:
			select {
			case c.outbound <- &PingrespPacket{}:
			case <-c.closed:
				return
			}
		case *DisconnectPacket:
			_ = c.broker.HandleDisconnect(c, p)
			c.closeWithContext(true, 0, false, false)
			return
		default:
			c.closeWithContext(false, ReasonProtocolError, true, false)
			return
		}
	}
}

func (c *Conn) writeLoop() {
	timeout := c.broker.cfg.WriteTimeout
	for {
		select {
		case <-c.closed:
			return
		case pkt, ok := <-c.outbound:
			if !ok {
				return
			}
			if timeout > 0 {
				_ = c.netConn.SetWriteDeadline(time.Now().Add(timeout))
			}
			maxSize := uint32(268435455)
			if c.session != nil {
				maxSize = c.session.ClientMaxPacketSize
			}
			cw := &countingWriter{w: c.netConn}
			packetType := packetTypeLabel(pkt.PacketType())
			if err := WritePacket(cw, pkt, maxSize); err != nil {
				c.broker.metrics.IncEncodeError(packetType, errorReasonLabel(err))
				c.closeWithContext(false, ReasonUnspecifiedError, false, false)
				return
			}
			c.broker.metrics.IncPacketsOut(packetType)
			c.broker.metrics.ObservePacketSize("out", int64(cw.n))
		}
	}
}

type countingWriter struct {
	w net.Conn
	n int
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += n
	return n, err
}
