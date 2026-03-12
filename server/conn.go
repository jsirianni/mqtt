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
}

func NewConn(netConn net.Conn, broker *Broker, logger *zap.Logger) *Conn {
	return &Conn{
		netConn:  netConn,
		broker:   broker,
		logger:   logger,
		outbound: make(chan Packet, broker.cfg.MaxOutboundQueue),
		closed:   make(chan struct{}),
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
		_ = c.netConn.Close()
	})
}

func (c *Conn) Run() {
	go c.writeLoop()
	c.readLoop()
}

func (c *Conn) readLoop() {
	maxSize := c.broker.cfg.MaxPacketSize
	for {
		pkt, _, err := ReadPacket(c.netConn, maxSize)
		if err != nil {
			c.logger.Warn("connection read failed before/within session",
				zap.String("remote_addr", c.netConn.RemoteAddr().String()),
				zap.Error(err),
			)
			c.closeWithContext(false, ReasonUnspecifiedError, true, false)
			return
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
			if err := WritePacket(c.netConn, pkt, maxSize); err != nil {
				c.closeWithContext(false, ReasonUnspecifiedError, false, false)
				return
			}
		}
	}
}
