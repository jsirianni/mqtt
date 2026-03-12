package server

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

type Option func(*serverOptions) error

type serverOptions struct {
	cfg    Config
	logger *zap.Logger
}

func WithListenAddr(addr string) Option {
	return func(o *serverOptions) error {
		o.cfg.ListenAddr = addr
		return nil
	}
}

func WithMaxPacketSize(n uint32) Option {
	return func(o *serverOptions) error {
		o.cfg.MaxPacketSize = n
		return nil
	}
}

func WithReceiveMaximum(n uint16) Option {
	return func(o *serverOptions) error {
		o.cfg.ReceiveMaximum = n
		return nil
	}
}

func WithMaxOutboundQueue(n int) Option {
	return func(o *serverOptions) error {
		o.cfg.MaxOutboundQueue = n
		return nil
	}
}

func WithMaxSessionQueue(n int) Option {
	return func(o *serverOptions) error {
		o.cfg.MaxSessionQueue = n
		return nil
	}
}

func WithWriteTimeout(d time.Duration) Option {
	return func(o *serverOptions) error {
		o.cfg.WriteTimeout = d
		return nil
	}
}

func WithReadTimeout(d time.Duration) Option {
	return func(o *serverOptions) error {
		o.cfg.ReadTimeout = d
		return nil
	}
}

func WithSessionSweepInterval(d time.Duration) Option {
	return func(o *serverOptions) error {
		o.cfg.SessionSweepInterval = d
		return nil
	}
}

func WithLogger(l *zap.Logger) Option {
	return func(o *serverOptions) error {
		o.logger = l
		return nil
	}
}

func validateConfig(cfg Config) error {
	if cfg.ListenAddr == "" {
		return fmt.Errorf("listen address must not be empty")
	}
	if cfg.ReceiveMaximum == 0 {
		return fmt.Errorf("receive maximum must be > 0")
	}
	if cfg.MaxOutboundQueue <= 0 {
		return fmt.Errorf("max outbound queue must be > 0")
	}
	if cfg.MaxSessionQueue <= 0 {
		return fmt.Errorf("max session queue must be > 0")
	}
	if cfg.MaxPacketSize == 0 {
		return fmt.Errorf("max packet size must be > 0")
	}
	return nil
}
