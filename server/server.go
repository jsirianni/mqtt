package server

import (
	"context"

	"go.uber.org/zap"
)

// Server bundles broker state and network listener lifecycle.
type Server struct {
	cfg      Config
	logger   *zap.Logger
	broker   *Broker
	listener *Listener
}

// New constructs a server from the provided options.
func New(opts ...Option) (*Server, error) {
	o := &serverOptions{
		cfg:    defaultConfig(),
		logger: zap.NewNop(),
	}
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, err
		}
	}
	if err := validateConfig(o.cfg); err != nil {
		return nil, err
	}

	baseLogger := o.logger.Named("server")
	broker := NewBroker(o.cfg, baseLogger.Named("broker"), o.stores)
	listener := NewListener(o.cfg.ListenAddr, broker, baseLogger.Named("listener"))
	return &Server{
		cfg:      o.cfg,
		logger:   baseLogger,
		broker:   broker,
		listener: listener,
	}, nil
}

// Start runs the listener until the context is canceled or a listener error occurs.
func (s *Server) Start(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.listener.Start()
	}()

	select {
	case <-ctx.Done():
		return s.Stop(context.Background())
	case err := <-errCh:
		return err
	}
}

// Stop closes the server listener and active connections.
func (s *Server) Stop(_ context.Context) error {
	if s.listener == nil {
		return nil
	}
	return s.listener.Close()
}
