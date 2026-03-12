package server

import "time"

// Config holds broker configuration.
type Config struct {
	ListenAddr           string
	MaxPacketSize        uint32
	ReceiveMaximum       uint16
	MaxOutboundQueue     int
	MaxSessionQueue      int
	WriteTimeout         time.Duration
	ReadTimeout          time.Duration
	SessionSweepInterval time.Duration
}

func defaultConfig() Config {
	return Config{
		ListenAddr:           ":1883",
		MaxPacketSize:        1 << 20, // 1 MiB
		ReceiveMaximum:       32,
		MaxOutboundQueue:     1024,
		MaxSessionQueue:      1024,
		ReadTimeout:          0,
		WriteTimeout:         10 * time.Second,
		SessionSweepInterval: 30 * time.Second,
	}
}
