// Package main provides the mqtt server CLI entrypoint.
package main

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	telemetrymetrics "github.com/jsirianni/mqtt/internal/telemetry/metrics"
	"github.com/jsirianni/mqtt/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

var (
	version  = "dev"
	revision = "unknown"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mqtt",
		Short: "MQTT server",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServer(cmd.Context())
		},
	}

	flags := cmd.Flags()
	flags.String("config", "", "config file path")
	flags.String("listen-addr", ":1883", "server listen address")
	flags.Uint32("max-packet-size", 1<<20, "maximum packet size")
	flags.Uint16("receive-maximum", 32, "receive maximum")
	flags.Int("max-outbound-queue", 1024, "max outbound queue")
	flags.Int("max-session-queue", 1024, "max session queue")
	flags.Duration("write-timeout", 10*time.Second, "write timeout")
	flags.Duration("read-timeout", 0, "read timeout")
	flags.Duration("session-sweep-interval", 30*time.Second, "session sweep interval")
	flags.Bool("metrics-enabled", true, "enable Prometheus metrics endpoint")
	flags.String("metrics-listen-addr", ":9464", "Prometheus metrics listen address")
	flags.String("log-level", "info", "log level")
	flags.String("log-encoding", "json", "log encoding (forced to json)")

	_ = viper.BindPFlag("server.listen_addr", flags.Lookup("listen-addr"))
	_ = viper.BindPFlag("server.max_packet_size", flags.Lookup("max-packet-size"))
	_ = viper.BindPFlag("server.receive_maximum", flags.Lookup("receive-maximum"))
	_ = viper.BindPFlag("server.max_outbound_queue", flags.Lookup("max-outbound-queue"))
	_ = viper.BindPFlag("server.max_session_queue", flags.Lookup("max-session-queue"))
	_ = viper.BindPFlag("server.write_timeout", flags.Lookup("write-timeout"))
	_ = viper.BindPFlag("server.read_timeout", flags.Lookup("read-timeout"))
	_ = viper.BindPFlag("server.session_sweep_interval", flags.Lookup("session-sweep-interval"))
	_ = viper.BindPFlag("metrics.enabled", flags.Lookup("metrics-enabled"))
	_ = viper.BindPFlag("metrics.listen_addr", flags.Lookup("metrics-listen-addr"))
	_ = viper.BindPFlag("log.level", flags.Lookup("log-level"))
	_ = viper.BindPFlag("log.encoding", flags.Lookup("log-encoding"))

	cobra.OnInitialize(func() {
		cfgPath, _ := flags.GetString("config")
		if cfgPath != "" {
			viper.SetConfigFile(cfgPath)
			_ = viper.ReadInConfig()
		}
		viper.SetEnvPrefix("MQTT")
		viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		viper.AutomaticEnv()
	})

	return cmd
}

func runServer(ctx context.Context) error {
	logger, err := newLogger()
	if err != nil {
		return err
	}
	defer func() { _ = logger.Sync() }()

	log := logger.Named("main")
	log.Info("starting mqtt server")

	metricsCollector := server.NewNoopMetricsCollector()
	var metricsProvider *telemetrymetrics.Provider
	if viper.GetBool("metrics.enabled") {
		provider, err := telemetrymetrics.NewProvider("mqtt", version, revision)
		if err != nil {
			return err
		}
		if err := provider.Start(ctx); err != nil {
			return err
		}
		metricsProvider = provider
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = metricsProvider.Shutdown(shutdownCtx)
		}()

		collector, err := server.NewOTelMetricsCollector(otel.Meter("github.com/jsirianni/mqtt/server"))
		if err != nil {
			return err
		}
		collector.SetBuildInfo(version, revision)
		metricsCollector = collector
	}

	srv, err := server.New(
		server.WithLogger(logger.Named("mqtt")),
		server.WithListenAddr(viper.GetString("server.listen_addr")),
		server.WithMaxPacketSize(viper.GetUint32("server.max_packet_size")),
		server.WithReceiveMaximum(receiveMaximum()),
		server.WithMaxOutboundQueue(viper.GetInt("server.max_outbound_queue")),
		server.WithMaxSessionQueue(viper.GetInt("server.max_session_queue")),
		server.WithWriteTimeout(viper.GetDuration("server.write_timeout")),
		server.WithReadTimeout(viper.GetDuration("server.read_timeout")),
		server.WithSessionSweepInterval(viper.GetDuration("server.session_sweep_interval")),
		server.WithMetrics(metricsCollector),
	)
	if err != nil {
		return err
	}

	runCtx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if viper.GetBool("metrics.enabled") {
		httpSrv := &http.Server{
			Addr:              viper.GetString("metrics.listen_addr"),
			Handler:           telemetrymetrics.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			log.Info("metrics endpoint listening", zap.String("listen_addr", httpSrv.Addr))
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("metrics endpoint failed", zap.Error(err))
			}
		}()
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			_ = httpSrv.Shutdown(shutdownCtx)
		}()
	}

	return srv.Start(runCtx)
}

func newLogger() (*zap.Logger, error) {
	level := strings.ToLower(viper.GetString("log.level"))
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "json"
	switch level {
	case "debug":
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "warn":
		cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		cfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	return cfg.Build()
}

func receiveMaximum() uint16 {
	n := viper.GetUint("server.receive_maximum")
	if n > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(n)
}
