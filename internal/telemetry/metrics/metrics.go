// Package metrics wires OpenTelemetry metric export for mqtt.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"os"

	promclient "github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

// Provider manages metric provider lifecycle and Prometheus export.
type Provider struct {
	serviceName string
	version     string
	revision    string

	resources *resource.Resource
	provider  *sdkmetric.MeterProvider
}

// NewProvider creates a new metric provider configuration.
func NewProvider(serviceName, version, revision string) (*Provider, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("get hostname: %w", err)
	}

	attrs := []attribute.KeyValue{
		semconv.ServiceNameKey.String(serviceName),
		semconv.HostNameKey.String(hostname),
	}
	if version != "" {
		attrs = append(attrs, semconv.ServiceVersionKey.String(version))
	}
	if revision != "" {
		attrs = append(attrs, attribute.String("service.revision", revision))
	}

	return &Provider{
		serviceName: serviceName,
		version:     version,
		revision:    revision,
		resources:   resource.NewWithAttributes(semconv.SchemaURL, attrs...),
	}, nil
}

// Start initializes Prometheus export and sets the global meter provider.
func (p *Provider) Start(_ context.Context) error {
	exporter, err := otelprom.New(otelprom.WithNamespace(p.serviceName))
	if err != nil {
		return fmt.Errorf("create prometheus exporter: %w", err)
	}

	p.provider = sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithResource(p.resources),
	)
	otel.SetMeterProvider(p.provider)
	return nil
}

// Shutdown flushes and closes the metric provider.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.provider == nil {
		return nil
	}
	return p.provider.Shutdown(ctx)
}

// Handler returns the Prometheus scrape HTTP handler.
func Handler() http.Handler {
	return promclient.Handler()
}
