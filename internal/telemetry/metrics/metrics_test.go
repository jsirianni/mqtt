package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
)

func TestProviderStartAndScrape(t *testing.T) {
	provider, err := NewProvider("mqtttest", "test-version", "test-revision")
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	if err := provider.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if shutdownErr := provider.Shutdown(context.Background()); shutdownErr != nil {
			t.Fatalf("Shutdown() error = %v", shutdownErr)
		}
	})

	meter := otel.Meter("metrics-test")
	counter, err := meter.Int64Counter("mqtt_test_counter_total")
	if err != nil {
		t.Fatalf("Int64Counter() error = %v", err)
	}
	counter.Add(context.Background(), 3)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "mqtttest_mqtt_test_counter_total") {
		t.Fatalf("expected metric output to include mqtttest counter, got: %s", body)
	}
}
