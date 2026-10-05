package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SNCIC/odoo20iot/internal/auth"
)

type httpIngestPublisher struct{ subject, payload string }

func (p *httpIngestPublisher) Publish(_ context.Context, subject string, payload []byte) error {
	p.subject, p.payload = subject, string(payload)
	return nil
}

func (p *httpIngestPublisher) Close() error { return nil }

func TestHTTPIngestAuthenticatesAndPublishesEnvelope(t *testing.T) {
	salt := []byte("0123456789abcdef")
	id := &auth.Identity{ProjectID: 42, DeviceID: 7, DeviceTypeID: 9, DeviceKey: "dtu-1", Mode: auth.ModePerDevice, Secret: auth.HashSecret("secret", auth.Params{Time: 1, Memory: 8 * 1024, Threads: 1, KeyLen: 32}, salt)}
	a := auth.NewAuthenticator(auth.DirectoryFunc(func(_ context.Context, key string) (*auth.Identity, error) {
		if key == id.DeviceKey {
			return id, nil
		}
		return nil, nil
	}), auth.Policy{MaxConcurrentVerify: 1, VerifyQueueTimeout: 1000000000}, nil)
	publisher := new(httpIngestPublisher)
	h, err := NewHTTPIngestHandler(HTTPIngestOptions{Authenticator: a, Publisher: publisher, Router: ContractRouter{Shards: 8}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/ingest/v1/devices/dtu-1/telemetry", strings.NewReader(`{"barcode":"690123","weight":1.2}`))
	req.Header.Set("X-Device-Secret", "secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.HasPrefix(publisher.subject, "iot.telemetry.42.shard.") {
		t.Fatalf("subject=%q", publisher.subject)
	}
	if !strings.Contains(publisher.payload, `"device_key":"dtu-1"`) || !strings.Contains(publisher.payload, `"barcode":"690123"`) {
		t.Fatalf("payload=%s", publisher.payload)
	}
}

func TestHTTPIngestRejectsBadSecretAndInvalidPayload(t *testing.T) {
	id := &auth.Identity{ProjectID: 1, DeviceKey: "scanner-1", Mode: auth.ModePerDevice, Secret: auth.HashSecret("secret", auth.Params{Time: 1, Memory: 8 * 1024, Threads: 1, KeyLen: 32}, []byte("0123456789abcdef"))}
	a := auth.NewAuthenticator(auth.DirectoryFunc(func(_ context.Context, _ string) (*auth.Identity, error) { return id, nil }), auth.DefaultPolicy(), nil)
	h, _ := NewHTTPIngestHandler(HTTPIngestOptions{Authenticator: a, Publisher: new(httpIngestPublisher), Router: ContractRouter{Shards: 8}})
	for _, tc := range []struct {
		name, path, secret, body string
		status                   int
	}{
		{"bad secret", "/ingest/v1/devices/scanner-1/telemetry", "bad", `{}`, http.StatusUnauthorized},
		{"invalid json", "/ingest/v1/devices/scanner-1/telemetry", "secret", `{`, http.StatusBadRequest},
		{"bad path", "/ingest/v1/devices/scanner-1/telemetry/extra", "secret", `{}`, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			req.Header.Set("X-Device-Secret", tc.secret)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", rec.Code, tc.status, rec.Body.String())
			}
		})
	}
}

func TestHTTPIngestAcceptsBearerAndContentType(t *testing.T) {
	id := &auth.Identity{ProjectID: 1, DeviceKey: "scanner-1", Mode: auth.ModePerDevice, Secret: auth.HashSecret("secret", auth.Params{Time: 1, Memory: 8 * 1024, Threads: 1, KeyLen: 32}, []byte("0123456789abcdef"))}
	a := auth.NewAuthenticator(auth.DirectoryFunc(func(_ context.Context, _ string) (*auth.Identity, error) { return id, nil }), auth.DefaultPolicy(), nil)
	metrics := new(Metrics)
	h, _ := NewHTTPIngestHandler(HTTPIngestOptions{Authenticator: a, Publisher: new(httpIngestPublisher), Router: ContractRouter{Shards: 8}, Metrics: metrics})
	req := httptest.NewRequest(http.MethodPost, "/ingest/v1/devices/scanner-1/telemetry", strings.NewReader(`{"barcode":"abc"}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || metrics.HTTPIngestAccepted.Load() != 1 {
		t.Fatalf("status=%d accepted=%d body=%s", rec.Code, metrics.HTTPIngestAccepted.Load(), rec.Body.String())
	}
}
