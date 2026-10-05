package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/auth"
	"github.com/SNCIC/odoo20iot/internal/envelope"
)

const MaxHTTPPayloadBytes = 32 << 10

type HTTPIngestOptions struct {
	Authenticator *auth.Authenticator
	Publisher     Publisher
	Router        ContractRouter
	Logger        *slog.Logger
	Timeout       time.Duration
	Metrics       *Metrics
}

type HTTPIngestHandler struct {
	auth    *auth.Authenticator
	acker   *Acker
	router  ContractRouter
	logger  *slog.Logger
	timeout time.Duration
	metrics *Metrics
}

func NewHTTPIngestHandler(opts HTTPIngestOptions) (*HTTPIngestHandler, error) {
	if opts.Authenticator == nil || opts.Publisher == nil {
		return nil, fmt.Errorf("HTTP 接入依赖认证器和发布器")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.Metrics == nil {
		opts.Metrics = new(Metrics)
	}
	return &HTTPIngestHandler{auth: opts.Authenticator, acker: NewAcker(opts.Publisher, opts.Timeout, opts.Metrics), router: opts.Router, logger: opts.Logger, timeout: opts.Timeout, metrics: opts.Metrics}, nil
}

func (h *HTTPIngestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.metrics.HTTPIngestTotal.Add(1)
	reject := func(status int, body string) { h.metrics.HTTPIngestRejected.Add(1); http.Error(w, body, status) }
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		reject(http.StatusMethodNotAllowed, `{"error":"method_not_allowed"}`)
		return
	}
	deviceKey, stream, ok := parseHTTPIngestPath(r.URL.Path)
	if !ok {
		reject(http.StatusBadRequest, `{"error":"invalid_path"}`)
		return
	}
	if r.ContentLength > MaxHTTPPayloadBytes {
		reject(http.StatusRequestEntityTooLarge, `{"error":"payload_too_large"}`)
		return
	}
	if contentType := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0])); contentType != "" && contentType != "application/json" {
		reject(http.StatusUnsupportedMediaType, `{"error":"content_type_must_be_application_json"}`)
		return
	}
	secret := strings.TrimSpace(r.Header.Get("X-Device-Secret"))
	if secret == "" {
		const prefix = "Bearer "
		authz := strings.TrimSpace(r.Header.Get("Authorization"))
		if strings.HasPrefix(authz, prefix) {
			secret = strings.TrimSpace(strings.TrimPrefix(authz, prefix))
		}
	}
	if secret == "" {
		reject(http.StatusUnauthorized, `{"error":"missing_device_secret"}`)
		return
	}
	result, err := h.auth.Authenticate(r.Context(), auth.Request{ClientID: deviceKey, Username: deviceKey, Password: secret, RemoteIP: httpRemoteIP(r)})
	if err != nil {
		reject(http.StatusUnauthorized, `{"error":"unauthorized"}`)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxHTTPPayloadBytes+1))
	if err != nil || len(body) == 0 || len(body) > MaxHTTPPayloadBytes || !json.Valid(body) {
		reject(http.StatusBadRequest, `{"error":"invalid_json"}`)
		return
	}
	subject, err := h.router.RouteHTTPProject(deviceKey, stream, result.ProjectID)
	if err != nil {
		reject(http.StatusBadRequest, `{"error":"invalid_stream"}`)
		return
	}
	traceID, err := envelope.NewTraceID()
	if err != nil {
		reject(http.StatusInternalServerError, `{"error":"trace_id_failed"}`)
		return
	}
	data, err := (envelope.Envelope{SchemaVersion: envelope.CurrentSchemaVersion, TraceID: traceID, ProjectID: result.ProjectID, DeviceKey: result.DeviceKey, DeviceID: result.DeviceID, DeviceTypeID: result.DeviceTypeID, Stream: stream, ReceivedAt: time.Now().UTC(), Payload: json.RawMessage(body)}).Encode()
	if err != nil {
		reject(http.StatusInternalServerError, `{"error":"envelope_failed"}`)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	if err := h.acker.AwaitPersist(ctx, subject, data); err != nil {
		h.metrics.HTTPIngestPublishErrors.Add(1)
		h.logger.Error("HTTP 设备上报未持久化", "device_key", deviceKey, "error", err)
		reject(http.StatusServiceUnavailable, `{"error":"publish_failed"}`)
		return
	}
	h.metrics.HTTPIngestAccepted.Add(1)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"ok":true,"trace_id":"` + traceID + `"}`))
}

func parseHTTPIngestPath(path string) (string, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 5 || parts[0] != "ingest" || parts[1] != "v1" || parts[2] != "devices" || parts[3] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[3], parts[4], true
}

func httpRemoteIP(r *http.Request) string {
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i >= 0 {
		return strings.Trim(host[:i], "[]")
	}
	return host
}
