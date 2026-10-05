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
}

type HTTPIngestHandler struct {
	auth    *auth.Authenticator
	acker   *Acker
	router  ContractRouter
	logger  *slog.Logger
	timeout time.Duration
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
	return &HTTPIngestHandler{auth: opts.Authenticator, acker: NewAcker(opts.Publisher, opts.Timeout, new(Metrics)), router: opts.Router, logger: opts.Logger, timeout: opts.Timeout}, nil
}

func (h *HTTPIngestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, `{"error":"method_not_allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	deviceKey, stream, ok := parseHTTPIngestPath(r.URL.Path)
	if !ok {
		http.Error(w, `{"error":"invalid_path"}`, http.StatusBadRequest)
		return
	}
	if r.ContentLength > MaxHTTPPayloadBytes {
		http.Error(w, `{"error":"payload_too_large"}`, http.StatusRequestEntityTooLarge)
		return
	}
	secret := strings.TrimSpace(r.Header.Get("X-Device-Secret"))
	if secret == "" {
		http.Error(w, `{"error":"missing_device_secret"}`, http.StatusUnauthorized)
		return
	}
	result, err := h.auth.Authenticate(r.Context(), auth.Request{ClientID: deviceKey, Username: deviceKey, Password: secret, RemoteIP: httpRemoteIP(r)})
	if err != nil {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxHTTPPayloadBytes+1))
	if err != nil || len(body) == 0 || len(body) > MaxHTTPPayloadBytes || !json.Valid(body) {
		http.Error(w, `{"error":"invalid_json"}`, http.StatusBadRequest)
		return
	}
	subject, err := h.router.RouteHTTPProject(deviceKey, stream, result.ProjectID)
	if err != nil {
		http.Error(w, `{"error":"invalid_stream"}`, http.StatusBadRequest)
		return
	}
	traceID, err := envelope.NewTraceID()
	if err != nil {
		http.Error(w, `{"error":"trace_id_failed"}`, http.StatusInternalServerError)
		return
	}
	data, err := (envelope.Envelope{SchemaVersion: envelope.CurrentSchemaVersion, TraceID: traceID, ProjectID: result.ProjectID, DeviceKey: result.DeviceKey, DeviceID: result.DeviceID, DeviceTypeID: result.DeviceTypeID, Stream: stream, ReceivedAt: time.Now().UTC(), Payload: json.RawMessage(body)}).Encode()
	if err != nil {
		http.Error(w, `{"error":"envelope_failed"}`, http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	if err := h.acker.AwaitPersist(ctx, subject, data); err != nil {
		h.logger.Error("HTTP 设备上报未持久化", "device_key", deviceKey, "error", err)
		http.Error(w, `{"error":"publish_failed"}`, http.StatusServiceUnavailable)
		return
	}
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
