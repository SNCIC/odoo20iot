package querysvc

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/SNCIC/odoo20iot/internal/apiauth"
	"github.com/SNCIC/odoo20iot/internal/catalog"
	"github.com/SNCIC/odoo20iot/internal/cluster"
	"github.com/SNCIC/odoo20iot/internal/command"
	"github.com/SNCIC/odoo20iot/internal/metering"
	"github.com/SNCIC/odoo20iot/internal/ota"
	"github.com/SNCIC/odoo20iot/internal/quota"
	"github.com/SNCIC/odoo20iot/internal/shadow"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

func (s *Service) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", consoleHandler())

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": "alive"})
	})
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/metrics", s.handleMetrics)

	auth := apiauth.RequireAuth(apiauth.Options{
		Verifier: s.deps.Verifier,
		Metrics:  s.deps.AuthMetrics,
		Logger:   s.deps.Logger,
	})
	mux.Handle("/api/v1/devices", requireScope(auth, "device:read", http.HandlerFunc(s.handleDevices), s.deps.Meter))
	mux.Handle("/api/v1/series", requireScope(auth, "telemetry:read", http.HandlerFunc(s.handleSeries), s.deps.Meter))
	mux.Handle("/api/v1/series/multi", requireScope(auth, "telemetry:read", http.HandlerFunc(s.handleSeriesMulti), s.deps.Meter))
	mux.Handle("/api/v1/export", requireScope(auth, "telemetry:read", http.HandlerFunc(s.handleExport), s.deps.Meter))
	if s.deps.Latest != nil {
		mux.Handle("/api/v1/latest", requireScope(auth, "telemetry:read", http.HandlerFunc(s.handleLatest), s.deps.Meter))
	}
	if s.deps.Endpoints != nil {
		mux.Handle("/api/v1/notification-endpoints", auth(http.HandlerFunc(s.handleNotificationEndpoints)))
	}
	if s.deps.Quota != nil {
		mux.Handle("/api/v1/quota/policies", auth(http.HandlerFunc(s.handleQuotaPolicies)))
		mux.Handle("/api/v1/quota/policies/", auth(http.HandlerFunc(s.handleQuotaPolicies)))
	}
	if s.deps.Alarms != nil {
		mux.Handle("/api/v1/alarms/", auth(http.HandlerFunc(s.handleAlarmAction)))
	}
	if s.deps.Commands != nil {
		mux.Handle("/api/v1/commands", requireScope(auth, "command:write", http.HandlerFunc(s.handleCommands), s.deps.Meter))
		mux.Handle("/api/v1/commands/", auth(http.HandlerFunc(s.handleCommandStatus)))
	}
	if s.deps.Shadows != nil {
		mux.Handle("/api/v1/shadows/", auth(http.HandlerFunc(s.handleShadow)))
	}
	if s.deps.OTA != nil {
		mux.Handle("/api/v1/ota/firmwares", auth(http.HandlerFunc(s.handleOTAFirmwares)))
		mux.Handle("/api/v1/ota/tasks", auth(http.HandlerFunc(s.handleOTATasks)))
		mux.Handle("/api/v1/ota/tasks/", auth(http.HandlerFunc(s.handleOTATask)))
	}
	if s.deps.OTAArtifact != nil {
		mux.Handle("/api/v1/ota/artifacts", auth(http.HandlerFunc(s.handleOTAArtifactUpload)))
		if s.deps.OTASigner != nil && s.deps.OTADownloadSecret != "" && s.deps.OTADownloadBaseURL != "" {
			mux.Handle("/api/v1/ota/firmwares/", auth(http.HandlerFunc(s.handleOTAFirmwareAction)))
			mux.HandleFunc("/api/v1/ota/download/", s.handleOTADownload)
		}
	}

	return securityHeaders(mux)
}

func (s *Service) handleOTATaskAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !strings.HasSuffix(strings.Trim(r.URL.Path, "/"), "/start") {
		return
	}
	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	if !id.Dev && !id.HasScope("ota:write") {
		writeError(w, http.StatusForbidden, CodeForbidden, "缺少 ota:write 权限")
		return
	}
	if s.deps.OTARouter == nil || s.deps.OTASigner == nil || s.deps.OTADownloadSecret == "" || s.deps.OTADownloadBaseURL == "" {
		writeError(w, http.StatusServiceUnavailable, CodeUpstream, "OTA 下发依赖未配置")
		return
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/ota/tasks/"), "/"), "/")
	if len(parts) != 2 || parts[1] != "start" || !ota.ValidTaskID(parts[0]) {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "启动任务路径非法")
		return
	}
	task, err := s.deps.OTA.StartTask(r.Context(), id.ProjectID, parts[0])
	if err != nil {
		s.fail(w, "启动 OTA 任务", err)
		return
	}
	if task.Status == ota.TaskRunning {
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "task": task, "notified": 0, "idempotent": true})
		return
	}
	firmware, err := s.deps.OTA.GetFirmware(r.Context(), id.ProjectID, task.FirmwareID)
	if err != nil {
		s.fail(w, "读取 OTA 固件", err)
		return
	}
	devices, err := s.deps.OTA.ListTaskDevices(r.Context(), id.ProjectID, task.ID)
	if err != nil {
		s.fail(w, "读取 OTA 设备任务", err)
		return
	}
	devices, err = ota.NextBatchDevices(devices, task.Rollout)
	if err != nil {
		s.fail(w, "计算 OTA 灰度批次", err)
		return
	}
	sent := 0
	for _, device := range devices {
		if device.Status != ota.DevicePending {
			continue
		}
		expires := s.deps.Now().Add(time.Hour)
		objectKey := firmware.ObjectKey
		downloadURL := strings.TrimRight(s.deps.OTADownloadBaseURL, "/") + path.Join("/", strconv.FormatInt(id.ProjectID, 10), url.PathEscape(path.Base(objectKey)))
		manifest, signErr := s.deps.OTASigner.SignManifest(ota.Manifest{Version: firmware.Version, Filename: firmware.Filename, URL: downloadURL, SizeBytes: firmware.SizeBytes, SHA256: firmware.SHA256}, s.deps.Now(), time.Hour)
		if signErr != nil {
			s.fail(w, "签署 OTA 通知", signErr)
			return
		}
		parsed, parseErr := url.Parse(manifest.URL)
		if parseErr != nil {
			s.fail(w, "构造 OTA 下载地址", parseErr)
			return
		}
		query := parsed.Query()
		query.Set("expires", strconv.FormatInt(expires.Unix(), 10))
		query.Set("sig", ota.DownloadSignature(s.deps.OTADownloadSecret, id.ProjectID, objectKey, expires))
		parsed.RawQuery = query.Encode()
		notify, notifyErr := manifest.Notification(task.ID)
		if notifyErr != nil {
			s.fail(w, "构造 OTA 通知", notifyErr)
			return
		}
		notify.DownloadURL = parsed.String()
		payload, marshalErr := json.Marshal(notify)
		if marshalErr != nil {
			s.fail(w, "编码 OTA 通知", marshalErr)
			return
		}
		if routeErr := s.deps.OTARouter.RouteExternal(r.Context(), cluster.Envelope{Topic: "v1/devices/" + device.DeviceKey + "/ota/notify", Payload: payload, Qos: 1, DeviceKey: device.DeviceKey}); routeErr != nil {
			s.fail(w, "下发 OTA 通知", routeErr)
			return
		}
		if markErr := s.deps.OTA.MarkNotified(r.Context(), id.ProjectID, task.ID, device.DeviceKey); markErr != nil {
			s.fail(w, "更新 OTA 设备状态", markErr)
			return
		}
		sent++
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "task": task, "notified": sent})
}

func (s *Service) handleOTAFirmwareAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, CodeInvalidArgument, "仅支持 GET")
		return
	}
	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	if !id.Dev && !id.HasScope("ota:read") {
		writeError(w, http.StatusForbidden, CodeForbidden, "缺少 ota:read 权限")
		return
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/ota/firmwares/"), "/"), "/")
	if len(parts) != 2 || parts[1] != "manifest" {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "固件清单路径非法")
		return
	}
	firmwareID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || firmwareID <= 0 {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "firmware_id 非法")
		return
	}
	firmware, err := s.deps.OTA.GetFirmware(r.Context(), id.ProjectID, firmwareID)
	if err != nil {
		if errors.Is(err, ota.ErrFirmwareNotFound) {
			writeError(w, http.StatusNotFound, CodeNotFound, "OTA 固件不存在")
			return
		}
		s.fail(w, "读取 OTA 固件", err)
		return
	}
	ttl := time.Hour
	if raw := r.URL.Query().Get("ttl_seconds"); raw != "" {
		seconds, parseErr := strconv.Atoi(raw)
		if parseErr != nil {
			writeError(w, http.StatusBadRequest, CodeInvalidArgument, "ttl_seconds 非法")
			return
		}
		ttl = time.Duration(seconds) * time.Second
	}
	now := s.deps.Now()
	expires := now.Add(ttl)
	objectKey := firmware.ObjectKey
	signature := ota.DownloadSignature(s.deps.OTADownloadSecret, id.ProjectID, objectKey, expires)
	downloadURL := strings.TrimRight(s.deps.OTADownloadBaseURL, "/") + path.Join("/", strconv.FormatInt(id.ProjectID, 10), url.PathEscape(path.Base(objectKey)))
	manifest, err := s.deps.OTASigner.SignManifest(ota.Manifest{Version: firmware.Version, Filename: firmware.Filename, URL: downloadURL, SizeBytes: firmware.SizeBytes, SHA256: firmware.SHA256}, now, ttl)
	if err != nil {
		s.fail(w, "签署 OTA 固件清单", err)
		return
	}
	parsed, err := url.Parse(manifest.URL)
	if err != nil {
		s.fail(w, "构造 OTA 下载地址", err)
		return
	}
	query := parsed.Query()
	query.Set("expires", strconv.FormatInt(expires.Unix(), 10))
	query.Set("sig", signature)
	parsed.RawQuery = query.Encode()
	manifest.URL = parsed.String()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "manifest": manifest})
}

func (s *Service) handleOTADownload(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/ota/download/"), "/"), "/")
	if len(parts) != 2 {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "下载路径非法")
		return
	}
	projectID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || projectID <= 0 {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "project_id 非法")
		return
	}
	objectKey := parts[0] + "/" + parts[1]
	if err := ota.VerifyDownloadSignature(s.deps.OTADownloadSecret, projectID, objectKey, r.URL.Query().Get("expires"), r.URL.Query().Get("sig"), s.deps.Now()); err != nil {
		writeError(w, http.StatusForbidden, CodeForbidden, "下载地址无效或已过期")
		return
	}
	file, err := s.deps.OTAArtifact.Open(projectID, objectKey)
	if err != nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "固件制品不存在")
		return
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		writeError(w, http.StatusInternalServerError, CodeInternal, "读取固件制品失败")
		return
	}
	http.ServeContent(w, r, path.Base(objectKey), stat.ModTime(), file)
}

func (s *Service) handleOTAArtifactUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, CodeInvalidArgument, "仅支持 POST")
		return
	}
	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	if !id.Dev && !id.HasScope("ota:write") {
		writeError(w, http.StatusForbidden, CodeForbidden, "缺少 ota:write 权限")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, ota.DefaultMaxArtifactBytes+1<<20)
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "固件上传表单非法或超过大小限制")
		return
	}
	file, header, err := r.FormFile("firmware")
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "缺少 firmware 文件")
		return
	}
	defer file.Close()
	artifact, err := s.deps.OTAArtifact.PutContext(r.Context(), id.ProjectID, header.Filename, file)
	if err != nil {
		s.fail(w, "保存 OTA 固件制品", err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ok": true, "artifact": artifact, "signed_manifest_required": true})
}

func (s *Service) handleOTAFirmwares(w http.ResponseWriter, r *http.Request) {
	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	if r.Method == http.MethodGet {
		if !id.Dev && !id.HasScope("ota:read") {
			writeError(w, http.StatusForbidden, CodeForbidden, "缺少 ota:read 权限")
			return
		}
		items, err := s.deps.OTA.ListFirmwares(r.Context(), id.ProjectID)
		if err != nil {
			s.fail(w, "读取 OTA 固件", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "firmwares": items})
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		writeError(w, http.StatusMethodNotAllowed, CodeInvalidArgument, "仅支持 GET 或 POST")
		return
	}
	if !id.Dev && !id.HasScope("ota:write") {
		writeError(w, http.StatusForbidden, CodeForbidden, "缺少 ota:write 权限")
		return
	}
	var req struct {
		Version      string         `json:"version"`
		Filename     string         `json:"filename"`
		ObjectKey    string         `json:"object_key"`
		SizeBytes    int64          `json:"size_bytes"`
		SHA256       string         `json:"sha256"`
		Signature    string         `json:"signature"`
		SigningKeyID string         `json:"signing_key_id"`
		Metadata     map[string]any `json:"metadata"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "固件元数据 JSON 非法")
		return
	}
	firmware, err := s.deps.OTA.RegisterFirmware(r.Context(), ota.Firmware{ProjectID: id.ProjectID, Version: req.Version, Filename: req.Filename, ObjectKey: req.ObjectKey, SizeBytes: req.SizeBytes, SHA256: req.SHA256, Signature: req.Signature, SigningKeyID: req.SigningKeyID, Metadata: req.Metadata}, id.ActorID)
	if err != nil {
		s.fail(w, "登记 OTA 固件", err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ok": true, "firmware": firmware})
}

func (s *Service) handleOTATasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, CodeInvalidArgument, "仅支持 POST")
		return
	}
	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	if !id.Dev && !id.HasScope("ota:write") {
		writeError(w, http.StatusForbidden, CodeForbidden, "缺少 ota:write 权限")
		return
	}
	var req struct {
		FirmwareID int64       `json:"firmware_id"`
		DeviceKeys []string    `json:"device_keys"`
		Rollout    ota.Rollout `json:"rollout"`
		OfflineTTL int64       `json:"offline_ttl_seconds"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "OTA 任务 JSON 非法")
		return
	}
	if req.Rollout.Batches == nil {
		req.Rollout = ota.DefaultRollout()
	}
	if req.OfflineTTL == 0 {
		req.OfflineTTL = int64(ota.DefaultOfflineTTL / time.Second)
	}
	task, err := s.deps.OTA.CreateTask(r.Context(), id.ProjectID, req.FirmwareID, req.DeviceKeys, req.Rollout, time.Duration(req.OfflineTTL)*time.Second, id.ActorID)
	if err != nil {
		s.fail(w, "创建 OTA 任务", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "task": task})
}

func (s *Service) handleOTATask(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(strings.Trim(r.URL.Path, "/"), "/start") {
		s.handleOTATaskAction(w, r)
		return
	}
	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	path := strings.TrimPrefix(strings.Trim(r.URL.Path, "/"), "api/v1/ota/tasks/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" || len(parts) > 2 || (len(parts) == 2 && parts[1] != "devices") {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "task_id 路径非法")
		return
	}
	if !ota.ValidTaskID(parts[0]) {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "task_id 格式非法")
		return
	}
	if !id.Dev && !id.HasScope("ota:read") {
		writeError(w, http.StatusForbidden, CodeForbidden, "缺少 ota:read 权限")
		return
	}
	task, err := s.deps.OTA.GetTask(r.Context(), id.ProjectID, parts[0])
	if err != nil {
		if errors.Is(err, ota.ErrTaskNotFound) {
			writeError(w, http.StatusNotFound, CodeNotFound, "OTA 任务不存在")
			return
		}
		s.fail(w, "读取 OTA 任务", err)
		return
	}
	if len(parts) == 2 {
		devices, err := s.deps.OTA.ListTaskDevices(r.Context(), id.ProjectID, parts[0])
		if err != nil {
			s.fail(w, "读取 OTA 设备任务", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "task": task, "devices": devices})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "task": task})
}

func (s *Service) handleShadow(w http.ResponseWriter, r *http.Request) {
	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/shadows/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" || strings.ContainsAny(parts[0], "/+#") {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "device_key 非法")
		return
	}
	deviceKey := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		if !id.Dev && !id.HasScope("shadow:read") {
			writeError(w, http.StatusForbidden, CodeForbidden, "缺少 shadow:read 权限")
			return
		}
		snapshot, err := s.deps.Shadows.Get(r.Context(), id.ProjectID, deviceKey)
		if err != nil {
			s.fail(w, "读取设备影子", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "shadow": snapshot})
		return
	}
	if len(parts) != 2 || parts[1] != "desired" || r.Method != http.MethodPatch {
		w.Header().Set("Allow", "GET, PATCH")
		writeError(w, http.StatusMethodNotAllowed, CodeInvalidArgument, "影子仅支持 GET 或 PATCH desired")
		return
	}
	if !id.Dev && !id.HasScope("shadow:write") {
		writeError(w, http.StatusForbidden, CodeForbidden, "缺少 shadow:write 权限")
		return
	}
	var req struct {
		Patch           map[string]any `json:"patch"`
		ExpectedVersion *int64         `json:"expected_version"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, shadow.MaxStateBytes)).Decode(&req); err != nil || len(req.Patch) == 0 {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "patch 非法或为空")
		return
	}
	snapshot, err := s.deps.Shadows.UpdateDesired(r.Context(), id.ProjectID, deviceKey, req.Patch, req.ExpectedVersion)
	if err != nil {
		if errors.Is(err, shadow.ErrVersionConflict) {
			writeError(w, http.StatusConflict, CodeConflict, "影子版本冲突")
			return
		}
		s.fail(w, "更新设备影子", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "shadow": snapshot})
}

func (s *Service) handleCommandStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, CodeInvalidArgument, "仅支持 GET")
		return
	}
	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	if !id.Dev && !id.HasScope("command:read") {
		writeError(w, http.StatusForbidden, CodeForbidden, "缺少 command:read 权限")
		return
	}
	commandID := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/commands/"), "/")
	if commandID == "" || strings.Contains(commandID, "/") {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "command_id 非法")
		return
	}
	record, err := s.deps.Commands.Get(r.Context(), id.ProjectID, commandID)
	if err != nil {
		s.fail(w, "查询命令状态", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "command": record})
}

func (s *Service) handleCommands(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, CodeInvalidArgument, "仅支持 POST")
		return
	}
	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	var req struct {
		DeviceKey     string          `json:"device_key"`
		Command       string          `json:"command"`
		Payload       json.RawMessage `json:"payload"`
		CorrelationID string          `json:"correlation_id"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "请求体非法")
		return
	}
	if strings.TrimSpace(req.DeviceKey) == "" || strings.TrimSpace(req.Command) == "" {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "device_key 和 command 必填")
		return
	}
	record, err := s.deps.Commands.Issue(r.Context(), command.Record{ProjectID: id.ProjectID, CommandID: req.CorrelationID, DeviceKey: strings.TrimSpace(req.DeviceKey), CommandKey: strings.TrimSpace(req.Command), CorrelationID: strings.TrimSpace(req.CorrelationID), Payload: req.Payload})
	if err != nil {
		s.fail(w, "下发命令", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "command": record})
}

func (s *Service) handleQuotaPolicies(w http.ResponseWriter, r *http.Request) {
	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	if r.Method == http.MethodGet {
		if !id.Dev && !id.HasScope("quota:read") {
			writeError(w, http.StatusForbidden, CodeForbidden, "缺少 quota:read 权限")
			return
		}
		s.recordAPICall(id.ProjectID)
		items, err := s.deps.Quota.Policies(r.Context(), id.ProjectID)
		if err != nil {
			s.fail(w, "读取配额策略", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "policies": items})
		return
	}
	if !id.Dev && !id.HasScope("quota:write") {
		writeError(w, http.StatusForbidden, CodeForbidden, "缺少 quota:write 权限")
		return
	}
	s.recordAPICall(id.ProjectID)
	metric := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/quota/policies/"), "/")
	if r.Method == http.MethodDelete {
		if _, ok := quota.AllowedMetrics[metric]; !ok {
			writeError(w, http.StatusBadRequest, CodeInvalidArgument, "metric 非法")
			return
		}
		if err := s.deps.Quota.DeletePolicy(r.Context(), id.ProjectID, metric, id.ActorID); err != nil {
			s.fail(w, "删除配额策略", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		w.Header().Set("Allow", "GET, POST, PUT, DELETE")
		writeError(w, http.StatusMethodNotAllowed, CodeInvalidArgument, "不支持的 HTTP 方法")
		return
	}
	var req struct {
		Metric       string `json:"metric"`
		SoftLimit    int64  `json:"soft_limit"`
		WarningLimit int64  `json:"warning_limit"`
		HardLimit    int64  `json:"hard_limit"`
		Window       string `json:"window"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "请求体非法")
		return
	}
	if r.Method == http.MethodPut && metric != "" && metric != req.Metric {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "路径 metric 与请求体不一致")
		return
	}
	p := quota.NormalizePolicy(quota.Policy{ProjectID: id.ProjectID, Metric: req.Metric, SoftLimit: req.SoftLimit, WarningLimit: req.WarningLimit, HardLimit: req.HardLimit, Window: req.Window})
	if err := s.deps.Quota.SetPolicy(r.Context(), p, id.ActorID); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "policy": p})
}

func requireScope(auth func(http.Handler) http.Handler, scope string, next http.Handler, meter Meter) http.Handler {
	return auth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := apiauth.IdentityFrom(r.Context())
		if !ok {
			writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
			return
		}
		if !id.Dev && !id.HasScope(scope) {
			writeError(w, http.StatusForbidden, CodeForbidden, "缺少 "+scope+" 权限")
			return
		}
		if meter != nil {
			meter.Add(id.ProjectID, metering.MetricAPICalls, 1)
		}
		next.ServeHTTP(w, r)
	}))
}

func (s *Service) handleAlarmAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !strings.HasSuffix(strings.TrimSuffix(r.URL.Path, "/"), "/ack") {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, CodeInvalidArgument, "仅支持 POST /api/v1/alarms/{id}/ack")
		return
	}
	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	if !id.Dev && !id.HasScope("alarm:write") {
		writeError(w, http.StatusForbidden, CodeForbidden, "缺少 alarm:write 权限")
		return
	}
	s.recordAPICall(id.ProjectID)
	path := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/alarms/"), "/ack")
	path = strings.TrimSuffix(path, "/")
	if path == "" || strings.Contains(path, "/") {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "告警 ID 非法")
		return
	}
	if err := s.deps.Alarms.Acknowledge(r.Context(), id.ProjectID, path, id.ActorID, s.deps.Now()); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, CodeNotFound, "告警不存在、已恢复或不属于本租户")
			return
		}
		s.fail(w, "确认告警", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "alarm_id": path, "acknowledged": true})
}

func (s *Service) handleSeriesMulti(w http.ResponseWriter, r *http.Request) {
	metrics := splitMetrics(r.URL.Query().Get("metrics"))
	if len(metrics) == 0 || len(metrics) > tsdb.MaxProjectedMetrics {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, fmt.Sprintf("metrics 必须包含 1-%d 个指标", tsdb.MaxProjectedMetrics))
		return
	}
	result := make(map[string]json.RawMessage, len(metrics))
	for _, metric := range metrics {
		q := r.URL.Query()
		q.Set("metric", metric)
		q.Del("metrics")
		req := r.Clone(r.Context())
		req.URL.RawQuery = q.Encode()
		rec := httptest.NewRecorder()
		s.handleSeries(rec, req)
		if rec.Code >= http.StatusBadRequest {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(rec.Code)
			_, _ = w.Write(rec.Body.Bytes())
			return
		}
		result[metric] = json.RawMessage(rec.Body.Bytes())
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "series": result})
}

func (s *Service) handleExport(w http.ResponseWriter, r *http.Request) {
	if strings.TrimSpace(r.URL.Query().Get("metric")) == "" {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "metric 必填")
		return
	}
	rec := httptest.NewRecorder()
	s.handleSeries(rec, r)
	if rec.Code >= http.StatusBadRequest {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
		return
	}
	var body seriesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		writeError(w, http.StatusInternalServerError, CodeInternal, "导出结果解析失败")
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="iot-series.csv"`)
	w.WriteHeader(http.StatusOK)
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{"ts", "device_id", "value", "avg", "max", "count"})
	for _, p := range body.Points {
		value := ""
		if p.Value != nil {
			value = fmt.Sprintf("%v", *p.Value)
		}
		_ = cw.Write([]string{p.TS.UTC().Format(time.RFC3339Nano), strconv.FormatInt(p.DeviceID, 10), value, "", "", ""})
	}
	for _, b := range body.Buckets {
		avg, max := "", ""
		if b.Avg != nil {
			avg = fmt.Sprintf("%v", *b.Avg)
		}
		if b.Max != nil {
			max = fmt.Sprintf("%v", *b.Max)
		}
		_ = cw.Write([]string{b.Bucket.UTC().Format(time.RFC3339Nano), strconv.FormatInt(b.DeviceID, 10), "", avg, max, strconv.FormatInt(b.Count, 10)})
	}
	cw.Flush()
}

func splitMetrics(raw string) []string {
	seen := map[string]bool{}
	var out []string
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item != "" && !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:")
		if r.TLS != nil {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Service) handleNotificationEndpoints(w http.ResponseWriter, r *http.Request) {
	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	requiredScope := "notification:read"
	if r.Method != http.MethodGet {
		requiredScope = "notification:write"
	}
	if !id.Dev && !id.HasScope(requiredScope) {
		writeError(w, http.StatusForbidden, CodeForbidden, "缺少 "+requiredScope+" 权限")
		return
	}
	s.recordAPICall(id.ProjectID)
	if r.Method == http.MethodGet {
		items, err := s.deps.Endpoints.List(r.Context(), id.ProjectID)
		if err != nil {
			s.fail(w, "读取通知端点", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "endpoints": items})
		return
	}
	if r.Method == http.MethodDelete {
		endpointID, err := strconv.ParseInt(strings.TrimSpace(r.URL.Query().Get("id")), 10, 64)
		if err != nil || endpointID <= 0 {
			writeError(w, http.StatusBadRequest, CodeInvalidArgument, "id 必须为正整数")
			return
		}
		if err := s.deps.Endpoints.Delete(r.Context(), id.ProjectID, endpointID); err != nil {
			s.fail(w, "删除通知端点", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST, DELETE")
		writeError(w, http.StatusMethodNotAllowed, CodeInvalidArgument, "不支持的 HTTP 方法")
		return
	}
	var req struct{ Name, Channel, Target string }
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "请求体非法")
		return
	}
	if req.Channel != "webhook" && req.Channel != "email" && req.Channel != "sms" {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "channel 必须是 webhook、email 或 sms")
		return
	}
	e, err := s.deps.Endpoints.Create(r.Context(), id.ProjectID, strings.TrimSpace(req.Name), req.Channel, strings.TrimSpace(req.Target))
	if err != nil {
		s.fail(w, "创建通知端点", err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ok": true, "endpoint": e})
}

func (s *Service) recordAPICall(projectID int64) {
	if s.deps.Meter != nil {
		s.deps.Meter.Add(projectID, metering.MetricAPICalls, 1)
	}
}

func (s *Service) handleLatest(w http.ResponseWriter, r *http.Request) {
	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	q := r.URL.Query()
	if q.Has("project_id") {
		writeError(w, http.StatusBadRequest, CodeTenantNotAllowed, "租户只能来自令牌，不能作为查询参数")
		return
	}
	deviceIDs, err := parseDeviceIDs(q.Get("device_ids"))
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, err.Error())
		return
	}
	if len(deviceIDs) > 50 {
		writeError(w, http.StatusUnprocessableEntity, CodeUnprocessable, "device_ids 最多 50 台")
		return
	}
	owned, err := s.deps.Catalog.DeviceIDsOwned(r.Context(), id.ProjectID, deviceIDs)
	if err != nil {
		s.fail(w, "校验设备归属", err)
		return
	}
	items := make([]latestDTO, 0, len(deviceIDs))
	for _, deviceID := range deviceIDs {
		if !owned[deviceID] {
			writeError(w, http.StatusForbidden, CodeForbidden, "设备不存在或不属于本租户")
			return
		}
		snapshot, err := s.deps.Latest.Get(r.Context(), id.ProjectID, deviceID)
		if errors.Is(err, redis.Nil) {
			items = append(items, latestDTO{DeviceID: deviceID})
			continue
		}
		if err != nil {
			s.fail(w, "读取最新值", err)
			return
		}
		ts := snapshot.TS
		items = append(items, latestDTO{DeviceID: deviceID, Available: true, TS: &ts, Values: snapshot.Values})
	}
	writeJSON(w, http.StatusOK, latestResponse{OK: true, Latest: items})
}

// handleReadyz 真打 PG（并把迁移状态查清楚）与 GreptimeDB。
//
// 与 /healthz 分开：PG 抖一下就把进程重启一遍，解决不了 PG 的问题。
func (s *Service) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	if s.deps.Health.PingPG != nil {
		if err := s.deps.Health.PingPG(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok": false, "code": "PG_UNREACHABLE", "error": err.Error(),
			})
			return
		}
	}
	if s.deps.Health.PendingMigrations != nil {
		pending, err := s.deps.Health.PendingMigrations(ctx)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok": false, "code": "MIGRATION_CHECK_FAILED", "error": err.Error(),
			})
			return
		}
		if len(pending) > 0 {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok": false, "code": "MIGRATION_PENDING", "pending": pending,
			})
			return
		}
	}
	if s.deps.Health.PingTSDB != nil {
		if err := s.deps.Health.PingTSDB(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok": false, "code": "GREPTIMEDB_UNREACHABLE", "error": err.Error(),
			})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "pg": "up-to-date", "greptimedb": "reachable"})
}

func (s *Service) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	s.deps.Metrics.Render(w)
	if s.deps.AuthMetrics != nil {
		if n := s.deps.AuthMetrics.AuthFailures.Load(); n > 0 {
			_, _ = w.Write([]byte("querysvc_auth_failures_total " + strconv.FormatInt(n, 10) + "\n"))
		}
		if n := s.deps.AuthMetrics.AuthUnavailable.Load(); n > 0 {
			_, _ = w.Write([]byte("querysvc_auth_unavailable_total " + strconv.FormatInt(n, 10) + "\n"))
		}
	}
}

// handleDevices 列出本租户设备。
func (s *Service) handleDevices(w http.ResponseWriter, r *http.Request) {
	s.deps.Metrics.Requests.Add(1)

	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	q := r.URL.Query()
	if q.Has("project_id") {
		writeError(w, http.StatusBadRequest, CodeTenantNotAllowed, "租户只能来自令牌，不能作为查询参数")
		return
	}

	f := catalog.DeviceFilter{ProjectID: id.ProjectID, Status: q.Get("status"), Query: q.Get("q")}
	if v := q.Get("device_type_id"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, CodeInvalidArgument, "device_type_id 必须是正整数")
			return
		}
		f.DeviceTypeID = n
	}
	if v := q.Get("cursor"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, CodeInvalidArgument, "cursor 非法")
			return
		}
		f.AfterID = n
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, CodeInvalidArgument, "limit 必须是正整数")
			return
		}
		if n > s.cfg.MaxDeviceLimit {
			writeError(w, http.StatusBadRequest, CodeInvalidArgument,
				"limit 超过上限 "+strconv.Itoa(s.cfg.MaxDeviceLimit))
			return
		}
		f.Limit = n
	} else {
		f.Limit = s.cfg.DefaultDeviceLimit
	}

	page, err := s.deps.Catalog.ListDevices(r.Context(), f)
	if err != nil {
		s.fail(w, "列设备", err)
		return
	}

	resp := devicesResponse{OK: true, Devices: make([]deviceDTO, 0, len(page.Devices))}
	for _, d := range page.Devices {
		resp.Devices = append(resp.Devices, toDeviceDTO(d))
	}
	if page.NextAfterID > 0 {
		resp.NextCursor = strconv.FormatInt(page.NextAfterID, 10)
	}
	s.deps.Metrics.DevicesOK.Add(1)
	writeJSON(w, http.StatusOK, resp)
}

// handleSeries 是曲线/聚合查询。
func (s *Service) handleSeries(w http.ResponseWriter, r *http.Request) {
	s.deps.Metrics.Requests.Add(1)

	id, ok := apiauth.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, CodeUnauthenticated, "缺少身份")
		return
	}
	q := r.URL.Query()
	if q.Has("project_id") {
		// 显式拒绝而不是静默忽略：让「租户只来自令牌」这条不变量可被测试钉住。
		writeError(w, http.StatusBadRequest, CodeTenantNotAllowed, "租户只能来自令牌，不能作为查询参数")
		return
	}

	deviceIDs, err := parseDeviceIDs(q.Get("device_ids"))
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, err.Error())
		return
	}
	metric := strings.TrimSpace(q.Get("metric"))
	if metric == "" {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, "metric 必填")
		return
	}
	since, err := parseTimeParam("since", q.Get("since"))
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, err.Error())
		return
	}
	until, err := parseTimeParam("until", q.Get("until"))
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, err.Error())
		return
	}
	bucket, err := parseDurationParam("bucket", q.Get("bucket"))
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, err.Error())
		return
	}
	limit, err := parseLimitParam(q.Get("limit"))
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidArgument, err.Error())
		return
	}

	sq := tsdb.SeriesQuery{
		ProjectID: id.ProjectID,
		DeviceIDs: deviceIDs,
		Since:     since,
		Until:     until,
		Metric:    metric,
		Limit:     limit,
		Bucket:    bucket,
	}
	// 先本地 Normalize：形状非法就别去打库、也别去打归属校验。
	nq, err := sq.Normalize(s.deps.Now())
	if err != nil {
		s.fail(w, "校验查询参数", err)
		return
	}

	// 归属校验：不属于本租户的设备与「不存在」返回**同一个** 403，不泄露存在性。
	owned, err := s.deps.Catalog.DeviceIDsOwned(r.Context(), id.ProjectID, nq.DeviceIDs)
	if err != nil {
		s.fail(w, "校验设备归属", err)
		return
	}
	for _, did := range nq.DeviceIDs {
		if !owned[did] {
			writeError(w, http.StatusForbidden, CodeForbidden, "设备不存在或不属于本租户")
			return
		}
	}

	// 每租户并发保护（02 §4.3）。
	release, err := s.limiter.Acquire(r.Context(), id.ProjectID)
	if err != nil {
		s.deps.Metrics.Rejected.Add(1)
		w.Header().Set("Retry-After", "1")
		switch {
		case errors.Is(err, ErrRateLimited):
			writeError(w, http.StatusTooManyRequests, CodeRateLimited, "该租户请求过于频繁，请稍后重试")
		case errors.Is(err, ErrQueueFull):
			writeError(w, http.StatusServiceUnavailable, CodeQueryBusy, "该租户查询排队已满，请稍后重试")
		case errors.Is(err, ErrQueueTimeout):
			writeError(w, http.StatusServiceUnavailable, CodeQueryBusy, "该租户查询排队超时，请稍后重试")
		default:
			s.fail(w, "排队", err)
		}
		return
	}
	defer release()
	s.deps.Metrics.InFlight.Add(1)
	defer s.deps.Metrics.InFlight.Add(-1)

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()

	start := s.deps.Now()
	res, err := s.deps.Reader.QuerySeries(ctx, s.cfg.Plan, nq)
	elapsed := s.deps.Now().Sub(start)
	if err != nil {
		s.fail(w, "查询时序", err)
		return
	}

	// 慢查询：先记指标 + WARN。自动降级到预聚合表**未实现**（见 09 §5.1 遗留）——
	// 短跨度强制降级会丢原始点，需要按租户迟滞，不是本批的事。
	if elapsed > s.cfg.SlowQueryThreshold {
		s.deps.Metrics.Slow.Add(1)
		s.deps.Logger.Warn("慢查询",
			"project_id", id.ProjectID, "devices", len(nq.DeviceIDs), "metric", nq.Metric,
			"source", string(res.Source), "granularity", string(res.Granularity),
			"elapsed_ms", elapsed.Milliseconds())
	}
	s.recordSource(res)
	s.deps.Metrics.SeriesOK.Add(1)
	writeJSON(w, http.StatusOK, toSeriesResponse(res))
}

func (s *Service) recordSource(res tsdb.SeriesResult) {
	switch res.Source {
	case tsdb.SourceRollup1m:
		s.deps.Metrics.Source1m.Add(1)
	case tsdb.SourceRollup1h:
		s.deps.Metrics.Source1h.Add(1)
	default:
		s.deps.Metrics.SourceRaw.Add(1)
	}
	if res.CapHit {
		s.deps.Metrics.CapHit.Add(1)
	}
}

// fail 把内部错误映射成稳定状态码。
func (s *Service) fail(w http.ResponseWriter, what string, err error) {
	var pe *tsdb.PolicyError
	if errors.As(err, &pe) {
		if pe.Rule == "project_id" {
			// 只可能来自我们自己的装配错误，不是客户端问题。
			s.deps.Metrics.Errors.Add(1)
			s.deps.Logger.Error("查询保护拒绝：租户缺失（装配错误）", "rule", pe.Rule, "error", pe)
			writeError(w, http.StatusInternalServerError, CodeInternal, "内部错误")
			return
		}
		s.deps.Metrics.Errors.Add(1)
		writeJSON(w, http.StatusUnprocessableEntity, apiError{
			OK: false, Code: CodeUnprocessable, Message: pe.Msg,
			Rule: pe.Rule, ExportRequired: pe.Rule == "lookback",
		})
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		s.deps.Metrics.TimedOut.Add(1)
		writeError(w, http.StatusGatewayTimeout, CodeQueryTimeout, "查询超时")
		return
	}
	s.deps.Metrics.Errors.Add(1)
	s.deps.Logger.Error(what, "error", err)
	writeError(w, http.StatusInternalServerError, CodeInternal, "内部错误")
}

func toSeriesResponse(res tsdb.SeriesResult) seriesResponse {
	out := seriesResponse{
		OK:          true,
		Granularity: string(res.Granularity),
		Source:      string(res.Source),
		CapHit:      res.CapHit,
	}
	if res.Bucket > 0 {
		out.Bucket = res.Bucket.String()
	}
	if len(res.Points) > 0 {
		out.Points = make([]pointDTO, 0, len(res.Points))
		for _, p := range res.Points {
			dto := pointDTO{TS: p.TS.UTC(), DeviceID: p.DeviceID}
			if !p.Null {
				v := p.Value
				dto.Value = &v
			}
			out.Points = append(out.Points, dto)
		}
	}
	if len(res.Buckets) > 0 {
		out.Buckets = make([]bucketDTO, 0, len(res.Buckets))
		for _, b := range res.Buckets {
			dto := bucketDTO{Bucket: b.Bucket.UTC(), DeviceID: b.DeviceID, Count: b.Count}
			avg, max := b.Avg, b.Max
			dto.Avg, dto.Max = &avg, &max
			out.Buckets = append(out.Buckets, dto)
		}
	}
	return out
}

func toDeviceDTO(d catalog.Device) deviceDTO {
	return deviceDTO{
		ID: d.ID, DeviceKey: d.DeviceKey, Name: d.Name, DeviceTypeID: d.DeviceTypeID,
		Status: d.Status, AuthMode: d.AuthMode, Online: d.Online, LastSeenAt: d.LastSeenAt,
		Tags: d.Tags, Version: d.Version, CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt,
	}
}

func parseDeviceIDs(raw string) ([]int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("device_ids 必填（逗号分隔）")
	}
	seen := map[int64]bool{}
	var out []int64
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.ParseInt(part, 10, 64)
		if err != nil || n <= 0 {
			return nil, errors.New("device_ids 必须是正整数，用逗号分隔")
		}
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("device_ids 不能为空")
	}
	return out, nil
}

func parseTimeParam(name, raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts.UTC(), nil
		}
	}
	return time.Time{}, errors.New(name + " 必须是 RFC3339 时间（如 2026-10-01T00:00:00Z）")
}

func parseDurationParam(name, raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, errors.New(name + " 必须是正的时间长度（如 5m）")
	}
	return d, nil
}

func parseLimitParam(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, errors.New("limit 必须是正整数")
	}
	return n, nil
}
