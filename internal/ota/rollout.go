package ota

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/cluster"
)

const (
	rolloutTaskLease = 2 * time.Minute
	maxDispatchBatch = 100
)

type rolloutStore interface {
	ClaimRunningTasks(context.Context, int64, int, time.Duration) ([]Task, error)
	ListTaskDevices(context.Context, int64, string) ([]TaskDevice, error)
	GetFirmware(context.Context, int64, int64) (Firmware, error)
	MarkNotified(context.Context, int64, string, string) error
	BeginDispatch(context.Context, int64, string, string) error
	ResetDispatch(context.Context, int64, string, string) error
	SetBatchState(context.Context, int64, string, int, int, TaskStatus) error
	ReleaseDispatchLease(context.Context, int64, string) error
}

type rolloutRouter interface {
	RouteExternal(context.Context, cluster.Envelope) error
}

type rolloutSigner interface {
	SignManifest(Manifest, time.Time, time.Duration) (Manifest, error)
}

type RolloutWorker struct {
	store   rolloutStore
	router  rolloutRouter
	signer  rolloutSigner
	secret  string
	baseURL string
	now     func() time.Time
}

func NewRolloutWorker(store rolloutStore, router rolloutRouter, signer rolloutSigner, secret, baseURL string) (*RolloutWorker, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if store == nil || router == nil || signer == nil || strings.TrimSpace(secret) == "" || err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, fmt.Errorf("ota: 灰度 worker 依赖不完整，且下载基址必须为 HTTPS")
	}
	return &RolloutWorker{store: store, router: router, signer: signer, secret: secret, baseURL: strings.TrimRight(baseURL, "/"), now: time.Now}, nil
}

func RolloutCohort(devices []TaskDevice, rollout Rollout, batchIndex int) ([]TaskDevice, error) {
	if err := rollout.Validate(); err != nil {
		return nil, err
	}
	if batchIndex < 0 || batchIndex >= len(rollout.Batches) {
		return nil, fmt.Errorf("ota: 灰度批次索引越界")
	}
	previous := 0
	if batchIndex > 0 {
		previous = rollout.Batches[batchIndex-1]
	}
	start := (len(devices)*previous + 99) / 100
	end := (len(devices)*rollout.Batches[batchIndex] + 99) / 100
	if start > len(devices) {
		start = len(devices)
	}
	if end > len(devices) {
		end = len(devices)
	}
	return append([]TaskDevice(nil), devices[start:end]...), nil
}

func (w *RolloutWorker) RunProject(ctx context.Context, projectID int64) error {
	tasks, err := w.store.ClaimRunningTasks(ctx, projectID, 20, rolloutTaskLease)
	if err != nil {
		return err
	}
	var firstErr error
	for _, task := range tasks {
		if err := w.processTask(ctx, task); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (w *RolloutWorker) processTask(ctx context.Context, task Task) (err error) {
	defer func() {
		if err != nil {
			_ = w.store.ReleaseDispatchLease(context.WithoutCancel(ctx), task.ProjectID, task.ID)
		}
	}()
	devices, err := w.store.ListTaskDevices(ctx, task.ProjectID, task.ID)
	if err != nil {
		return err
	}
	firmware, err := w.store.GetFirmware(ctx, task.ProjectID, task.FirmwareID)
	if err != nil {
		return err
	}
	batchIndex := task.BatchIndex
	for batchIndex < len(task.Rollout.Batches) {
		cohort, cohortErr := RolloutCohort(devices, task.Rollout, batchIndex)
		if cohortErr != nil {
			return cohortErr
		}
		pending := make([]TaskDevice, 0)
		ready, succeeded := true, 0
		for _, device := range cohort {
			switch device.Status {
			case DevicePending, DeviceDispatching:
				ready = false
				pending = append(pending, device)
			case DeviceSucceeded:
				succeeded++
			case DeviceFailed, DeviceExpired, DeviceRolledBack:
			default:
				ready = false
			}
		}
		if len(pending) > 0 {
			limit := len(pending)
			if limit > maxDispatchBatch {
				limit = maxDispatchBatch
			}
			for _, device := range pending[:limit] {
				if err := w.store.BeginDispatch(ctx, task.ProjectID, task.ID, device.DeviceKey); err != nil {
					return err
				}
				if err := w.dispatch(ctx, task, firmware, device); err != nil {
					_ = w.store.ResetDispatch(context.WithoutCancel(ctx), task.ProjectID, task.ID, device.DeviceKey)
					return err
				}
				if err := w.store.MarkNotified(ctx, task.ProjectID, task.ID, device.DeviceKey); err != nil {
					return err
				}
			}
			return w.store.ReleaseDispatchLease(ctx, task.ProjectID, task.ID)
		}
		if !ready {
			return w.store.ReleaseDispatchLease(ctx, task.ProjectID, task.ID)
		}
		rate := 1.0
		if len(cohort) > 0 {
			rate = float64(succeeded) / float64(len(cohort))
		}
		if rate < task.Rollout.SuccessThreshold {
			return w.store.SetBatchState(ctx, task.ProjectID, task.ID, batchIndex, batchIndex, TaskPaused)
		}
		batchIndex++
		status := TaskRunning
		if batchIndex == len(task.Rollout.Batches) {
			status = TaskCompleted
		}
		if err := w.store.SetBatchState(ctx, task.ProjectID, task.ID, task.BatchIndex, batchIndex, status); err != nil {
			return err
		}
		return nil
	}
	return w.store.SetBatchState(ctx, task.ProjectID, task.ID, task.BatchIndex, batchIndex, TaskCompleted)
}

func (w *RolloutWorker) dispatch(ctx context.Context, task Task, firmware Firmware, device TaskDevice) error {
	now := w.now().UTC()
	expires := now.Add(time.Hour)
	base := strings.TrimRight(w.baseURL, "/") + path.Join("/", strconv.FormatInt(task.ProjectID, 10), url.PathEscape(path.Base(firmware.ObjectKey)))
	manifest, err := w.signer.SignManifest(Manifest{Version: firmware.Version, Filename: firmware.Filename, URL: base, SizeBytes: firmware.SizeBytes, SHA256: firmware.SHA256}, now, time.Hour)
	if err != nil {
		return err
	}
	parsed, err := url.Parse(manifest.URL)
	if err != nil {
		return err
	}
	query := parsed.Query()
	query.Set("expires", strconv.FormatInt(expires.Unix(), 10))
	query.Set("sig", DownloadSignature(w.secret, task.ProjectID, firmware.ObjectKey, expires))
	parsed.RawQuery = query.Encode()
	manifest.URL = parsed.String()
	notify, err := manifest.Notification(task.ID)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(notify)
	if err != nil {
		return err
	}
	routeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return w.router.RouteExternal(routeCtx, cluster.Envelope{Topic: "v1/devices/" + device.DeviceKey + "/ota/notify", Payload: payload, Qos: 1, DeviceKey: device.DeviceKey})
}
