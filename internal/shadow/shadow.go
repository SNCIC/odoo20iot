package shadow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/cluster"
)

const MaxStateBytes = 8 << 10

var (
	ErrNotFound        = errors.New("shadow: device not found")
	ErrVersionConflict = errors.New("shadow: version conflict")
)

type Snapshot struct {
	ProjectID         int64          `json:"project_id"`
	DeviceKey         string         `json:"device_key"`
	Desired           map[string]any `json:"desired"`
	Reported          map[string]any `json:"reported"`
	Delta             map[string]any `json:"delta"`
	Version           int64          `json:"version"`
	DesiredUpdatedAt  time.Time      `json:"desired_updated_at"`
	ReportedUpdatedAt *time.Time     `json:"reported_updated_at,omitempty"`
	UpdatedAt         time.Time      `json:"updated_at"`
}

type Store interface {
	Get(context.Context, int64, string) (Snapshot, error)
	UpdateDesired(context.Context, int64, string, map[string]any, *int64) (Snapshot, error)
	ApplyReported(context.Context, int64, string, map[string]any) (Snapshot, error)
}

type Router interface {
	RouteExternal(context.Context, cluster.Envelope) error
}

type Service struct {
	Store  Store
	Router Router
}

func (s *Service) Get(ctx context.Context, projectID int64, deviceKey string) (Snapshot, error) {
	if s == nil || s.Store == nil {
		return Snapshot{}, fmt.Errorf("shadow: store 未配置")
	}
	return s.Store.Get(ctx, projectID, deviceKey)
}

func (s *Service) UpdateDesired(ctx context.Context, projectID int64, deviceKey string, patch map[string]any, expectedVersion *int64) (Snapshot, error) {
	if s == nil || s.Store == nil || s.Router == nil {
		return Snapshot{}, fmt.Errorf("shadow: desired 路由未配置")
	}
	snapshot, err := s.Store.UpdateDesired(ctx, projectID, deviceKey, patch, expectedVersion)
	if err != nil {
		return Snapshot{}, err
	}
	if len(snapshot.Delta) == 0 {
		return snapshot, nil
	}
	data, err := json.Marshal(struct {
		Version int64          `json:"version"`
		Desired map[string]any `json:"desired"`
		Delta   map[string]any `json:"delta"`
	}{snapshot.Version, snapshot.Desired, snapshot.Delta})
	if err != nil {
		return snapshot, fmt.Errorf("shadow: 编码 desired 下发载荷: %w", err)
	}
	if strings.ContainsAny(deviceKey, "/+#") {
		return snapshot, fmt.Errorf("shadow: device_key 含 MQTT 非法字符")
	}
	if err := s.Router.RouteExternal(ctx, cluster.Envelope{
		Topic:     fmt.Sprintf("v1/devices/%s/shadow/desired", deviceKey),
		Payload:   data,
		Qos:       1,
		DeviceKey: deviceKey,
	}); err != nil {
		return snapshot, fmt.Errorf("shadow: 保存成功但下发失败: %w", err)
	}
	return snapshot, nil
}

func (s *Service) ApplyReported(ctx context.Context, projectID int64, deviceKey string, reported map[string]any) (Snapshot, error) {
	if s == nil || s.Store == nil {
		return Snapshot{}, fmt.Errorf("shadow: store 未配置")
	}
	return s.Store.ApplyReported(ctx, projectID, deviceKey, reported)
}

func Merge(base, patch map[string]any) map[string]any {
	merged := cloneMap(base)
	for key, value := range patch {
		if value == nil {
			delete(merged, key)
			continue
		}
		patchMap, patchIsMap := value.(map[string]any)
		baseMap, baseIsMap := merged[key].(map[string]any)
		if patchIsMap && baseIsMap {
			merged[key] = Merge(baseMap, patchMap)
		} else {
			merged[key] = value
		}
	}
	return merged
}

func Delta(desired, reported map[string]any) map[string]any {
	delta := make(map[string]any)
	for key, desiredValue := range desired {
		reportedValue, exists := reported[key]
		if !exists {
			delta[key] = desiredValue
			continue
		}
		desiredMap, desiredIsMap := desiredValue.(map[string]any)
		reportedMap, reportedIsMap := reportedValue.(map[string]any)
		if desiredIsMap && reportedIsMap {
			nested := Delta(desiredMap, reportedMap)
			if len(nested) > 0 {
				delta[key] = nested
			}
			continue
		}
		if !reflect.DeepEqual(desiredValue, reportedValue) {
			delta[key] = desiredValue
		}
	}
	return delta
}

func cloneMap(source map[string]any) map[string]any {
	copy := make(map[string]any, len(source))
	for key, value := range source {
		if nested, ok := value.(map[string]any); ok {
			copy[key] = cloneMap(nested)
		} else {
			copy[key] = value
		}
	}
	return copy
}
