package catalog

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MemStore 是 Store 的内存实现，供 handler 单测使用（不连库）。
type MemStore struct {
	mu      sync.RWMutex
	devices []Device
}

var _ Store = (*MemStore)(nil)

// NewMemStore 构造空的内存存储。
func NewMemStore() *MemStore { return &MemStore{} }

// Add 追加设备（测试装配用）。
func (m *MemStore) Add(devices ...Device) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.devices = append(m.devices, devices...)
}

// ListDevices 实现 Store。
func (m *MemStore) ListDevices(_ context.Context, f DeviceFilter) (DevicePage, error) {
	nf, err := f.Normalize()
	if err != nil {
		return DevicePage{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var matched []Device
	for _, d := range m.devices {
		if d.ProjectID != nf.ProjectID || d.DeletedAt != nil {
			continue
		}
		if nf.DeviceTypeID > 0 && d.DeviceTypeID != nf.DeviceTypeID {
			continue
		}
		if nf.Status != "" && d.Status != nf.Status {
			continue
		}
		if nf.Query != "" {
			q := strings.ToLower(nf.Query)
			if !strings.Contains(strings.ToLower(d.Name), q) && !strings.Contains(strings.ToLower(d.DeviceKey), q) {
				continue
			}
		}
		if nf.AfterID > 0 && d.ID <= nf.AfterID {
			continue
		}
		matched = append(matched, d)
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].ID < matched[j].ID })

	page := DevicePage{}
	if len(matched) > nf.Limit {
		page.NextAfterID = matched[nf.Limit-1].ID
		matched = matched[:nf.Limit]
	}
	page.Devices = matched
	return page, nil
}

// DeviceIDsOwned 实现 Store。
func (m *MemStore) DeviceIDsOwned(_ context.Context, projectID int64, ids []int64) (map[int64]bool, error) {
	out := map[int64]bool{}
	if projectID <= 0 {
		return out, fmt.Errorf("catalog: 必须提供正的 project_id")
	}

	want := make(map[int64]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, d := range m.devices {
		if want[d.ID] && d.ProjectID == projectID && d.DeletedAt == nil {
			out[d.ID] = true
		}
	}
	return out, nil
}
