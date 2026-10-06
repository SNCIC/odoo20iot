package odoo

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
)

// MaintenanceRequest 是 Odoo maintenance.request 的受控读模型。
type MaintenanceRequest struct {
	ID                int64          `json:"id"`
	Name              string         `json:"name"`
	State             string         `json:"state"`
	Priority          string         `json:"priority"`
	MaintenanceType   string         `json:"maintenance_type"`
	EquipmentID       int64          `json:"equipment_id"`
	EquipmentName     string         `json:"equipment_name"`
	IoTAlarmID        string         `json:"iot_alarm_id"`
	IoTSeverity       string         `json:"iot_severity"`
	IoTDeviceKey      string         `json:"iot_device_key"`
	IoTMetricSnapshot map[string]any `json:"iot_metric_snapshot,omitempty"`
	IoTAlarmTS        string         `json:"iot_alarm_ts"`
	CreateDate        string         `json:"create_date"`
	WriteDate         string         `json:"write_date"`
	CloseDate         string         `json:"close_date"`
	CompanyID         int64          `json:"company_id"`
}

type maintenanceEnvelope struct {
	OK      bool   `json:"ok"`
	Code    any    `json:"code"`
	Message string `json:"msg"`
	Data    struct {
		Items  []MaintenanceRequest `json:"items"`
		Count  int                  `json:"count"`
		Limit  int                  `json:"limit"`
		Offset int                  `json:"offset"`
		Item   MaintenanceRequest   `json:"-"`
	} `json:"data"`
}

// MaintenanceRequestQuery 定义维修单列表筛选条件。
type MaintenanceRequestQuery struct {
	DeviceKey string
	AlarmID   string
	State     string
	Since     string
	Limit     int
	Offset    int
}

// ListMaintenanceRequests 从 Odoo 自定义业务 API 读取维修单。
func (c *Client) ListMaintenanceRequests(ctx context.Context, q MaintenanceRequestQuery) ([]MaintenanceRequest, error) {
	values := url.Values{}
	if q.DeviceKey != "" {
		values.Set("device_key", q.DeviceKey)
	}
	if q.AlarmID != "" {
		values.Set("alarm_id", q.AlarmID)
	}
	if q.State != "" {
		values.Set("state", q.State)
	}
	if q.Since != "" {
		values.Set("since", q.Since)
	}
	if q.Limit > 0 {
		values.Set("limit", strconv.Itoa(q.Limit))
	}
	if q.Offset > 0 {
		values.Set("offset", strconv.Itoa(q.Offset))
	}
	var envelope maintenanceEnvelope
	if err := c.GetJSON(ctx, "/api/iot/v1/maintenance/requests", values, &envelope); err != nil {
		return nil, err
	}
	if !envelope.OK {
		return nil, fmt.Errorf("odoo: 维修单列表失败: %v %s", envelope.Code, envelope.Message)
	}
	return envelope.Data.Items, nil
}

// GetMaintenanceRequest 读取单个维修单。
func (c *Client) GetMaintenanceRequest(ctx context.Context, id int64) (MaintenanceRequest, error) {
	if id <= 0 {
		return MaintenanceRequest{}, fmt.Errorf("odoo: 维修单 ID 必须为正数")
	}
	var envelope struct {
		OK   bool               `json:"ok"`
		Code any                `json:"code"`
		Msg  string             `json:"msg"`
		Data MaintenanceRequest `json:"data"`
	}
	if err := c.GetJSON(ctx, fmt.Sprintf("/api/iot/v1/maintenance/request/%d", id), url.Values{}, &envelope); err != nil {
		return MaintenanceRequest{}, err
	}
	if !envelope.OK {
		return MaintenanceRequest{}, fmt.Errorf("odoo: 维修单详情失败: %v %s", envelope.Code, envelope.Msg)
	}
	return envelope.Data, nil
}
