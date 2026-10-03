package quota

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

const QuotaAlertSubjectPrefix = "iot.quota.alert"

func QuotaAlertSubject(projectID int64) string {
	return fmt.Sprintf("%s.%d", QuotaAlertSubjectPrefix, projectID)
}

type AlertEvent struct {
	ProjectID   int64     `json:"project_id"`
	Metric      string    `json:"metric"`
	Level       string    `json:"level"`
	Usage       int64     `json:"usage"`
	Limit       int64     `json:"limit"`
	WindowStart time.Time `json:"window_start"`
}

func (a Alert) Event() AlertEvent {
	return AlertEvent{ProjectID: a.ProjectID, Metric: a.Metric, Level: a.Level, Usage: a.Usage, Limit: a.Limit, WindowStart: a.WindowStart}
}

func (e AlertEvent) Data() ([]byte, error) { return json.Marshal(e) }

func (e AlertEvent) Subject() string { return QuotaAlertSubject(e.ProjectID) }

func (e AlertEvent) RuleID() string { return "quota:" + e.Metric }

func (e AlertEvent) Project() string { return strconv.FormatInt(e.ProjectID, 10) }
