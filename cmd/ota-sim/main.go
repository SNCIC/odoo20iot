// ota-sim 向 IOT_OTA_PROGRESS 发布虚拟设备进度，用于开发环境闭环验收。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/SNCIC/odoo20iot/internal/cluster"
	"github.com/SNCIC/odoo20iot/internal/ota"
	"github.com/nats-io/nats.go"
)

func main() {
	var natsURL, device, task, sequence string
	var project int64
	var interval time.Duration
	flag.StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS 地址")
	flag.Int64Var(&project, "project-id", 1, "租户 ID")
	flag.StringVar(&device, "device-key", "", "设备 Key")
	flag.StringVar(&task, "task-id", "", "OTA 任务 ID")
	flag.StringVar(&sequence, "sequence", "downloading,verifying,installing,succeeded", "状态序列")
	flag.DurationVar(&interval, "interval", 500*time.Millisecond, "状态间隔")
	flag.Parse()
	if device == "" || !ota.ValidTaskID(task) || project <= 0 || interval <= 0 {
		fmt.Fprintln(os.Stderr, "必须提供有效的 -project-id、-device-key、-task-id 和 -interval")
		os.Exit(2)
	}
	subject, err := cluster.OTAProgressSubject(project, device)
	if err != nil {
		fatal(err)
	}
	conn, err := nats.Connect(natsURL)
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	js, err := conn.JetStream()
	if err != nil {
		fatal(err)
	}
	statuses := strings.Split(sequence, ",")
	for index, raw := range statuses {
		status := strings.TrimSpace(raw)
		progress := 100
		if len(statuses) > 1 {
			progress = index * 100 / (len(statuses) - 1)
		}
		payload, err := json.Marshal(ota.Progress{TaskID: task, Status: status, Progress: progress, BytesDownloaded: int64(progress), FirmwareVersion: "simulated"})
		if err != nil {
			fatal(err)
		}
		if _, err := js.Publish(subject, payload); err != nil {
			fatal(err)
		}
		fmt.Printf("published device=%s task=%s status=%s progress=%d%%\n", device, task, status, progress)
		if index+1 < len(statuses) {
			time.Sleep(interval)
		}
	}
}

func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
