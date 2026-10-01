package notify

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"text/template"
	"time"
)

// DefaultAlarmTemplate 是告警通知的默认模板（04 §2.3 第 2 步）。
//
// 刻意用纯文本而不是 HTML/Markdown：三条通道里两条（短信、邮件纯文本）
// 读不了富文本，而短信那条根本发不出正文（只能发模板变量）。
// 用一份文本渲染三处，好过让三处各自漂移。
const DefaultAlarmTemplate = `【{{ levelZh .Level }}】{{ .Subject }}
设备：{{ .DeviceID }}
告警：{{ .AlarmID }}
时间：{{ timeFmt .At }}`

// Renderer 渲染通知正文（04 §2.3：Go text/template，支持过滤器）。
type Renderer struct {
	mu    sync.RWMutex
	cache map[string]*template.Template
	src   map[string]string
}

// NewRenderer 构造并注册内置模板。
func NewRenderer() *Renderer {
	r := &Renderer{
		cache: make(map[string]*template.Template),
		src:   make(map[string]string),
	}
	r.src["alarm"] = DefaultAlarmTemplate
	return r
}

// Parse 注册/替换一个模板并**立即解析**。
//
// 立刻解析是刻意的：模板来自控制台配置，解析错误必须在保存那一刻就报出去
// （对齐 04 §1.6「保存即阻断」）。等到凌晨三点告警要发的时候才发现
// 模板里少个括号，代价是一整次通知丢失。
func (r *Renderer) Parse(name, src string) error {
	t, err := template.New(name).Funcs(FuncMap()).Parse(src)
	if err != nil {
		return fmt.Errorf("notify: 模板 %s 解析失败: %w", name, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache[name] = t
	r.src[name] = src
	return nil
}

// Render 渲染指定模板；name 为空时用 "alarm"。
func (r *Renderer) Render(name string, data any) (string, error) {
	if name == "" {
		name = "alarm"
	}
	r.mu.RLock()
	t, ok := r.cache[name]
	src := r.src[name]
	r.mu.RUnlock()

	if !ok {
		if src == "" {
			return "", Permanent("模板 %s 不存在", name)
		}
		var err error
		if t, err = template.New(name).Funcs(FuncMap()).Parse(src); err != nil {
			return "", Permanent("模板 %s 解析失败: %v", name, err)
		}
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		// 渲染失败是**配置错误**：重试三次不会让模板变得可渲染。
		return "", Permanent("模板 %s 渲染失败: %v", name, err)
	}
	return strings.TrimRight(buf.String(), "\n"), nil
}

// FuncMap 是模板可用的过滤器（04 §2.3 的「支持过滤器」）。
func FuncMap() template.FuncMap {
	return template.FuncMap{
		"levelZh": levelZh,
		"timeFmt": func(t time.Time) string {
			if t.IsZero() {
				return "-"
			}
			// 展示层按项目时区转换是控制台的职责；通知面向值班的人，
			// 用带时区的本地格式比 UTC 更不容易误判「到底是几点」。
			return t.Local().Format("2006-01-02 15:04:05")
		},
		"upper": strings.ToUpper,
		"truncate": func(n int, s string) string {
			if n <= 0 || len(s) <= n {
				return s
			}
			return s[:n] + "…"
		},
	}
}

func levelZh(level string) string {
	switch strings.ToLower(level) {
	case "critical":
		return "严重"
	case "warn", "warning":
		return "警告"
	case "info":
		return "提示"
	default:
		// 不认识的级别**原样透出**而不是编一个中文名：
		// 编出来的「未知」会让人以为是平台的问题，
		// 而原样透出能直接看出是规则里 level 写错了。
		return level
	}
}
