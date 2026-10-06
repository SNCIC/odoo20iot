package ruleengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/SNCIC/odoo20iot/internal/alarm"
	"github.com/SNCIC/odoo20iot/internal/command"
	"github.com/SNCIC/odoo20iot/internal/dag"
	"github.com/SNCIC/odoo20iot/internal/envelope"
	"github.com/SNCIC/odoo20iot/internal/latest"
	"github.com/SNCIC/odoo20iot/internal/ruleconfig"
	"github.com/SNCIC/odoo20iot/internal/rules"
	"github.com/SNCIC/odoo20iot/internal/rules/script"
	"github.com/SNCIC/odoo20iot/internal/tsdb"
)

type Publisher interface {
	Publish(context.Context, string, []byte) error
}
type RuleLoader interface {
	LoadEnabled(context.Context, string, *rules.DeviceSchema, *dag.Registry) ([]ruleconfig.Rule, error)
}
type schemaProvider interface {
	DeviceSchema(context.Context, string, int64) (*rules.DeviceSchema, error)
}
type fixedLocator struct{ projectID, deviceID int64 }

func (l fixedLocator) LocateDevice(context.Context, string) (int64, int64, error) {
	return l.projectID, l.deviceID, nil
}

type Config struct {
	ReloadEvery   time.Duration
	ScriptEnabled bool
	CommandSender command.Sender
}
type Engine struct {
	loader        RuleLoader
	cache         rules.PrevSnapshotStore
	latest        latest.Store
	publish       Publisher
	compiler      *rules.Compiler
	script        *script.Engine
	commandSender command.Sender
	logger        *slog.Logger
	mu            sync.Mutex
	cached        map[string]cachedRules
	reloadEvery   time.Duration
}
type cachedRules struct {
	at    time.Time
	rules []ruleconfig.Rule
}

func New(loader RuleLoader, cache latest.Store, history rules.PrevSnapshotStore, publish Publisher, cfg Config, logger *slog.Logger) (*Engine, error) {
	if loader == nil || publish == nil {
		return nil, fmt.Errorf("ruleengine: loader 和 publisher 不能为空")
	}
	if cfg.ReloadEvery <= 0 {
		cfg.ReloadEvery = time.Minute
	}
	if logger == nil {
		logger = slog.Default()
	}
	var scriptEngine *script.Engine
	if cfg.ScriptEnabled {
		var err error
		scriptEngine, err = script.New(script.Config{Enabled: true})
		if err != nil {
			return nil, err
		}
	}
	return &Engine{loader: loader, latest: cache, cache: history, publish: publish, compiler: rules.NewCompiler(10000), script: scriptEngine, commandSender: cfg.CommandSender, logger: logger, cached: make(map[string]cachedRules), reloadEvery: cfg.ReloadEvery}, nil
}

func (e *Engine) Process(ctx context.Context, env envelope.Envelope) error {
	var body struct {
		Ts   string         `json:"ts"`
		Seq  *int64         `json:"seq"`
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(env.Payload, &body); err != nil {
		return fmt.Errorf("解析遥测 payload: %w", err)
	}
	at := env.ReceivedAt
	if body.Ts != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, body.Ts); err == nil {
			at = parsed
		}
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	ruleset, err := e.load(ctx, fmt.Sprint(env.ProjectID), env.DeviceTypeID)
	if err != nil {
		return err
	}
	seq := int64(0)
	if body.Seq != nil {
		seq = *body.Seq
	}
	meta := rules.NewMeta(env.DeviceID, env.DeviceTypeID, env.ProjectID, 0, "", nil, nil, at, seq)
	meta["device_key"] = env.DeviceKey
	var prevResolver rules.CachedPrevResolver
	hasPrev := false
	if e.latest != nil || e.cache != nil {
		prevResolver = rules.CachedPrevResolver{Locator: fixedLocator{projectID: env.ProjectID, deviceID: env.DeviceID}, Cache: e.latest, History: e.cache}
		hasPrev = true
	}
	var prev map[string]any
	if hasPrev {
		if body.Seq == nil {
			prev, err = prevResolver.ResolvePrev(ctx, env.DeviceKey, at)
		} else {
			prev, err = prevResolver.ResolvePrevWithSeq(ctx, env.DeviceKey, at, seq)
		}
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, pgx.ErrNoRows) {
				prev = nil
			} else {
				e.logger.Warn("读取 prev 快照失败，按空快照求值", "device_key", env.DeviceKey, "error", err)
				prev = nil
			}
		}
	}
	for _, rule := range ruleset {
		if rule.Match.DeviceTypeID > 0 && rule.Match.DeviceTypeID != env.DeviceTypeID {
			continue
		}
		ok, err := rule.Program.Eval(rules.NewEnv().Bind(body.Data, meta, prev, rule.Window, nil))
		if err != nil {
			return fmt.Errorf("规则 %s 求值: %w", rule.RuleID, err)
		}
		if !ok {
			continue
		}
		if rule.Graph != nil {
			ruleEnv := rules.NewEnv().Bind(body.Data, meta, prev, rule.Window, nil)
			executor := dag.NewExecutor(rules.NewRunner(), e.logger)
			result, err := executor.Run(ctx, rule.Graph, ruleEnv, dag.RunOptions{Policy: rule.ErrorPolicy})
			if err != nil || result.Failed {
				if err != nil {
					return fmt.Errorf("规则 %s DAG: %w", rule.RuleID, err)
				}
				return fmt.Errorf("规则 %s DAG 执行失败", rule.RuleID)
			}
			continue
		}
		payload := alarm.TriggerPayload{ProjectID: fmt.Sprint(env.ProjectID), DeviceID: fmt.Sprint(env.DeviceID), DeviceTypeID: env.DeviceTypeID, RuleID: rule.RuleID, RuleName: rule.Name, Level: rule.Level, At: at, Value: env.Payload}
		data, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if err := e.publish.Publish(ctx, alarm.TriggerSubject, data); err != nil {
			return fmt.Errorf("发布规则告警: %w", err)
		}
	}
	return nil
}

func (e *Engine) load(ctx context.Context, projectID string, deviceTypeID int64) ([]ruleconfig.Rule, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	cacheKey := fmt.Sprintf("%s:%d", projectID, deviceTypeID)
	if got, ok := e.cached[cacheKey]; ok && time.Since(got.at) < e.reloadEvery {
		return got.rules, nil
	}
	actions := []dag.Action{dag.Func{ActionName: "alarm.raise", Idem: true, Run: e.raiseAction}}
	if e.commandSender.Router != nil {
		actions = append(actions, command.Action{Sender: e.commandSender})
	}
	if e.script != nil {
		actions = append(actions, script.Action{Engine: e.script, Emit: e.emitScript})
	}
	registry, err := dag.NewRegistry(actions...)
	if err != nil {
		return nil, err
	}
	schema := &rules.DeviceSchema{Metrics: metricSchema(), AllowUnknownMetrics: true}
	if provider, ok := e.loader.(schemaProvider); ok {
		loadedSchema, schemaErr := provider.DeviceSchema(ctx, projectID, deviceTypeID)
		if schemaErr == nil && loadedSchema != nil {
			schema = loadedSchema
		}
	}
	loaded, err := e.loader.LoadEnabled(ctx, projectID, schema, registry)
	if err != nil {
		return nil, err
	}
	for i := range loaded {
		if len(loaded[i].DAG.Nodes) == 0 {
			continue
		}
		for nodeIndex := range loaded[i].DAG.Nodes {
			node := &loaded[i].DAG.Nodes[nodeIndex]
			if node.Type == dag.NodeAction && node.Action == "script.run" {
				if e.script == nil {
					return nil, fmt.Errorf("规则 %s 使用 script.run，但脚本开关未开启", loaded[i].RuleID)
				}
				if !contains(loaded[i].Capabilities, "script.run") {
					return nil, fmt.Errorf("规则 %s 使用 script.run，但 capabilities 未声明", loaded[i].RuleID)
				}
			}
			if node.Type == dag.NodeAction && node.Action == "alarm.raise" {
				if node.Params == nil {
					node.Params = map[string]any{}
				}
				setDefault(node.Params, "project_id", "$meta.project_id")
				setDefault(node.Params, "device_id", "$meta.device_id")
				setDefault(node.Params, "device_type_id", "$meta.device_type_id")
				setDefault(node.Params, "rule_id", loaded[i].RuleID)
				setDefault(node.Params, "rule_name", loaded[i].Name)
				setDefault(node.Params, "level", loaded[i].Level)
			}
			if node.Type == dag.NodeAction && node.Action == "command.send" {
				if e.commandSender.Router == nil {
					return nil, fmt.Errorf("规则 %s 使用 command.send，但下行路由未配置", loaded[i].RuleID)
				}
				if node.Params == nil {
					node.Params = map[string]any{}
				}
				// 命令只能发回触发当前规则的设备；规则配置不可覆盖可信设备身份。
				node.Params["device_key"] = "$meta.device_key"
				node.Params["project_id"] = "$meta.project_id"
			}
			if node.Type == dag.NodeAction && node.Action == "script.run" {
				if node.Params == nil {
					node.Params = map[string]any{}
				}
				setDefault(node.Params, "msg", "$msg")
				setDefault(node.Params, "meta", "$meta")
				setDefault(node.Params, "prev", "$prev")
				setDefault(node.Params, "state", "$state")
				setDefault(node.Params, "capabilities", loaded[i].Capabilities)
				setDefault(node.Params, "project_id", "$meta.project_id")
				setDefault(node.Params, "device_id", "$meta.device_id")
				setDefault(node.Params, "device_type_id", "$meta.device_type_id")
				setDefault(node.Params, "rule_id", loaded[i].RuleID)
				setDefault(node.Params, "rule_name", loaded[i].Name)
			}
		}
		graph, err := dag.Compile(loaded[i].DAG, dag.Deps{Registry: registry, Compiler: e.compiler, Schema: schema, KeyPrefix: loaded[i].RuleID})
		if err != nil {
			return nil, fmt.Errorf("规则 %s DAG: %w", loaded[i].RuleID, err)
		}
		loaded[i].Graph = graph
	}
	e.cached[cacheKey] = cachedRules{at: time.Now(), rules: loaded}
	e.logger.Info("规则已加载", "project_id", projectID, "rules", len(loaded))
	return loaded, nil
}

func setDefault(values map[string]any, key string, value any) {
	if _, exists := values[key]; !exists {
		values[key] = value
	}
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (e *Engine) raiseAction(ctx context.Context, params map[string]any) error {
	projectValue, ok := params["project_id"]
	projectID := fmt.Sprint(projectValue)
	if !ok || projectID == "" || projectID == "<nil>" {
		return fmt.Errorf("alarm.raise 缺少 project_id")
	}
	deviceValue, ok := params["device_id"]
	deviceID := fmt.Sprint(deviceValue)
	if !ok || deviceID == "" || deviceID == "<nil>" {
		return fmt.Errorf("alarm.raise 缺少 device_id")
	}
	ruleID, ok := params["rule_id"].(string)
	if !ok || ruleID == "" {
		return fmt.Errorf("alarm.raise 缺少 rule_id")
	}
	level, ok := params["level"].(string)
	if !ok || level == "" {
		return fmt.Errorf("alarm.raise 缺少 level")
	}
	payload := alarm.TriggerPayload{ProjectID: projectID, DeviceID: deviceID, RuleID: ruleID, Level: level}
	if v, ok := params["device_type_id"].(int64); ok {
		payload.DeviceTypeID = v
	}
	if v, ok := params["rule_name"].(string); ok {
		payload.RuleName = v
	}
	if v, ok := params["value"]; ok {
		raw, err := json.Marshal(v)
		if err != nil {
			return err
		}
		payload.Value = raw
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return e.publish.Publish(ctx, alarm.TriggerSubject, data)
}

func (e *Engine) emitScript(ctx context.Context, kind string, value any) error {
	if kind != "alarm" {
		return fmt.Errorf("script emit 类型 %q 未实现", kind)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("script alarm 输出编码失败: %w", err)
	}
	if _, err := alarm.ParseTrigger(data); err != nil {
		return fmt.Errorf("script alarm 输出契约无效: %w", err)
	}
	if err := e.publish.Publish(ctx, alarm.TriggerSubject, data); err != nil {
		return fmt.Errorf("script alarm 输出发布失败: %w", err)
	}
	return nil
}

func metricSchema() map[string]rules.Kind {
	out := make(map[string]rules.Kind, len(tsdb.BenchMetrics))
	for _, metric := range tsdb.BenchMetrics {
		if metric.Kind == tsdb.KindBool {
			out[metric.Key] = rules.KindBool
		} else {
			out[metric.Key] = rules.KindNumber
		}
	}
	return out
}
