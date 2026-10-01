package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	mqttauth "github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	"github.com/SNCIC/odoo20iot/internal/auth"
)

// Options 是接入网关的启动参数。
type Options struct {
	// MQTTAddr 是 MQTT 监听地址（TCP）。
	MQTTAddr string
	// Publisher 是内部事件总线的同步投递器，A2 时序依赖它。
	Publisher Publisher
	// Router 把设备 topic 映射为总线 subject。
	Router SubjectRouter
	// PubackTimeout 是等待 PublishAck 的上限（§4.4 默认 5s）。
	PubackTimeout time.Duration
	// Metrics 可为空；空时内部新建。
	Metrics *Metrics
	// Log 可为空；空时使用 slog 默认实例。
	Log *slog.Logger

	// Authenticator 提供 03 §2.1 的三档设备认证。
	// 与 AllowAnonymous 二选一，**必须显式指定其一**。
	Authenticator *auth.Authenticator

	// AllowAnonymous 显式开启「放行全部连接」。
	//
	// 它存在只是为了本地冒烟与单元测试。把它做成必须显式声明的开关，
	// 是为了让「忘记配认证」不能静默退化成「谁都能连」——
	// 后者是接入层最危险的默认值。
	AllowAnonymous bool
}

// Broker 是接入网关的内嵌 MQTT Broker（ADR-001）。
type Broker struct {
	Server  *mqtt.Server
	Metrics *Metrics

	addr     string
	serveCh  chan error
	authHook *AuthHook

	cancelClose context.CancelFunc
	closeOnce   sync.Once
	closeErr    error
}

// 默认等待 PublishAck 的上限（docs/03-ingestion.md §4.4）。
const DefaultPubackTimeout = 5 * time.Second

// New 组装 Broker：监听器 + 认证 hook + A2 hook。
func New(ctx context.Context, opts Options) (*Broker, error) {
	if opts.Publisher == nil {
		return nil, fmt.Errorf("Options.Publisher 不能为空：A2 时序依赖同步投递器")
	}
	if opts.Router == nil {
		return nil, fmt.Errorf("Options.Router 不能为空")
	}
	if opts.PubackTimeout <= 0 {
		opts.PubackTimeout = DefaultPubackTimeout
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.Metrics == nil {
		opts.Metrics = new(Metrics)
	}

	// 关闭时取消在途的等待：否则每个阻塞在 OnPublish 的连接都要等满
	// PubackTimeout，优雅关闭会被拖长到超时上限。取消后这些消息走
	// 「未确认」分支 —— 语义正确（设备会重传），且不再阻塞关闭。
	runCtx, cancelClose := context.WithCancel(ctx)

	ln, err := net.Listen("tcp", opts.MQTTAddr)
	if err != nil {
		cancelClose()
		return nil, fmt.Errorf("监听 MQTT %q: %w", opts.MQTTAddr, err)
	}

	server := mqtt.New(&mqtt.Options{Logger: opts.Log})
	var authHook *AuthHook

	// 认证必须显式配置：要么给认证器，要么显式声明放行匿名。
	if opts.Authenticator == nil && !opts.AllowAnonymous {
		cancelClose()
		ln.Close()
		return nil, fmt.Errorf(
			"未配置 Options.Authenticator：请提供认证器，或显式设置 Options.AllowAnonymous=true（仅限本地冒烟/测试）")
	}
	if opts.Authenticator == nil {
		opts.Log.Error("⚠️ 已放行全部连接（Options.AllowAnonymous=true）：此模式不得用于任何非本地环境")
		if err := server.AddHook(new(mqttauth.AllowHook), nil); err != nil {
			cancelClose()
			ln.Close()
			return nil, fmt.Errorf("装载匿名放行 hook: %w", err)
		}
	} else {
		authHook = NewAuthHook(runCtx, opts.Authenticator, opts.Metrics, opts.Log)
		if err := server.AddHook(authHook, nil); err != nil {
			cancelClose()
			ln.Close()
			return nil, fmt.Errorf("装载认证 hook: %w", err)
		}
	}

	acker := NewAcker(opts.Publisher, opts.PubackTimeout, opts.Metrics)
	hook := NewHook(runCtx, acker, opts.Router, opts.Metrics, opts.Log)
	if err := server.AddHook(hook, nil); err != nil {
		cancelClose()
		ln.Close()
		return nil, fmt.Errorf("装载 A2 hook: %w", err)
	}

	if err := server.AddListener(listeners.NewNet("mqtt-tcp", ln)); err != nil {
		cancelClose()
		ln.Close()
		return nil, fmt.Errorf("装载监听器: %w", err)
	}

	return &Broker{
		Server:      server,
		Metrics:     opts.Metrics,
		addr:        ln.Addr().String(),
		serveCh:     make(chan error, 1),
		cancelClose: cancelClose,
		authHook:    authHook,
	}, nil
}

// AuthHook 返回认证 Hook；未启用认证时为 nil。
//
// 暴露它是为了让控制面（凭据轮换后清缓存）与测试能拿到它 ——
// mochi 的 Server 没有 Hooks() 访问器。
func (b *Broker) AuthHook() *AuthHook { return b.authHook }

// Addr 返回实际监听地址（测试中用 :0 时取真实端口）。
func (b *Broker) Addr() string { return b.addr }

// Serve 启动服务，非阻塞；错误通过 Err() 取出。
func (b *Broker) Serve() {
	go func() { b.serveCh <- b.Server.Serve() }()
}

// Err 返回 Serve 的退出原因（阻塞）。
func (b *Broker) Err() error { return <-b.serveCh }

// Close 优雅关闭 broker（含监听器与全部客户端连接）。
//
// 幂等：mochi-mqtt 的 Server.Close 会 close 内部 done channel 且**不可重入**，
// 而监听器也由它负责关闭，因此这里必须自己做一次性保护，且不要重复关闭监听器。
func (b *Broker) Close() error {
	b.closeOnce.Do(func() {
		b.cancelClose() // 先放掉在途的 PUBACK 等待，再关服务
		b.closeErr = b.Server.Close()
	})
	return b.closeErr
}
