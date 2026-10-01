package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
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
}

// Broker 是接入网关的内嵌 MQTT Broker（ADR-001）。
type Broker struct {
	Server  *mqtt.Server
	Metrics *Metrics

	addr    string
	serveCh chan error

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

	// ⚠️ Phase 0 临时放行全部连接：三档设备认证（A1）尚未实现。
	// 此处刻意保持「无认证」而不是伪造一个假认证 hook —— A1 未完成就是未完成。
	if err := server.AddHook(new(auth.AllowHook), nil); err != nil {
		cancelClose()
		ln.Close()
		return nil, fmt.Errorf("装载认证 hook: %w", err)
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
	}, nil
}

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
