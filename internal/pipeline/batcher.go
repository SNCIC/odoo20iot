// Package pipeline 实现 03 §4 的消息管道（svc-pipeline）核心：
// 信封解包 → 物模型解析 → 攒批写入。
package pipeline

import (
	"context"
	"errors"
	"sync"
	"time"
)

// DefaultBatchSize / DefaultBatchWait 是 03 §4.2 的攒批阈值。
const (
	DefaultBatchSize = 1000
	DefaultBatchWait = 200 * time.Millisecond
)

// ErrBatcherClosed 表示攒批器已关闭，不再接收新元素。
var ErrBatcherClosed = errors.New("攒批器已关闭")

// Ticket 是一次入批的凭据。调用方在 **ACK 之前**必须调用 Wait：
// 只有拿到 nil 才代表该元素已随批次持久化。
//
// 为什么不是「Add 自己阻塞」：若 Add 阻塞到批次 flush，未满批次的元素会
// 一直等到 maxWait，顺序调用者的吞吐被压到 1/maxWait（≈5 msg/s），
// 与 03 §4.2「单分片吞吐 ≥ 5000 msg/s」直接冲突。因此拆成
// 「非阻塞入批 + 显式等待」，让调用方可以并发入批、再统一等待。
type Ticket struct {
	once sync.Once
	ch   chan error
	err  error
}

// Wait 阻塞到该元素所属批次 flush 完成。返回 nil 表示已落库，可 ACK。
//
// 可重复调用：首次等待后结果被缓存。这一点必须有 —— 结果 channel 在
// 广播后会被关闭，若不缓存，第二次读会拿到零值 nil，把失败误报成成功。
func (t *Ticket) Wait(ctx context.Context) error {
	t.once.Do(func() {
		select {
		case t.err = <-t.ch:
		case <-ctx.Done():
			t.err = ctx.Err()
		}
	})
	return t.err
}

// Batcher 实现 03 §4.3 的攒批语义：
//
//   - flush 触发：`len(buf) >= maxSize` **或** `距上次 flush ≥ maxWait`，
//     **任一先到即 flush**（不是 max —— 那是「两者都满足」，最坏延迟翻倍）；
//   - 入批非阻塞，返回 Ticket；`Ticket.Wait` 代表持久化结果；
//   - 优雅退出时 flush 剩余元素。
//
// 并发模型：`Add` 可由多个消费协程并发调用；flush 串行执行（flushMu），
// 保证批次写入不交错。
type Batcher[T any] struct {
	maxSize int
	maxWait time.Duration
	flush   func(context.Context, []T) error

	mu      sync.Mutex
	buf     []T
	waiters []chan error
	closed  bool

	flushMu sync.Mutex
	flushCh chan struct{} // 满批通知（容量 1，天然合并重复通知）

	loopCancel context.CancelFunc
	loopDone   chan struct{}
}

// NewBatcher 构造攒批器。maxSize <= 0 取 1000，maxWait <= 0 取 200ms。
func NewBatcher[T any](maxSize int, maxWait time.Duration, flush func(context.Context, []T) error) *Batcher[T] {
	if maxSize <= 0 {
		maxSize = DefaultBatchSize
	}
	if maxWait <= 0 {
		maxWait = DefaultBatchWait
	}
	return &Batcher[T]{
		maxSize: maxSize,
		maxWait: maxWait,
		flush:   flush,
		flushCh: make(chan struct{}, 1),
	}
}

// Start 启动 flush 循环（幂等）。未启动时不会发生超时 flush。
func (b *Batcher[T]) Start(ctx context.Context) {
	b.mu.Lock()
	if b.loopCancel != nil {
		b.mu.Unlock()
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	b.loopCancel = cancel
	b.loopDone = make(chan struct{})
	done := b.loopDone
	b.mu.Unlock()

	go func() {
		defer close(done)
		b.loop(loopCtx)
	}()
}

func (b *Batcher[T]) loop(ctx context.Context) {
	tick := time.NewTicker(b.maxWait)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			b.flushCurrent(ctx)
		case <-b.flushCh:
			b.flushCurrent(ctx)
		}
	}
}

// Add 把一个元素加入批次，**立即返回**（不阻塞）。
//
// 达到 maxSize 时向 flush 循环发信号立即 flush，不等 maxWait。
// 调用方必须在 ACK 前对返回的 Ticket 调用 Wait。
func (b *Batcher[T]) Add(item T) (*Ticket, error) {
	ch := make(chan error, 1)
	t := &Ticket{ch: ch}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrBatcherClosed
	}
	b.buf = append(b.buf, item)
	b.waiters = append(b.waiters, ch)
	full := len(b.buf) >= b.maxSize
	b.mu.Unlock()

	if full {
		select {
		case b.flushCh <- struct{}{}:
		default: // 已有待处理信号：本次批次会在那次 flush 中一并处理
		}
	}
	return t, nil
}

// Close 停止循环并 flush 剩余元素（03 §4.3「优雅退出」）。
func (b *Batcher[T]) Close(ctx context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	cancel, done := b.loopCancel, b.loopDone
	b.mu.Unlock()

	if cancel != nil {
		cancel()
		<-done
	}
	b.flushCurrent(ctx)
	return nil
}

// flushCurrent 取出当前批次并 flush（空批次直接返回）。
func (b *Batcher[T]) flushCurrent(ctx context.Context) {
	b.mu.Lock()
	batch, waiters := b.takeLocked()
	b.mu.Unlock()

	if len(batch) == 0 {
		return
	}
	b.doFlush(ctx, batch, waiters)
}

func (b *Batcher[T]) takeLocked() ([]T, []chan error) {
	batch, waiters := b.buf, b.waiters
	b.buf, b.waiters = nil, nil
	return batch, waiters
}

// doFlush 串行执行一次持久化，并把结果广播给该批次的所有等待者。
func (b *Batcher[T]) doFlush(ctx context.Context, batch []T, waiters []chan error) {
	b.flushMu.Lock()
	err := b.flush(ctx, batch)
	b.flushMu.Unlock()

	for _, w := range waiters {
		w <- err
		close(w)
	}
}
