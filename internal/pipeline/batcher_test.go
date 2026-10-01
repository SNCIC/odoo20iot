package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// collector 记录 flush 到的批次。
type collector struct {
	mu      sync.Mutex
	batches [][]int
}

func (c *collector) flush(_ context.Context, items []int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches = append(c.batches, append([]int(nil), items...))
	return nil
}

func (c *collector) snapshot() [][]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]int, len(c.batches))
	copy(out, c.batches)
	return out
}

// TestBatcher_FlushOnMaxSize 验证「达到 maxSize 立即 flush，不等 maxWait」。
func TestBatcher_FlushOnMaxSize(t *testing.T) {
	c := new(collector)
	b := NewBatcher[int](3, time.Hour, c.flush) // maxWait 极大：只可能由 size 触发
	b.Start(context.Background())
	defer func() { _ = b.Close(context.Background()) }()

	tickets := make([]*Ticket, 0, 3)
	for i := 0; i < 3; i++ {
		tk, err := b.Add(i)
		if err != nil {
			t.Fatalf("Add(%d) 失败: %v", i, err)
		}
		tickets = append(tickets, tk)
	}
	for i, tk := range tickets {
		if err := tk.Wait(context.Background()); err != nil {
			t.Fatalf("ticket %d 未持久化: %v", i, err)
		}
	}

	got := c.snapshot()
	if len(got) != 1 || len(got[0]) != 3 {
		t.Fatalf("应在第 3 个元素时 flush 一个 3 元素批次，实际 %v", got)
	}
}

// TestBatcher_FlushOnMaxWait 验证「未达 maxSize 时由 maxWait 触发」。
//
// 与上一条共同证明是**或**语义：若误写成 max（两者都满足才 flush），
// 单元素批次永远等不到 flush（size 条件永不满足），会一直卡到 Close。
func TestBatcher_FlushOnMaxWait(t *testing.T) {
	c := new(collector)
	b := NewBatcher[int](1000, 30*time.Millisecond, c.flush)
	b.Start(context.Background())
	defer func() { _ = b.Close(context.Background()) }()

	tk, err := b.Add(42)
	if err != nil {
		t.Fatalf("Add 失败: %v", err)
	}
	if err := tk.Wait(context.Background()); err != nil {
		t.Fatalf("maxWait 应触发 flush，Wait 得到: %v", err)
	}

	got := c.snapshot()
	if len(got) != 1 || len(got[0]) != 1 || got[0][0] != 42 {
		t.Fatalf("maxWait 应触发单元素批次 flush，实际 %v", got)
	}
}

// TestBatcher_WaitBlocksUntilFlush 是「落库后 ACK」的核心保证：
// flush 未完成前，Ticket.Wait 不得返回。
func TestBatcher_WaitBlocksUntilFlush(t *testing.T) {
	release := make(chan struct{})
	b := NewBatcher[int](1, time.Hour, func(context.Context, []int) error {
		<-release // 卡住 flush
		return nil
	})
	b.Start(context.Background())
	defer func() { _ = b.Close(context.Background()) }()

	tk, err := b.Add(1) // maxSize=1：立即触发 flush，但 flush 被卡住
	if err != nil {
		t.Fatalf("Add 失败: %v", err)
	}

	returned := make(chan struct{})
	go func() { _ = tk.Wait(context.Background()); close(returned) }()

	select {
	case <-returned:
		t.Fatal("flush 未完成时 Wait 不应返回（否则会提前 ACK，丢数据）")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("flush 完成后 Wait 应返回")
	}
}

// TestBatcher_PropagatesFlushError 验证持久化失败会如实回传给等待者。
func TestBatcher_PropagatesFlushError(t *testing.T) {
	wantErr := errors.New("写库失败（测试注入）")
	b := NewBatcher[int](2, time.Hour, func(context.Context, []int) error { return wantErr })
	b.Start(context.Background())
	defer func() { _ = b.Close(context.Background()) }()

	t1, err := b.Add(1)
	if err != nil {
		t.Fatalf("Add 失败: %v", err)
	}
	t2, err := b.Add(2) // 触发 flush（同一批次）
	if err != nil {
		t.Fatalf("Add 失败: %v", err)
	}

	if err := t2.Wait(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("触发 flush 的 ticket 应返回错误，得到 %v", err)
	}
	if err := t1.Wait(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("同批次的其他 ticket 也应收到错误，得到 %v", err)
	}
}

// TestBatcher_CloseFlushesRemaining 验证优雅退出会 flush 缓冲区剩余元素。
func TestBatcher_CloseFlushesRemaining(t *testing.T) {
	c := new(collector)
	b := NewBatcher[int](1000, time.Hour, c.flush) // 既不满批、maxWait 也极长
	b.Start(context.Background())

	tk, err := b.Add(7)
	if err != nil {
		t.Fatalf("Add 失败: %v", err)
	}

	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}

	if err := tk.Wait(context.Background()); err != nil {
		t.Fatalf("Close 应 flush 剩余元素并让 ticket 成功，得到 %v", err)
	}
	got := c.snapshot()
	if len(got) != 1 || len(got[0]) != 1 || got[0][0] != 7 {
		t.Fatalf("Close 应 flush 剩余元素 [7]，实际 %v", got)
	}
}

// TestBatcher_AddAfterClose 验证关闭后拒绝新元素。
func TestBatcher_AddAfterClose(t *testing.T) {
	b := NewBatcher[int](10, time.Hour, func(context.Context, []int) error { return nil })
	b.Start(context.Background())
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}

	if _, err := b.Add(1); !errors.Is(err, ErrBatcherClosed) {
		t.Fatalf("关闭后 Add 应返回 ErrBatcherClosed，得到 %v", err)
	}
}
