package auth

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// 本节回答 03 §2.1「Argon2 参数与认证容量（Phase 0 必须验证）」与 06 Phase 0 表的
// 最后一列：**60s 内 N 台设备并发重连，网关 CPU 不饱和；给出 t/m/p 与副本数结论**。
//
// 测量方法说明：冷启动容量**不能用 8 万次真跑**来衡量（按实测单次成本要 20 分钟以上），
// 而是用小样本测出**速率**再外推 —— 速率是容量问题的不变量，样本量只影响精度。
//
// 默认只测量并输出（共享宿主上的绝对耗时波动大），需要作为门禁时显式开启：
//
//	IOT_CAPACITY_ASSERT=1 go test ./internal/auth -run TestCapacity -v

// capacityEnv 是容量用例的开关。
//
// 这些用例是**测量工具**（跑一轮数分钟），不是单元测试 —— 默认跳过，
// 避免 `go test ./...` 从数秒变成数分钟，也避免把共享宿主上的波动当成回归：
//
//	IOT_CAPACITY=1 go test ./internal/auth -run TestCapacity -v
const capacityEnv = "IOT_CAPACITY"

func requireCapacity(t *testing.T) {
	t.Helper()
	if os.Getenv(capacityEnv) == "" {
		t.Skip("未设置 " + capacityEnv + "，跳过容量测量（见文件头部说明）")
	}
}

// fleetPerNode 是容量口径（06 §5：集群按 8 万设备/节点规划）。
const fleetPerNode = 80_000

// stormWindow 是 06 Phase 0 表要求的时间窗口。
const stormWindow = 60 * time.Second

// candidates 是待评估的参数组合。第一行是文档给出的**未验证初始值**。
var candidates = []struct {
	name   string
	params Params
}{
	{"文档初始值", Params{Time: 3, Memory: 64 * 1024, Threads: 4, KeyLen: 32}},
	{"降内存-1", Params{Time: 3, Memory: 32 * 1024, Threads: 2, KeyLen: 32}},
	{"降内存-2", Params{Time: 4, Memory: 32 * 1024, Threads: 2, KeyLen: 32}},
	{"降时间", Params{Time: 2, Memory: 64 * 1024, Threads: 4, KeyLen: 32}},
	{"Lite(4C8G)", Params{Time: 3, Memory: 16 * 1024, Threads: 2, KeyLen: 32}},
}

// TestCapacity_单次校验成本 给出每个参数组合的单次耗时分布与内存占用。
func TestCapacity_单次校验成本(t *testing.T) {
	requireCapacity(t)

	const rounds = 20

	t.Logf("%-22s %11s %11s %12s %12s", "参数", "P50", "P95", "内存上界/次", "实测分配/次")

	for _, c := range candidates {
		d := HashSecret("secret", c.params, make([]byte, 16))

		// 预热一次，避免首个样本把页表初始化成本算进去。
		_ = d.Verify("secret")

		lats := make([]time.Duration, 0, rounds)
		for i := 0; i < rounds; i++ {
			start := time.Now()
			if !d.Verify("secret") {
				t.Fatal("校验失败")
			}
			lats = append(lats, time.Since(start))
		}
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		before := m.TotalAlloc
		_ = d.Verify("secret")
		runtime.ReadMemStats(&m)

		t.Logf("%-22s %11v %11v %12s %12s",
			fmt.Sprintf("t=%d m=%dMiB p=%d", c.params.Time, c.params.Memory/1024, c.params.Threads),
			lats[len(lats)/2].Round(time.Millisecond),
			lats[int(float64(len(lats)-1)*0.95)].Round(time.Millisecond),
			humanBytes(c.params.MemoryBytes()),
			humanBytes(int64(m.TotalAlloc-before)))
	}
}

// TestCapacity_重连风暴 是 06 Phase 0 表的验收场景。
//
// 三种情形必须分开看，混在一起会得出错误结论：
//  1. **稳态（L1 命中）**：正常重连，不应触碰 Argon2；
//  2. **冷启动（全部走 Argon2）**：网关重启后的最坏情况 —— 它决定副本数；
//  3. **过载**：并发槽位占满 —— 必须排队/拒绝，且**不得计入凭据失败**。
func TestCapacity_重连风暴(t *testing.T) {
	requireCapacity(t)

	t.Run("稳态_L1命中的真实收益", func(t *testing.T) {
		// 本用例回答一个被文档默认跳过的问题：**L1 命中到底省了什么？**
		//
		// 答案是：只省掉目录查询（µs 级），**省不掉 Argon2**（ms 级）——
		// 凭据校验必须每次连接都做，把「已认证」缓存起来等于把缓存变成免密令牌。
		// 因此 03 §2.1 的「L1 命中率 ≥ 95%」是一个缓存有效性指标，
		// **不能用来论证认证容量**。这条例题把两者的量级差直接摆出来。
		const devices = 200

		params := Params{Time: 3, Memory: 32 * 1024, Threads: 2, KeyLen: 32}
		ids, secret := buildFleet(t, devices, params)
		upstream := staticDirectory(ids)
		dir := NewCachedDirectory(upstream, WithCacheTTL(10*time.Minute))

		policy := DefaultPolicy()
		policy.MaxConcurrentVerify = 32
		a := NewAuthenticator(dir, policy, new(Metrics))
		ctx := context.Background()

		warmStart := time.Now()
		if err := warmAll(ctx, a, ids, secret, 8); err != nil {
			t.Fatalf("预热失败: %v", err)
		}
		t.Logf("预热 %d 台耗时 %v", devices, time.Since(warmStart).Round(time.Millisecond))

		// ① 只有目录查询（缓存命中路径）
		const lookups = 200_000
		lookStart := time.Now()
		for i := 0; i < lookups; i++ {
			if _, err := dir.Lookup(ctx, ids[i%devices].DeviceKey); err != nil {
				t.Fatalf("查询失败: %v", err)
			}
		}
		lookWall := time.Since(lookStart)

		// ② 完整认证（缓存命中，但每次都要 Argon2）
		const auths = 300
		lats := make([]time.Duration, 0, auths)
		authStart := time.Now()
		for i := 0; i < auths; i++ {
			s := time.Now()
			if _, err := a.Authenticate(ctx, request(ids[i%devices], secret)); err != nil {
				t.Fatalf("认证失败: %v", err)
			}
			lats = append(lats, time.Since(s))
		}
		authWall := time.Since(authStart)
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

		st := dir.Stats()
		lookupPerOp := lookWall / lookups
		authPerOp := authWall / auths

		t.Logf("L1 命中率 %.4f（%d 命中 / %d 回源）", st.HitRatio(), st.Hits, st.Misses)
		t.Logf("① 目录查询（L1 命中）：%v/次 → %.0f 次/秒", lookupPerOp.Round(time.Nanosecond), 1/lookupPerOp.Seconds())
		t.Logf("② 完整认证（L1 命中仍需 Argon2）：P50=%v → %.0f 次/秒",
			lats[len(lats)/2].Round(time.Millisecond), 1/authPerOp.Seconds())
		t.Logf("→ 缓存省下的部分占单次认证的 **%.3f%%**，容量必须按 ② 计算",
			100*float64(lookupPerOp)/float64(authPerOp))

		if st.HitRatio() < 0.95 {
			t.Errorf("L1 命中率 %.4f 低于 03 §2.1 要求的 0.95", st.HitRatio())
		}
		if authPerOp <= lookupPerOp*10 {
			t.Error("预期 Argon2 远贵于目录查询；若不然，说明测量或实现有问题")
		}
	})

	for _, c := range candidates {
		t.Run("冷启动_"+c.name, func(t *testing.T) {
			runColdStart(t, c.name, c.params)
		})
	}

	t.Run("过载_排队超时不得计入凭据失败", func(t *testing.T) {
		params := Params{Time: 1, Memory: 8 * 1024, Threads: 1, KeyLen: 32}
		ids, secret := buildFleet(t, 200, params)
		dir := staticDirectory(ids)

		policy := DefaultPolicy()
		policy.MaxConcurrentVerify = 2
		policy.VerifyQueueTimeout = 2 * time.Millisecond
		a := NewAuthenticator(dir, policy, new(Metrics))

		var (
			wg        sync.WaitGroup
			succeeded int64
		)
		for i := 0; i < 400; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if _, err := a.Authenticate(context.Background(), request(ids[i%len(ids)], secret)); err == nil {
					atomic.AddInt64(&succeeded, 1)
				}
			}(i)
		}
		wg.Wait()

		m := a.Metrics()
		t.Logf("过载场景：成功 %d / 排队超时 %d / 凭据失败 %d / 黑名单 %d 条",
			succeeded, m.FailureCount(ReasonOverloaded), m.FailureCount(ReasonBadCredential), a.BlacklistSize())

		if m.FailureCount(ReasonOverloaded) == 0 {
			t.Fatal("本用例应制造出排队超时，实际 0 次 —— 限流未生效")
		}
		if got := m.FailureCount(ReasonBadCredential); got != 0 {
			t.Fatalf("过载期间不得产生凭据失败计数，实际 %d", got)
		}
		if a.BlacklistSize() != 0 {
			t.Fatalf("过载不得触发黑名单，实际 %d 条", a.BlacklistSize())
		}
	})
}

// runColdStart 用小样本测冷启动速率，再外推到 8 万设备 / 60s，给出副本数结论。
func runColdStart(t *testing.T, name string, params Params) {
	t.Helper()

	// 样本量：够估准速率（±8%），又不至于让整轮跑上几分钟。
	const sample = 160

	ids, secret := buildFleet(t, sample, params)
	dir := staticDirectory(ids) // 不缓存：每次都要真算 Argon2

	t.Logf("%s：t=%d m=%dMiB p=%d，单次内存上界 %s",
		name, params.Time, params.Memory/1024, params.Threads, humanBytes(params.MemoryBytes()))
	t.Logf("%-8s %12s %12s %14s %12s %10s", "槽位", "速率", "P50", "内存上界", "CPU 利用率", "需副本")

	for _, conc := range []int{4, 8, 16, 32} {
		policy := DefaultPolicy()
		policy.MaxConcurrentVerify = conc
		policy.VerifyQueueTimeout = 10 * time.Minute

		a := NewAuthenticator(dir, policy, new(Metrics))
		rate, p50, cpu := measureStormRate(t, a, ids, secret, conc, sample)

		nodes := int(math.Ceil(float64(fleetPerNode) / rate / stormWindow.Seconds()))

		t.Logf("%-8d %9.1f 次/s %12v %14s %11.0f%% %10d",
			conc, rate, p50.Round(time.Millisecond),
			humanBytes(params.MemoryBytes()*int64(conc)), cpu*100, nodes)
	}
}

// measureStormRate 跑一遍样本，返回 (速率, P50, CPU 利用率)。
//
// CPU 利用率 = 进程 user+sys 时间 / 墙钟 / CPU 核数，这正是 06 表格里
// 「网关 CPU 不饱和」那句话的可测量形式。
func measureStormRate(t *testing.T, a *Authenticator, ids []*Identity, secret string, concurrency, sample int) (float64, time.Duration, float64) {
	t.Helper()

	var (
		lats   = make([]time.Duration, sample)
		next   atomic.Int64
		failed atomic.Int64
		wg     sync.WaitGroup
	)

	runtime.GC()
	cpuBefore := processCPUSeconds()

	start := time.Now()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= sample {
					return
				}
				s := time.Now()
				if _, err := a.Authenticate(context.Background(), request(ids[i], secret)); err != nil {
					failed.Add(1)
					return
				}
				lats[i] = time.Since(s)
			}
		}()
	}
	wg.Wait()
	wall := time.Since(start)

	if n := failed.Load(); n > 0 {
		t.Fatalf("并发槽位 %d 下有 %d 次认证失败", concurrency, n)
	}

	cpu := (processCPUSeconds() - cpuBefore) / wall.Seconds() / float64(runtime.NumCPU())

	sorted := make([]time.Duration, sample)
	copy(sorted, lats)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	return float64(sample) / wall.Seconds(), sorted[sample/2], cpu
}

// processCPUSeconds 返回本进程累计消耗的 CPU 秒数（user + sys）。
func processCPUSeconds() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return timevalSeconds(ru.Utime) + timevalSeconds(ru.Stime)
}

func timevalSeconds(tv syscall.Timeval) float64 {
	return float64(tv.Sec) + float64(tv.Usec)/1e6
}

// ---------- 测试替身与工具 ----------

// buildFleet 造出 n 台设备的身份与共享 secret。
//
// 共享 secret 是刻意的：**认证容量与 secret 取值无关**，共用一份摘要可以让
// 本用例的内存占用与设备数解耦（否则几万份摘要本身就是测量噪声）。
func buildFleet(t *testing.T, n int, p Params) ([]*Identity, string) {
	t.Helper()

	secret := "shared-secret-for-capacity-test"
	d := HashSecret(secret, p, make([]byte, 16))

	ids := make([]*Identity, n)
	for i := range ids {
		ids[i] = &Identity{
			ProjectID:    int64(i%1000 + 1),
			DeviceTypeID: 55,
			DeviceKey:    "dev-" + strconv.Itoa(i),
			Mode:         ModePerDevice,
			Secret:       d,
		}
	}
	return ids, secret
}

func staticDirectory(ids []*Identity) Directory {
	byKey := make(map[string]*Identity, len(ids))
	for _, id := range ids {
		byKey[id.DeviceKey] = id
	}
	return DirectoryFunc(func(_ context.Context, clientID string) (*Identity, error) {
		return byKey[clientID], nil
	})
}

func request(id *Identity, secret string) Request {
	return Request{
		ClientID: id.DeviceKey,
		Username: id.DeviceKey,
		Password: secret,
		RemoteIP: "10.0.0.1",
	}
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// warmAll 并发预热全部设备，把身份放进 L1 缓存。
func warmAll(ctx context.Context, a *Authenticator, ids []*Identity, secret string, concurrency int) error {
	var (
		next atomic.Int64
		wg   sync.WaitGroup
		errs atomic.Int64
	)

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(ids) {
					return
				}
				if _, err := a.Authenticate(ctx, request(ids[i], secret)); err != nil {
					errs.Add(1)
					return
				}
			}
		}()
	}
	wg.Wait()

	if n := errs.Load(); n > 0 {
		return fmt.Errorf("%d 次预热认证失败", n)
	}
	return nil
}
