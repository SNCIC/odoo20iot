package alarm

import (
	"testing"
	"time"
)

func TestSilenceCoversScopes(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := &Alarm{ProjectID: "p1", DeviceID: "d1", DeviceTypeID: 55, RuleID: "ar_001"}

	cases := []struct {
		name string
		w    Silence
		want bool
	}{
		{"零值即不限", Silence{}, true},
		{"命中设备", Silence{DeviceID: "d1"}, true},
		{"设备不符", Silence{DeviceID: "d2"}, false},
		{"命中机型", Silence{DeviceTypeID: 55}, true},
		{"机型不符", Silence{DeviceTypeID: 56}, false},
		{"命中规则", Silence{RuleID: "ar_001"}, true},
		{"规则不符", Silence{RuleID: "ar_002"}, false},
		{"租户不符", Silence{ProjectID: "p2"}, false},
		{"窗口未开始", Silence{Start: base.Add(time.Minute)}, false},
		{"窗口已结束", Silence{End: base}, false},
	}
	for _, c := range cases {
		if got := c.w.Covers(a, base); got != c.want {
			t.Errorf("%s: Covers=%v，期望 %v", c.name, got, c.want)
		}
	}
}

func TestSilencesCoveringReturnsFirstMatch(t *testing.T) {
	s := NewSilences()
	s.Add(Silence{ID: "w1", DeviceTypeID: 99, Reason: "别的机型"})
	s.Add(Silence{ID: "w2", DeviceID: "d1", Reason: "计划检修"})
	got, ok := s.Covering(&Alarm{ProjectID: "p1", DeviceID: "d1"}, time.Now())
	if !ok || got.ID != "w2" {
		t.Fatalf("应命中 w2，得 %+v / %v", got, ok)
	}
}

func TestStormGuardTripsOnceThenHolds(t *testing.T) {
	g := newStormGuard(StormConfig{Threshold: 3, Window: time.Minute})
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 1; i <= 3; i++ {
		if s := g.hit("p1", at); s != StormOK {
			t.Fatalf("第 %d 条不该熔断（阈值 3），得 %v", i, s)
		}
	}
	if s := g.hit("p1", at); s != StormTripped {
		t.Fatalf("第 4 条应熔断，得 %v", s)
	}
	// 04 §2.2 要求「只发一条告警风暴聚合通知」：后续必须 Holding 而不是再 Tripped。
	for i := 0; i < 5; i++ {
		if s := g.hit("p1", at); s != StormHolding {
			t.Fatalf("熔断窗口内应保持 Holding，得 %v", s)
		}
	}
	// 风暴按租户隔离。
	if s := g.hit("p2", at); s != StormOK {
		t.Fatalf("另一租户不该被牵累，得 %v", s)
	}
	// 窗口过后重新计数。
	if s := g.hit("p1", at.Add(2*time.Minute)); s != StormOK {
		t.Fatalf("熔断窗口过后应重新计数，得 %v", s)
	}
}

func TestStormStateDoesNotCount(t *testing.T) {
	g := newStormGuard(StormConfig{Threshold: 1, Window: time.Minute})
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// state 是只读的：调 100 次也不该把阈值撑爆 ——
	// 否则抑制判定会把一次风暴数两遍。
	for i := 0; i < 100; i++ {
		if s := g.state("p1", at); s != StormOK {
			t.Fatalf("state 不该计数，得 %v", s)
		}
	}
	if s := g.hit("p1", at); s != StormOK {
		t.Fatalf("第 1 条不该熔断，得 %v", s)
	}
	if s := g.hit("p1", at); s != StormTripped {
		t.Fatalf("第 2 条应熔断，得 %v", s)
	}
	if s := g.state("p1", at); s != StormHolding {
		t.Fatalf("熔断后 state 应报 Holding，得 %v", s)
	}
}

func TestAggregatorCountsDistinctDevices(t *testing.T) {
	ag := newAggregator(AggregatorConfig{Threshold: 3, Window: time.Minute})
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const g = "g"

	// 同一台设备刷 5 次不该被当成 5 台设备 ——
	// 那会把「一台设备抖动」误判成「批量故障」，是聚合最容易写错的地方。
	for i := 0; i < 5; i++ {
		n, _, batched := ag.add(g, "d1", at.Add(time.Duration(i)*time.Second))
		if n != 1 || batched {
			t.Fatalf("同设备重复：n=%d batched=%v", n, batched)
		}
	}
	if _, _, batched := ag.add(g, "d2", at.Add(10*time.Second)); batched {
		t.Fatal("2 台不该达到阈值 3")
	}
	n, id, batched := ag.add(g, "d3", at.Add(11*time.Second))
	if !batched || id == "" || n != 3 {
		t.Fatalf("3 台不同设备应聚合：n=%d id=%q batched=%v", n, id, batched)
	}

	// 窗口外的旧事件必须裁剪，否则一小时前的告警会永远拖累判定。
	n, _, batched = ag.add(g, "d9", at.Add(2*time.Minute))
	if n != 1 || batched {
		t.Fatalf("窗口外事件应被裁剪：n=%d batched=%v", n, batched)
	}
}

func TestSuppressPrefix(t *testing.T) {
	if got := SuppressPrefix("静默窗口：计划检修"); got != "静默窗口" {
		t.Fatalf("得 %q", got)
	}
	if got := SuppressPrefix("根因抑制：父告警 alarm-1 处于 active"); got != "根因抑制" {
		t.Fatalf("得 %q", got)
	}
	if got := SuppressPrefix("告警风暴"); got != "告警风暴" {
		t.Fatalf("无分隔符时应原样返回，得 %q", got)
	}
}
