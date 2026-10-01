package rollup

import (
	"testing"
	"time"
)

func at(h, m int) time.Time {
	return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC)
}

// TestAlignFloor 校验向下对齐（与 date_bin 的整数倍窗口一致）。
func TestAlignFloor(t *testing.T) {
	in := at(12, 34).Add(37 * time.Second)
	if got := AlignFloor(in, time.Minute); !got.Equal(at(12, 34)) {
		t.Errorf("1m 对齐期望 %s，得到 %s", at(12, 34), got)
	}
	if got := AlignFloor(in, time.Hour); !got.Equal(at(12, 0)) {
		t.Errorf("1h 对齐期望 %s，得到 %s", at(12, 0), got)
	}
}

// TestClosedEnd 校验「滞后 lag 之后才认为窗口闭合」。
func TestClosedEnd(t *testing.T) {
	now := at(12, 0).Add(90 * time.Second) // 12:01:30
	if got := ClosedEnd(now, time.Minute, 2*time.Minute); !got.Equal(at(11, 59)) {
		t.Errorf("期望 11:59，得到 %s", got)
	}
	// lag 让「刚好结束」的窗口也不算闭合，给迟到点留余地。
	if got := ClosedEnd(now, time.Minute, 0); !got.Equal(at(12, 1)) {
		t.Errorf("lag=0 期望 12:01，得到 %s", got)
	}
}

// TestInitialWatermark 校验首次运行的水位是有界的（不回看到最早数据）。
func TestInitialWatermark(t *testing.T) {
	now := at(12, 0)
	// now - lag(2m) - lookback(1h) = 10:58
	got := InitialWatermark(now, time.Minute, 2*time.Minute, time.Hour)
	if !got.Equal(at(10, 58)) {
		t.Fatalf("期望 10:58，得到 %s", got)
	}
}

// TestWindowsToProcess 校验窗口平铺：半开、不重不漏、受 max 约束、追平即空。
func TestWindowsToProcess(t *testing.T) {
	base := at(12, 0)

	t.Run("平铺三个窗口", func(t *testing.T) {
		ws := WindowsToProcess(base, base.Add(3*time.Minute), time.Minute, 10)
		if len(ws) != 3 {
			t.Fatalf("期望 3 个窗口，得到 %d", len(ws))
		}
		for i, w := range ws {
			if !w.Start.Equal(base.Add(time.Duration(i) * time.Minute)) {
				t.Errorf("第 %d 个窗口起点 %s 不符", i, w.Start)
			}
			if w.End.Sub(w.Start) != time.Minute {
				t.Errorf("第 %d 个窗口宽度 %s 不符", i, w.End.Sub(w.Start))
			}
		}
		// 相邻窗口必须首尾相接（半开 ⇒ 不重不漏）。
		for i := 1; i < len(ws); i++ {
			if !ws[i].Start.Equal(ws[i-1].End) {
				t.Fatalf("窗口 %d 与 %d 不相接", i-1, i)
			}
		}
	})

	t.Run("半个窗口不计入", func(t *testing.T) {
		ws := WindowsToProcess(base, base.Add(2*time.Minute+30*time.Second), time.Minute, 10)
		if len(ws) != 2 {
			t.Fatalf("closedEnd 落在窗口中间时应只算完整的 2 个，得到 %d", len(ws))
		}
	})

	t.Run("受 max 约束", func(t *testing.T) {
		ws := WindowsToProcess(base, base.Add(10*time.Minute), time.Minute, 3)
		if len(ws) != 3 {
			t.Fatalf("max=3 应最多 3 个，得到 %d", len(ws))
		}
	})

	t.Run("已追平返回空", func(t *testing.T) {
		if ws := WindowsToProcess(base, base, time.Minute, 10); ws != nil {
			t.Fatalf("watermark==closedEnd 应为空，得到 %d", len(ws))
		}
		if ws := WindowsToProcess(base.Add(time.Minute), base, time.Minute, 10); ws != nil {
			t.Fatalf("watermark 超前 closedEnd 应为空，得到 %d", len(ws))
		}
	})

	t.Run("非法参数返回空", func(t *testing.T) {
		if ws := WindowsToProcess(base, base.Add(time.Hour), 0, 10); ws != nil {
			t.Fatalf("width=0 应为空，得到 %d", len(ws))
		}
		if ws := WindowsToProcess(base, base.Add(time.Hour), time.Minute, 0); ws != nil {
			t.Fatalf("max=0 应为空，得到 %d", len(ws))
		}
	})
}
