package rollup

import "time"

// 本文件的函数**不碰数据库**，全部是纯时间算术 —— 调度逻辑最容易出错的地方
// （少算/多算一个窗口、重复物化、卡在边界）都在这里可确定性地单测。

// Window 是一个半开窗口 [Start, End)。
type Window struct {
	Start time.Time
	End   time.Time
}

// AlignFloor 把时刻向下对齐到 width 的整数倍（与 GreptimeDB `date_bin` 的 epoch 对齐一致）。
func AlignFloor(t time.Time, width time.Duration) time.Time {
	if width <= 0 {
		return t
	}
	return t.Truncate(width)
}

// ClosedEnd 返回「已闭合窗口」的上界：只有 End <= ClosedEnd 的窗口才允许物化。
//
// lag 是抗乱序/迟到的安全边界 —— 原始表允许乱序写入（02 §4.2），
// 若把「刚好结束」的窗口立刻物化，稍后到达的迟到点就会永久丢在聚合之外。
func ClosedEnd(now time.Time, width, lag time.Duration) time.Time {
	return AlignFloor(now.Add(-lag), width)
}

// InitialWatermark 是账本无记录时的起点：now - lag - lookback 向下对齐。
//
// 刻意**不**回看到最早数据（`MIN(ts)`）：那会让首次启动就尝试物化全部历史，
// 在真实体量下把服务打挂。更早的历史需要显式 `-backfill-since`。
func InitialWatermark(now time.Time, width, lag, lookback time.Duration) time.Time {
	return AlignFloor(now.Add(-lag).Add(-lookback), width)
}

// WindowsToProcess 从 watermark 起平铺出所有 End <= closedEnd 的窗口，最多 max 个。
//
// 返回空切片表示已追平（没有可处理的窗口）。watermark 需自行保证已对齐；
// 未对齐时最多多算一个「半截」窗口，但不会重复或漏算（窗口边界由 [start,end) 决定）。
func WindowsToProcess(watermark, closedEnd time.Time, width time.Duration, max int) []Window {
	if width <= 0 || max <= 0 || !watermark.Before(closedEnd) {
		return nil
	}

	out := make([]Window, 0, max)
	for start := watermark; len(out) < max; start = start.Add(width) {
		end := start.Add(width)
		if end.After(closedEnd) {
			break
		}
		out = append(out, Window{Start: start, End: end})
	}
	return out
}
