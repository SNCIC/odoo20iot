package pg_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/SNCIC/odoo20iot/internal/pg/pgtest"
)

func currentDB(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var name string
	if err := pool.QueryRow(context.Background(), "SELECT current_database()").Scan(&name); err != nil {
		t.Fatalf("取当前库名: %v", err)
	}
	return name
}

func TestTryLockExcludesOtherSessions(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.DB(t)
	key := pg.AdvisoryKey("test:scan")

	first, ok, err := pg.TryLock(ctx, pool, key)
	if err != nil || !ok {
		t.Fatalf("第一把锁应拿到，得 ok=%v err=%v", ok, err)
	}

	// 用另一个连接池（同一库）模拟另一个副本：同一把键的第二把锁拿不到 ——
	// 这正是「多副本只让一个扫」的机制。
	other := pgtest.OpenDB(t, pgtest.DSN(t), currentDB(t, pool))
	_, ok2, err := pg.TryLock(ctx, other, key)
	if err != nil {
		t.Fatalf("第二把锁不该报错（拿不到是正常情况，不是错误），得 %v", err)
	}
	if ok2 {
		t.Fatal("同一键的第二把锁不该拿到 —— 否则多副本会重复扫描全量数据（04 §2.5）")
	}

	if err := first.Unlock(ctx); err != nil {
		t.Fatalf("解锁: %v", err)
	}
	if first.Held() {
		t.Fatal("解锁后不应仍标记持有")
	}

	// 释放后应能重新拿到：验证锁没泄漏（会话级锁最容易出的问题就是取到放不掉）。
	second, ok3, err := pg.TryLock(ctx, other, key)
	if err != nil || !ok3 {
		t.Fatalf("释放后应能重新拿到，得 ok=%v err=%v", ok3, err)
	}
	if err := second.Unlock(ctx); err != nil {
		t.Fatalf("解锁: %v", err)
	}
}

func TestUnlockWorksAfterContextCanceled(t *testing.T) {
	pool := pgtest.DB(t)
	key := pg.AdvisoryKey("test:cancel")

	// 用会被取消的 ctx 取锁，再在 ctx 已取消的情况下解锁：
	// 若解锁跟着 ctx 一起放弃，锁会挂在连接上直到池回收，
	// 期间所有实例都扫不上 —— 而且没有任何报错。
	ctx, cancel := context.WithCancel(context.Background())
	lock, ok, err := pg.TryLock(ctx, pool, key)
	if err != nil || !ok {
		t.Fatalf("取锁: ok=%v err=%v", ok, err)
	}
	cancel()
	if err := lock.Unlock(ctx); err != nil {
		t.Fatalf("ctx 已取消也必须解锁成功，得 %v", err)
	}

	again, ok2, err := pg.TryLock(context.Background(), pool, key)
	if err != nil || !ok2 {
		t.Fatalf("解锁后应能立刻重新拿到，得 ok=%v err=%v", ok2, err)
	}
	if err := again.Unlock(context.Background()); err != nil {
		t.Fatalf("解锁: %v", err)
	}
}

func TestUnlockIsIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.DB(t)
	lock, ok, err := pg.TryLock(ctx, pool, pg.AdvisoryKey("test:idem"))
	if err != nil || !ok {
		t.Fatalf("取锁: ok=%v err=%v", ok, err)
	}
	for i := 0; i < 3; i++ {
		if err := lock.Unlock(ctx); err != nil {
			t.Fatalf("第 %d 次解锁不该报错（重复解锁要安全）：%v", i+1, err)
		}
	}
	var nilLock *pg.Lock
	if err := nilLock.Unlock(ctx); err != nil {
		t.Fatalf("nil 锁解锁应安全，得 %v", err)
	}
}

func TestAdvisoryKeyIsStableAndDistinct(t *testing.T) {
	if pg.AdvisoryKey("a") != pg.AdvisoryKey("a") {
		t.Fatal("同一输入必须得到同一个键，否则锁根本对不上")
	}
	if pg.AdvisoryKey("scan:alarm") == pg.AdvisoryKey("scan:reconcile") {
		t.Fatal("不同用途应得到不同键，否则会互相阻塞")
	}
	if pg.AdvisoryKey("") == pg.AdvisoryKey("x") {
		t.Fatal("空串不该与普通输入撞键")
	}
}
