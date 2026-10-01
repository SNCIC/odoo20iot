package pg_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/SNCIC/odoo20iot/internal/pg/pgtest"
)

func TestMigrateAppliesThenNoOp(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.EmptyDB(t)

	applied, err := pg.Migrate(ctx, pool)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if len(applied) == 0 {
		t.Fatal("首次应应用至少一个迁移")
	}
	if applied[0] != "0001_alarm_active" {
		t.Fatalf("首个迁移应为 0001_alarm_active，得 %v", applied)
	}

	// 幂等：再跑一次不应重复应用（多副本启动、服务重启都会走到这里）。
	again, err := pg.Migrate(ctx, pool)
	if err != nil {
		t.Fatalf("二次 Migrate: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("二次应无迁移可应用，得 %v", again)
	}
}

func TestMigrateCreatesSchemaWithConstraints(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.EmptyDB(t)
	if _, err := pg.Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	var idx int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_indexes WHERE tablename = 't_alarm_active'`).Scan(&idx); err != nil {
		t.Fatalf("查索引: %v", err)
	}
	if idx < 4 {
		t.Fatalf("索引数不足：%d（主键 + 扫描/父子/租户三条）", idx)
	}

	insert := func(key, state, lastExpr string) error {
		// 刻意不传 id：要验证的正是 DEFAULT nextval 生效（在 Go 侧造 id
		// 才会出现「两个实例撞同一个 id」）。
		_, err := pool.Exec(ctx, `
			INSERT INTO t_alarm_active
			  (dedup_key, project_id, device_id, rule_id, level, state,
			   first_ts, last_ts, state_ts)
			VALUES ($1, 'p1', 'd1', 'ar_001', 'warn', $2, now(), `+lastExpr+`, now())`,
			key, state)
		return err
	}

	// 合法状态放行。
	if err := insert("k-1", "detected", "now()"); err != nil {
		t.Fatalf("合法状态被拒: %v", err)
	}
	// ⚠️ 拼错的 state 必须被拒：否则这条告警既不在 detected 也不在 active，
	// 扫描永远看不见它 —— 「告警安静地消失」。
	if err := insert("k-2", "typo", "now()"); err == nil {
		t.Fatal("非法 state 应被 ck_alarm_state 拒绝")
	}
	// ⚠️ 时间倒挂必须被拒：否则报表里会出现「确认时长为负」，倒查不到写入点。
	if err := insert("k-3", "confirmed", "now() - interval '1 hour'"); err == nil {
		t.Fatal("last_ts 早于 first_ts 应被 ck_alarm_ts_order 拒绝")
	}
	// id 由数据库生成并格式化为文本。
	var id string
	if err := pool.QueryRow(ctx, `SELECT id FROM t_alarm_active WHERE dedup_key = 'k-1'`).Scan(&id); err != nil {
		t.Fatalf("读 id: %v", err)
	}
	if !strings.HasPrefix(id, "alarm-") {
		t.Fatalf("id 应由 t_alarm_id_seq 生成，得 %q", id)
	}
}

func TestMigrateDetectsChecksumDrift(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.EmptyDB(t)
	if _, err := pg.Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// 篡改库中记录的校验和，模拟「有人改了已应用的迁移文件」。
	if _, err := pool.Exec(ctx,
		`UPDATE schema_migrations SET checksum = 'deadbeef' WHERE version = '0001_alarm_active'`); err != nil {
		t.Fatalf("篡改校验和: %v", err)
	}
	_, err := pg.Migrate(ctx, pool)
	if err == nil {
		t.Fatal("校验和变了却放行 —— 这种漂移会让本地与线上的 schema 悄悄分叉")
	}
	if !strings.Contains(err.Error(), "校验和") {
		t.Fatalf("错误信息应点明校验和，得 %v", err)
	}
}

func TestMigrateIsSerializedByAdvisoryLock(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.EmptyDB(t)

	// 并发跑迁移：advisory lock 应让它们串行，结果一致且无报错
	// （多副本同时启动就是这个场景）。
	const n = 4
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := pg.Migrate(ctx, pool)
			errs <- err
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("并发迁移出错: %v", err)
		}
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&rows); err != nil {
		t.Fatalf("查版本表: %v", err)
	}
	if rows != 1 {
		t.Fatalf("版本表应只有 1 行（迁移被重复记录），得 %d", rows)
	}
}

func TestOpenRejectsUnreachableDSN(t *testing.T) {
	// 真 PG 上 Ping 可用，故这里必须真的失败（端口 1 上没有 PG）。
	_, err := pg.Open(context.Background(),
		pg.Config{DSN: "postgres://nobody@127.0.0.1:1/none", ConnectTimeout: time.Second})
	if err == nil {
		t.Fatal("连不上却返回成功 —— DSN 写错会被拖到第一次业务查询才暴露")
	}
}
