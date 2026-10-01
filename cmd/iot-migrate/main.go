// Command iot-migrate 应用业务库（PostgreSQL）的增量迁移。
//
// 为什么单独成命令、而不是塞进各服务的启动流程：迁移是**部署动作**，
// 需要能单独跑、单独看结果。服务启动时顺手迁移在单实例下没问题，
// 多副本并发上线时就成了「谁先起来谁改 schema」的竞态（虽然有 advisory lock
// 兜着不会真的坏），更要紧的是——把建表藏在某个服务的启动日志里，
// 出问题时最难找的就是「到底哪次部署改的表」。
//
// 刻意**只做 up**、不做自动 down：回滚是人的决定，写成一个会丢数据的
// 自动步骤只会让它在某个深夜被误触发。
//
// 用法（devbox 内）：
//
//	go run ./cmd/iot-migrate
//	go run ./cmd/iot-migrate -dsn 'postgres://user:pass@host:5432/iot'
//	go run ./cmd/iot-migrate -check   # 只报告是否有待应用迁移，不改库（可用于 CI 门禁）
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/SNCIC/odoo20iot/internal/pg"
)

func main() {
	var (
		dsn     string
		check   bool
		timeout time.Duration
	)
	flag.StringVar(&dsn, "dsn", pg.DefaultDSN, "业务库 DSN")
	flag.BoolVar(&check, "check", false, "只检查是否有待应用的迁移，不改库")
	flag.DurationVar(&timeout, "timeout", 60*time.Second, "整体超时")
	flag.Parse()

	if err := run(dsn, check, timeout); err != nil {
		fmt.Fprintln(os.Stderr, "iot-migrate:", err)
		os.Exit(1)
	}
}

func run(dsn string, check bool, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	migs, err := pg.LoadMigrations()
	if err != nil {
		return err
	}

	pool, err := pg.Open(ctx, pg.Config{DSN: dsn})
	if err != nil {
		return err
	}
	defer pool.Close()

	if check {
		// 待应用集合 = 文件里有、库版本表里没有（或校验和不同的）。
		applied, err := pg.AppliedVersions(ctx, pool)
		if err != nil {
			return err
		}
		pending := 0
		for _, m := range migs {
			if sum, ok := applied[m.Version]; !ok {
				fmt.Printf("待应用: %s\n", m.Version)
				pending++
			} else if sum != m.Checksum {
				return fmt.Errorf("迁移 %s 的校验和与库中不符（库 %s / 文件 %s）："+
					"已应用的迁移不可修改，请新增一个", m.Version, sum, m.Checksum)
			}
		}
		if pending > 0 {
			return fmt.Errorf("有 %d 个迁移待应用", pending)
		}
		fmt.Printf("已是最新（%d 个迁移）\n", len(migs))
		return nil
	}

	done, err := pg.Migrate(ctx, pool)
	if err != nil {
		return err
	}
	if len(done) == 0 {
		fmt.Printf("无待应用迁移（共 %d 个）\n", len(migs))
		return nil
	}
	for _, v := range done {
		fmt.Printf("已应用: %s\n", v)
	}
	return nil
}
