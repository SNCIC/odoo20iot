package main

import (
	"context"
	"fmt"

	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/jackc/pgx/v5/pgxpool"
)

func pendingMigrations(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	migs, err := pg.LoadMigrations()
	if err != nil {
		return nil, err
	}
	applied, err := pg.AppliedVersions(ctx, pool)
	if err != nil {
		return nil, err
	}
	var pending []string
	for _, migration := range migs {
		if _, ok := applied[migration.Version]; !ok {
			pending = append(pending, migration.Version)
		}
	}
	if len(pending) > 0 {
		return pending, nil
	}
	if len(applied) == 0 {
		return nil, fmt.Errorf("数据库尚未建立迁移版本")
	}
	return nil, nil
}
