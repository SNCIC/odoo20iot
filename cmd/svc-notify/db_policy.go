package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/SNCIC/odoo20iot/internal/notify"
	"github.com/SNCIC/odoo20iot/internal/notifyconfig"
	"github.com/SNCIC/odoo20iot/internal/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type dbPolicySource struct {
	pool      *pgxpool.Pool
	fallback  notify.Policy
	endpoints *notifyconfig.Store
	logger    *slog.Logger
}

func newDBPolicySource(pool *pgxpool.Pool, fallback notify.Policy, endpoints *notifyconfig.Store) notify.PolicySource {
	return &dbPolicySource{pool: pool, fallback: fallback, endpoints: endpoints, logger: slog.Default()}
}

func (s *dbPolicySource) Resolve(ctx context.Context, req notify.Request) (notify.Policy, error) {
	var raw []byte
	project := projectID(req.ProjectID)
	if project <= 0 {
		return s.fallback, fmt.Errorf("通知策略 project_id 非法: %q", req.ProjectID)
	}
	var queryErr error
	err := pg.WithProjectTx(ctx, s.pool, project, func(ctx context.Context, tx pgx.Tx) error {
		queryErr = tx.QueryRow(ctx, `SELECT notify FROM t_alarm_rule WHERE project_id=$1 AND rule_id=$2 AND enabled ORDER BY version DESC LIMIT 1`, req.ProjectID, req.RuleID).Scan(&raw)
		if errors.Is(queryErr, pgx.ErrNoRows) {
			queryErr = tx.QueryRow(ctx, `SELECT notify FROM t_alarm_rule WHERE project_id=$1 AND rule_id='' AND enabled ORDER BY version DESC LIMIT 1`, req.ProjectID).Scan(&raw)
		}
		if errors.Is(queryErr, pgx.ErrNoRows) {
			return nil
		}
		return queryErr
	})
	if err != nil {
		return notify.Policy{}, fmt.Errorf("读取租户通知策略: %w", err)
	}
	p := s.fallback
	if queryErr != nil && !errors.Is(queryErr, pgx.ErrNoRows) {
		return notify.Policy{}, fmt.Errorf("读取通知策略: %w", queryErr)
	}
	if errors.Is(queryErr, pgx.ErrNoRows) {
		if s.logger != nil {
			s.logger.Info("未找到数据库通知策略，使用默认策略", "project_id", project, "rule_id", req.RuleID)
		}
	} else {
		var spec policySpec
		if err := json.Unmarshal(raw, &spec); err != nil {
			return notify.Policy{}, fmt.Errorf("解析数据库通知策略: %w", err)
		}
		p = spec.toPolicy()
	}
	if p.Recipients != nil {
		copyRecipients := make(map[string][]string, len(p.Recipients))
		for channel, recipients := range p.Recipients {
			copyRecipients[channel] = append([]string(nil), recipients...)
		}
		p.Recipients = copyRecipients
	}
	if len(p.Channels) == 0 {
		p.Channels = s.fallback.Channels
	}
	if p.Template == "" {
		p.Template = s.fallback.Template
	}
	if p.Recipients == nil {
		p.Recipients = map[string][]string{}
	}
	if len(p.Groups) > 0 {
		err := pg.WithProjectTx(ctx, s.pool, project, func(ctx context.Context, tx pgx.Tx) error {
			rows, err := tx.Query(ctx, `SELECT email FROM t_user WHERE project_id=$1 AND status='active' AND role_key = ANY($2) AND email <> ''`, project, p.Groups)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var email string
				if err := rows.Scan(&email); err != nil {
					return err
				}
				p.Recipients[notify.ChannelEmail] = append(p.Recipients[notify.ChannelEmail], email)
			}
			return rows.Err()
		})
		if err != nil {
			return notify.Policy{}, fmt.Errorf("展开通知组: %w", err)
		}
	}
	for _, channel := range p.Channels {
		if s.endpoints != nil {
			targets, endpointErr := s.endpoints.Targets(ctx, project, channel)
			if endpointErr != nil {
				return notify.Policy{}, fmt.Errorf("读取通知端点: %w", endpointErr)
			}
			if len(targets) > 0 {
				p.Recipients[channel] = targets
			}
		}
		if len(p.Recipients[channel]) == 0 && len(s.fallback.Recipients[channel]) > 0 {
			p.Recipients[channel] = s.fallback.Recipients[channel]
		}
	}
	return p, nil
}

func projectID(value string) int64 {
	result, err := strconv.ParseInt(value, 10, 64)
	if err != nil || result <= 0 {
		return 0
	}
	return result
}

func (s *dbPolicySource) String() string { return strings.TrimSpace("database") }
