package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/SNCIC/odoo20iot/internal/notify"
	"github.com/SNCIC/odoo20iot/internal/notifyconfig"
	"github.com/jackc/pgx/v5/pgxpool"
)

type dbPolicySource struct {
	pool      *pgxpool.Pool
	fallback  notify.Policy
	endpoints *notifyconfig.Store
}

func newDBPolicySource(pool *pgxpool.Pool, fallback notify.Policy, endpoints *notifyconfig.Store) notify.PolicySource {
	return &dbPolicySource{pool: pool, fallback: fallback, endpoints: endpoints}
}

func (s *dbPolicySource) Resolve(ctx context.Context, req notify.Request) (notify.Policy, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT notify FROM t_alarm_rule WHERE project_id=$1 AND rule_id=$2 AND enabled ORDER BY version DESC LIMIT 1`, req.ProjectID, req.RuleID).Scan(&raw)
	if err != nil {
		fallbackErr := s.pool.QueryRow(ctx, `SELECT notify FROM t_alarm_rule WHERE project_id=$1 AND rule_id='' AND enabled ORDER BY version DESC LIMIT 1`, req.ProjectID).Scan(&raw)
		if fallbackErr != nil {
			return s.fallback, nil
		}
	}
	var spec policySpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return notify.Policy{}, fmt.Errorf("解析数据库通知策略: %w", err)
	}
	p := spec.toPolicy()
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
		rows, err := s.pool.Query(ctx, `SELECT email FROM t_user WHERE project_id=$1 AND status='active' AND role_key = ANY($2) AND email <> ''`, req.ProjectID, p.Groups)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var email string
				if rows.Scan(&email) == nil {
					p.Recipients[notify.ChannelEmail] = append(p.Recipients[notify.ChannelEmail], email)
				}
			}
		}
	}
	for _, channel := range p.Channels {
		if s.endpoints != nil {
			if targets, endpointErr := s.endpoints.Targets(ctx, projectID(req.ProjectID), channel); endpointErr == nil && len(targets) > 0 {
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
	var result int64
	for _, char := range value {
		if char < '0' || char > '9' {
			return 0
		}
		result = result*10 + int64(char-'0')
	}
	return result
}

func (s *dbPolicySource) String() string { return strings.TrimSpace("database") }
