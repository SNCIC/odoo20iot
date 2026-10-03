package ruleengine

import "github.com/SNCIC/odoo20iot/internal/dag"

func (e *Engine) Registry() (*dag.Registry, error) { return dag.NewRegistry() }
