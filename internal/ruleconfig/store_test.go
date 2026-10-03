package ruleconfig

import "testing"

func TestMatchContract(t *testing.T) {
	if (Match{Expr: "msg.temperature > 60"}).Expr == "" {
		t.Fatal("match contract invalid")
	}
}
