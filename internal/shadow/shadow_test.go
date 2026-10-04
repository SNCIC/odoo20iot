package shadow

import "testing"

func TestMergeAndDelta(t *testing.T) {
	base := map[string]any{"mode": "auto", "limits": map[string]any{"high": 80.0, "low": 10.0}}
	merged := Merge(base, map[string]any{"limits": map[string]any{"high": 85.0}, "enabled": true})
	if merged["mode"] != "auto" || merged["enabled"] != true {
		t.Fatalf("merge lost fields: %#v", merged)
	}
	limits := merged["limits"].(map[string]any)
	if limits["high"] != 85.0 || limits["low"] != 10.0 {
		t.Fatalf("nested merge failed: %#v", limits)
	}
	delta := Delta(merged, map[string]any{"mode": "auto", "limits": map[string]any{"high": 80.0}})
	if len(delta) != 2 {
		t.Fatalf("delta = %#v", delta)
	}
}

func TestMergeNilDeletes(t *testing.T) {
	merged := Merge(map[string]any{"a": 1.0, "b": 2.0}, map[string]any{"a": nil})
	if _, ok := merged["a"]; ok {
		t.Fatalf("nil patch should delete key: %#v", merged)
	}
}
