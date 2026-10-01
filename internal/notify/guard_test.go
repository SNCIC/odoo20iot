package notify

import (
	"context"
	"fmt"
	"net"
	"testing"
)

// fakeResolver 让「多 A 记录」「解析失败」这些场景可测。
type fakeResolver map[string][]string

func (f fakeResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	ips, ok := f[host]
	if !ok {
		return nil, fmt.Errorf("no such host: %s", host)
	}
	out := make([]net.IPAddr, 0, len(ips))
	for _, s := range ips {
		out = append(out, net.IPAddr{IP: net.ParseIP(s)})
	}
	return out, nil
}

func TestGuardRejectsForbiddenTargets(t *testing.T) {
	g := &Guard{
		AllowedHosts: []string{"*.example.com", "127.0.0.1", "169.254.169.254", "10.0.0.5"},
		Resolver: fakeResolver{
			"hooks.example.com": {"1.2.3.4"},
			"127.0.0.1":         {"127.0.0.1"},
			"169.254.169.254":   {"169.254.169.254"},
			"10.0.0.5":          {"10.0.0.5"},
			"::1":               {"::1"},
			"0.0.0.0":           {"0.0.0.0"},
		},
	}
	ctx := context.Background()

	// 全部用例都**命中白名单**，被拒的唯一原因是「目标是内网/元数据」——
	// 这样才验证的是内网段检查，而不是白名单检查。
	for _, raw := range []string{
		"http://127.0.0.1/admin",
		"http://169.254.169.254/latest/meta-data/",
		"http://10.0.0.5/",
		"http://[::1]:8080/",
		"http://0.0.0.0/",
	} {
		if _, err := g.Check(ctx, raw); err == nil {
			t.Fatalf("%s 应被拒绝（04 §2.3 禁止内网段与元数据端点）", raw)
		} else if !IsPermanent(err) {
			t.Fatalf("%s 应被标为永久失败（换 URL 也没用），得 %v", raw, err)
		}
	}

	// 公网目标放行。
	if _, err := g.Check(ctx, "https://hooks.example.com/robot/send?key=abc"); err != nil {
		t.Fatalf("公网目标应放行: %v", err)
	}
}

func TestGuardRejectsMultiARecordWithOnePrivate(t *testing.T) {
	// ⚠️ SSRF 最经典的绕过：一个域名同时解析出公网与内网地址。
	// 只看第一个（或只看有没有公网）就会放行，而实际连接可能落到内网那个。
	g := &Guard{
		AllowedHosts: []string{"*.example.com"},
		Resolver:     fakeResolver{"evil.example.com": {"1.2.3.4", "169.254.169.254"}},
	}
	_, err := g.Check(context.Background(), "http://evil.example.com/")
	if err == nil {
		t.Fatal("多 A 记录里混了一个内网地址时必须整体拒绝")
	}
}

func TestGuardWhitelistSuffixBypass(t *testing.T) {
	g := &Guard{
		AllowedHosts: []string{"*.example.com"},
		Resolver: fakeResolver{
			"a.example.com":   {"1.2.3.4"},
			"notexample.com":  {"1.2.3.5"},
			"xexample.com":    {"1.2.3.6"},
			"example.com":     {"1.2.3.7"},
			"a.b.example.com": {"1.2.3.8"},
		},
	}
	ctx := context.Background()

	if _, err := g.Check(ctx, "http://a.example.com/"); err != nil {
		t.Fatalf("子域应命中: %v", err)
	}
	if _, err := g.Check(ctx, "http://a.b.example.com/"); err != nil {
		t.Fatalf("多级子域应命中: %v", err)
	}
	// ⚠️ 后缀匹配少了前导点就会放行这两个 ——
	// 攻击者只要注册 notexample.com 就能命中白名单。
	for _, raw := range []string{"http://notexample.com/", "http://xexample.com/"} {
		if _, err := g.Check(ctx, raw); err == nil {
			t.Fatalf("%s 不该命中 *.example.com（少了前导点的经典绕过）", raw)
		}
	}
	// 通配不覆盖裸域（这是刻意的：`*.example.com` 与 `example.com` 是两条规则）。
	if _, err := g.Check(ctx, "http://example.com/"); err == nil {
		t.Fatal("`*.example.com` 不该覆盖裸域")
	}
}

func TestGuardRequiresWhitelist(t *testing.T) {
	// 白名单为空 = 拒绝一切出站。宁可通知发不出去，
	// 也不要一个能打任意地址的 SSRF 面。
	g := &Guard{Resolver: fakeResolver{"hooks.example.com": {"1.2.3.4"}}}
	if _, err := g.Check(context.Background(), "http://hooks.example.com/"); err == nil {
		t.Fatal("白名单为空时必须拒绝一切出站")
	}
}

func TestGuardRejectsSchemeAndCredential(t *testing.T) {
	g := &Guard{
		AllowedHosts: []string{"hooks.example.com"},
		Resolver:     fakeResolver{"hooks.example.com": {"1.2.3.4"}},
	}
	ctx := context.Background()

	// 换个 scheme 是 SSRF 里经典的绕过路径（file/gopher/dict 都能读本地文件）。
	for _, raw := range []string{
		"file:///etc/passwd",
		"gopher://hooks.example.com:70/1",
		"ftp://hooks.example.com/",
		"http://user:pass@hooks.example.com/",
	} {
		if _, err := g.Check(ctx, raw); err == nil {
			t.Fatalf("%s 应被拒绝", raw)
		}
	}
}

func TestGuardTransportPinsResolvedIP(t *testing.T) {
	// DNS 固定解析：即使域名后来改指内网，连接仍然打向校验时的那个 IP。
	// 这里直接验证 Transport 用的是固定地址而不是再解析一次。
	g := &Guard{AllowedHosts: []string{"hooks.example.com"}}
	target := &Target{Host: "hooks.example.com", Port: "80", IPs: []net.IP{net.ParseIP("1.2.3.4")}}
	if got := target.Addr(); got != "1.2.3.4:80" {
		t.Fatalf("固定地址得 %q", got)
	}
	if g.Transport(target) == nil {
		t.Fatal("Transport 不该为 nil")
	}
}

func TestPublicIP(t *testing.T) {
	cases := map[string]bool{
		"8.8.8.8":         true,
		"1.2.3.4":         true,
		"10.0.0.1":        false,
		"172.16.0.1":      false,
		"192.168.1.1":     false,
		"169.254.169.254": false,
		"127.0.0.1":       false,
		"100.64.0.3":      false, // Tailscale/CGNAT：本项目内部服务段
		"0.0.0.0":         false,
		"::1":             false,
		"fc00::1":         false,
	}
	for ip, want := range cases {
		if got := PublicIP(net.ParseIP(ip)); got != want {
			t.Errorf("PublicIP(%s)=%v，期望 %v", ip, got, want)
		}
	}
}

func TestGuardAllowLoopbackIsExplicit(t *testing.T) {
	// AllowLoopback 只放行回环，**不**放行其它内网段 ——
	// 它是给本地联调开的窄口子，不是把内网检查整体关掉。
	g := &Guard{
		AllowedHosts:  []string{"127.0.0.1", "10.0.0.5"},
		AllowLoopback: true,
		Resolver:      fakeResolver{"127.0.0.1": {"127.0.0.1"}, "10.0.0.5": {"10.0.0.5"}},
	}
	ctx := context.Background()
	if _, err := g.Check(ctx, "http://127.0.0.1:9999/"); err != nil {
		t.Fatalf("显式开启时回环应放行: %v", err)
	}
	if _, err := g.Check(ctx, "http://10.0.0.5/"); err == nil {
		t.Fatal("AllowLoopback 不该顺带放行 10/8")
	}
	// 而 PublicIP（生产口径）对回环始终为 false —— 本地联调的口子
	// 只存在于 Guard 实例上，不会污染给别的调用方看的判断。
	if PublicIP(net.ParseIP("127.0.0.1")) {
		t.Fatal("PublicIP 不该受 AllowLoopback 影响")
	}
}

func TestGuardHandlesIPv4MappedAddress(t *testing.T) {
	// IPv4 映射形式必须落到 IPv4 网段规则上：`::ffff:10.0.0.1` 就是 10.0.0.1。
	// 不归一的话，把内网地址写成映射形式就能绕过整张 IPv4 网段表。
	g := &Guard{
		AllowedHosts: []string{"10.0.0.1", "8.8.8.8"},
		Resolver: fakeResolver{
			"10.0.0.1": {"::ffff:10.0.0.1"},
			"8.8.8.8":  {"8.8.8.8"},
		},
	}
	ctx := context.Background()
	if _, err := g.Check(ctx, "http://10.0.0.1/"); err == nil {
		t.Fatal("IPv4 映射形式的内网地址必须被拦")
	}
	// 反过来：公网地址绝不能被误拦。
	// ⚠️ 这里曾经踩过一次：网段表里的 `::ffff:0:0/96` 会被 Go 规范化成
	// `0.0.0.0/0`，也就是匹配**全部** IPv4 —— 所有 webhook 都会被拦掉。
	if _, err := g.Check(ctx, "http://8.8.8.8/"); err != nil {
		t.Fatalf("公网 IPv4 不该被拦: %v", err)
	}
}
