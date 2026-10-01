package notify

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Guard 是出站请求的 SSRF 防护（04 §2.3 的 Webhook 安全 / 05 §1）。
//
// 文档给的对策是四项：「出站白名单 + DNS 固定解析 + 禁止内网段 + 统一出口代理」。
// 统一出口代理（egress-proxy）尚未部署，本实现落地其余三项 ——
// 少一项代理不会降低**拦截强度**，只是少了集中出站审计。这个差异必须写明：
// 「文档要求四项、实现了三项」如果不说，读代码的人会以为已经全做了。
type Guard struct {
	// AllowedHosts 是租户白名单：精确域名、`*.example.com` 形式的后缀，
	// 或 CIDR（用于「允许某个网段」的场景）。
	AllowedHosts []string
	// AllowLoopback 放行 127/8 与 ::1，**仅供测试与本地联调**。
	//
	// ⚠️ 生产必须为 false：回环上通常跑着同一台机器上的**所有**服务
	// （包括 PG、Redis 的管理口），放行回环等于把 SSRF 的靶场从内网
	// 缩小到本机 —— 攻击面变小了，但一个能改本机配置的漏洞反而更致命。
	AllowLoopback bool
	// Resolver 注入便于测试；nil 用 net.DefaultResolver。
	Resolver IPResolver

	rules []hostRule
}

type hostRule struct {
	raw    string
	exact  string
	suffix string
	cidr   *net.IPNet
}

// IPResolver 解析主机名。
//
// 定义成接口而不是直接用 *net.Resolver：**多 A 记录里混一个内网地址**
// 是 SSRF 最经典的绕过方式（一个域名同时解析出公网与 169.254.169.254），
// 而这条路径如果不可注入，就只能靠真去控制 DNS 才能测 —— 于是它不会被测。
type IPResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

// Target 是校验通过且**已固定解析**的出站目标。
type Target struct {
	URL *url.URL
	// Host 是原始主机名（Host 头要用它，不是 IP）。
	Host string
	// Port 是端口（补全默认值后的）。
	Port string
	// IPs 是通过校验的地址。
	IPs []net.IP
	// Scheme 是 http 或 https。
	Scheme string
}

// Addr 返回固定解析后的 dial 地址。
func (t *Target) Addr() string { return net.JoinHostPort(t.IPs[0].String(), t.Port) }

// CheckHost 校验一个主机名（webhook URL、SMTP 服务器、短信网关都走它）。
//
// 顺序很重要：**先白名单、再内网段**。反过来的话，一个白名单里的内部域名
// 会因为命中内网段被拒，而运维只会看到「白名单配了却不通」——
// 于是很可能把内网段检查放宽，把防线拆掉。
func (g *Guard) CheckHost(ctx context.Context, host, port string) (*Target, error) {
	if host == "" {
		return nil, Permanent("出站目标缺少主机名")
	}
	if port == "" {
		return nil, Permanent("出站目标缺少端口")
	}
	if err := g.ensureRules(); err != nil {
		return nil, err
	}

	ips, err := g.lookup(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, Permanent("主机 %s 解析不出任何地址", host)
	}

	if !g.whitelisted(host, ips) {
		return nil, Permanent("主机 %s 未命中出站白名单", host)
	}

	// ⚠️ **逐个**校验解析结果，而不是只看第一个：
	// 一个域名可以同时解析出公网与内网地址（多 A 记录 / DNS 轮询），
	// 只看第一个就会漏掉「第二个是 169.254.169.254」这种情况。
	for _, ip := range ips {
		if err := checkIP(ip, g.AllowLoopback); err != nil {
			return nil, err
		}
	}

	return &Target{Host: host, Port: port, IPs: ips}, nil
}

// Check 校验一个完整 URL 并返回已固定解析的目标。
func (g *Guard) Check(ctx context.Context, rawURL string) (*Target, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, Permanent("URL 解析失败: %v", err)
	}
	switch u.Scheme {
	case "http", "https":
	default:
		// 只允许这两种：file / gopher / dict / ftp 是 SSRF 里经典的
		// 「换个 scheme 就绕过 host 检查」的载体。
		return nil, Permanent("只允许 http/https，得到 scheme=%q", u.Scheme)
	}
	if u.User != nil {
		// 带凭据的 URL 会出现在日志与 DLQ 里，等于把密码写进审计。
		return nil, Permanent("出站 URL 不得携带凭据")
	}

	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	t, err := g.CheckHost(ctx, u.Hostname(), port)
	if err != nil {
		return nil, err
	}
	t.URL = u
	t.Scheme = u.Scheme
	return t, nil
}

// Transport 返回一个**只连已校验地址**的 RoundTripper。
//
// 这是防 DNS rebinding 的关键：若让 http 包自己去解析，攻击者可以在
// 「我们校验」与「真正连接」这两次解析之间把域名改指内网 ——
// 结果是**校验通过、连到内网**。固定 IP 后两次用的是同一个结果。
func (g *Guard) Transport(t *Target) *http.Transport {
	pinned := t.Addr()
	return &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			// 忽略传入的 addr，只连校验时定下的地址。
			d := &net.Dialer{Timeout: 3 * time.Second}
			return d.DialContext(ctx, network, pinned)
		},
		MaxIdleConnsPerHost: 2,
	}
}

// lookup 解析主机名；主机名本身就是 IP 时直接返回，不走 DNS。
func (g *Guard) lookup(ctx context.Context, host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	resolver := g.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	// 带超时：DNS 卡住会让整个投递卡在重试阶梯里，
	// 而超时后通道才走降级，人已经晚收到几分钟了。
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	addrs, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("解析 %s: %w", host, err)
	}
	out := make([]net.IP, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.IP)
	}
	return out, nil
}

func (g *Guard) ensureRules() error {
	if g.rules != nil {
		return nil
	}
	for _, raw := range g.AllowedHosts {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		r := hostRule{raw: raw}
		if _, n, err := net.ParseCIDR(raw); err == nil {
			r.cidr = n
		} else if strings.HasPrefix(raw, "*.") {
			r.suffix = strings.ToLower(raw[1:]) // 保留前导点：".example.com"
		} else {
			r.exact = strings.ToLower(raw)
		}
		g.rules = append(g.rules, r)
	}
	if len(g.rules) == 0 {
		return Permanent("出站白名单为空：拒绝一切出站" +
			"（宁可通知发不出去，也不要一个能打任意地址的 SSRF 面）")
	}
	return nil
}

func (g *Guard) whitelisted(host string, ips []net.IP) bool {
	h := strings.ToLower(host)
	for _, r := range g.rules {
		switch {
		case r.cidr != nil:
			for _, ip := range ips {
				if r.cidr.Contains(ip) {
					return true
				}
			}
		case r.suffix != "":
			// 后缀匹配**带前导点**：`.example.com` 匹配 `a.example.com`，
			// 但**不**匹配 `notexample.com` —— 少了这个点就是经典的
			// 「白名单后缀绕过」（攻击者注册 notexample.com 即可）。
			if strings.HasSuffix(h, r.suffix) {
				return true
			}
		case r.exact != "":
			if h == r.exact {
				return true
			}
		}
	}
	return false
}

// blockedNets 是禁止访问的地址段（04 §2.3 明列 + IPv6 与几处补充）。
var blockedNets = func() []*net.IPNet {
	specs := []string{
		"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16", "127.0.0.0/8",
		// 0.0.0.0/8：在 Linux 上 0.0.0.0 会被当成「本机」。
		"0.0.0.0/8",
		// 100.64.0.0/10：CGNAT —— 本项目内部服务（Tailscale）就在这一段，
		// webhook 打到 100.64.x.x 等于绕过公网、直接从平台内部发起请求。
		"100.64.0.0/10",
		"224.0.0.0/4", "240.0.0.0/4",
		// IPv6：未指定、回环、唯一本地、链路本地。
		//
		// ⚠️ 这里**刻意不列** `::ffff:0:0/96`（IPv4 映射段）。
		// `net.ParseCIDR` 会把它规范化成 4 字节表示，也就是 **`0.0.0.0/0`** ——
		// 于是它匹配**全部** IPv4，所有 webhook 都会被拦掉，而错误信息
		// 只会说「目标是禁止访问的地址段」。
		// 正确的做法见 checkIP：先把地址归一成 4 字节（To4），
		// `::ffff:10.0.0.1` 自然就落到 10/8 规则上了。
		"::/128", "::1/128", "fc00::/7", "fe80::/10",
	}
	out := make([]*net.IPNet, 0, len(specs))
	for _, s := range specs {
		_, n, err := net.ParseCIDR(s)
		if err != nil {
			panic("notify: 内建的内网段配置写错了: " + s)
		}
		out = append(out, n)
	}
	return out
}()

// metadataIP 是云元数据端点（04 §2.3 点名）。
//
// 它在 169.254/16 里已被拦，这里再单列一次：元数据端点是 SSRF 里收益最高、
// 也最常被写进用例的目标，单独一条能让人一眼看到「这个考虑过了」。
var metadataIP = net.ParseIP("169.254.169.254")

func checkIP(ip net.IP, allowLoopback bool) error {
	if ip == nil {
		return Permanent("解析出空地址")
	}
	// 归一成 4 字节：这样 `::ffff:10.0.0.1` 这类 IPv4 映射地址
	// 会落到 IPv4 网段规则上。不归一的话，攻击者只要把内网地址写成
	// IPv4 映射形式就能绕过整张 IPv4 网段表。
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	if ip.Equal(metadataIP) {
		return Permanent("目标是云元数据端点 169.254.169.254（SSRF 的首要目标）")
	}
	if allowLoopback && ip.IsLoopback() {
		return nil
	}
	for _, n := range blockedNets {
		if n.Contains(ip) {
			return Permanent("目标是禁止访问的地址段：%s（命中 %s）", ip, n)
		}
	}
	return nil
}

// PublicIP 报告某个地址是否允许作为出站目标（供用例与启动自检使用）。
func PublicIP(ip net.IP) bool { return checkIP(ip, false) == nil }
