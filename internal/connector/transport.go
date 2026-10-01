package connector

import (
	"net"
	"net/http"
	"time"
)

// HTTPClient 按策略构造符合 07 §4.3 超时约定的 HTTP 客户端
// （连接 3s / 读 15s），直接交给 `odoo.Config.HTTPClient`。
//
// 两个超时的分工：
//   - 连接 3s 由 Dialer 兜住 —— 连不上要快速失败，不能占着在途名额；
//   - 读 15s 同时挂在 Client.Timeout 与 ResponseHeaderTimeout 上：
//     前者兜住「拿着连接慢慢读」，后者兜住「服务端迟迟不吐响应头」。
func (p Policy) HTTPClient() *http.Client {
	p = p.Normalize()

	dialer := &net.Dialer{Timeout: p.DialTimeout}

	return &http.Client{
		Timeout: p.ReadTimeout,
		Transport: &http.Transport{
			DialContext:           dialer.DialContext,
			ResponseHeaderTimeout: p.ReadTimeout,
			// 连接池按在途上限配：池子小于在途数会让请求白白排队等连接，
			// 把「限流」的意图变成「连接不够」的假象。
			MaxIdleConns:        2 * p.MaxInFlight,
			MaxIdleConnsPerHost: 2 * p.MaxInFlight,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}
