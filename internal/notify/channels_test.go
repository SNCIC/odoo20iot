package notify

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func loopbackGuard() *Guard {
	// AllowLoopback 只为让 httptest 的 127.0.0.1 可打；生产必须为 false。
	return &Guard{AllowedHosts: []string{"127.0.0.1"}, AllowLoopback: true}
}

func testMessage() Message {
	return Message{
		TraceID: "trace-1", AlarmID: "alarm-1", DedupKey: "dk1",
		ProjectID: "p1", DeviceID: "d1", Level: "critical",
		Subject: "温度过高", Body: "设备 d1 温度 92℃",
		At: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func TestWebhookSuccessAndBusinessError(t *testing.T) {
	ctx := context.Background()

	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("Content-Type 得 %q", ct)
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "alarm-1") {
			t.Errorf("载荷缺少 alarm_id: %s", body)
		}
		if !strings.Contains(string(body), "温度过高") {
			t.Errorf("载荷缺少告警内容: %s", body)
		}
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	defer ok.Close()

	ch := NewWebhookChannel(loopbackGuard(), time.Second)
	if err := ch.Send(ctx, testMessage(), []string{ok.URL}); err != nil {
		t.Fatalf("errcode=0 应成功: %v", err)
	}

	// ⚠️ HTTP 200 但 errcode != 0 —— 国内三家 IM 机器人都是这个形状。
	// 只判状态码会把「token 过期 / 机器人被移出群」记成投递成功，
	// 于是通知静默丢失而指标上一切正常。
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"errcode":310000,"errmsg":"token is not exist"}`))
	}))
	defer bad.Close()

	err := ch.Send(ctx, testMessage(), []string{bad.URL})
	if err == nil {
		t.Fatal("HTTP 200 但 errcode!=0 必须报失败")
	}
	if IsPermanent(err) {
		t.Fatalf("业务错误码应按**可重试**处理（频率超限重试就能过），得 %v", err)
	}
	if !strings.Contains(err.Error(), "310000") {
		t.Fatalf("错误信息要带码值（排障第一眼看的就是它），得 %v", err)
	}
}

func TestWebhookStatusClassification(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		code      int
		permanent bool
	}{
		{400, true}, {401, true}, {403, true}, {404, true},
		{408, false}, {429, false}, {500, false}, {503, false},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(c.code)
			_, _ = w.Write([]byte("detail"))
		}))
		err := NewWebhookChannel(loopbackGuard(), time.Second).
			Send(ctx, testMessage(), []string{srv.URL})
		srv.Close()

		if err == nil {
			t.Fatalf("%d 应报错", c.code)
		}
		if IsPermanent(err) != c.permanent {
			t.Fatalf("%d：permanent=%v，期望 %v（%v）", c.code, IsPermanent(err), c.permanent, err)
		}
	}
}

func TestWebhookTruncatesResponse(t *testing.T) {
	// 出站响应是**不可信输入**：对方可以返回 100MB 把内存打爆，
	// 也可以塞一大段 HTML 把日志搅乱。04 §2.3 要求截到 1 KB。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(strings.Repeat("x", 5000)))
	}))
	defer srv.Close()

	err := NewWebhookChannel(loopbackGuard(), time.Second).
		Send(context.Background(), testMessage(), []string{srv.URL})
	if err == nil {
		t.Fatal("应报错")
	}
	if len(err.Error()) > 3000 {
		t.Fatalf("响应体应被截断到 1KB，得 %d 字节", len(err.Error()))
	}
}

func TestWebhookRejectsNonWhitelistedBeforeSending(t *testing.T) {
	// 白名单外的目标必须在**发出请求之前**就被拒 —— 否则「拒绝」只是
	// 事后记录，请求已经打到对方了。
	var hit bool
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hit = true
	}))
	defer srv.Close()

	g := &Guard{AllowedHosts: []string{"hooks.example.com"}, AllowLoopback: true}
	err := NewWebhookChannel(g, time.Second).
		Send(context.Background(), testMessage(), []string{srv.URL})
	if err == nil || !IsPermanent(err) {
		t.Fatalf("白名单外应永久失败，得 %v", err)
	}
	if hit {
		t.Fatal("请求不该真的发出去")
	}
}

func TestWebhookOneRecipientFailingDoesNotBlockOthers(t *testing.T) {
	// 同一个告警发给三个群，其中一个机器人被禁用，
	// 不该导致另外两个群也收不到。
	var good int
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		good++
		_, _ = w.Write([]byte(`{"errcode":0}`))
	}))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()

	err := NewWebhookChannel(loopbackGuard(), time.Second).
		Send(context.Background(), testMessage(), []string{bad.URL, ok.URL})
	if good != 1 {
		t.Fatalf("正常收件人应收到，得 %d", good)
	}
	if err == nil {
		t.Fatal("有收件人失败时应报错（让上层重试/降级）")
	}
	if IsPermanent(err) {
		t.Fatal("5xx 属可重试，不该标成永久失败")
	}
}

func TestSMSConfigAndBusinessError(t *testing.T) {
	ctx := context.Background()

	// 未配置 → **永久失败**，而不是静默成功：
	// 静默成功会让「短信通道一直没配」永远不暴露，
	// 直到某天有人问「为什么半夜没人接到电话」。
	un := NewSMSChannel(loopbackGuard(), "", "", "", "", time.Second)
	err := un.Send(ctx, testMessage(), []string{"13800000000"})
	if err == nil || !IsPermanent(err) {
		t.Fatalf("未配置应永久失败，得 %v", err)
	}

	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tk" {
			t.Errorf("应带凭据，得 %q", got)
		}
		body, _ := io.ReadAll(r.Body)
		// 只发**模板变量**，不发自由正文（运营商的报备要求）。
		if !strings.Contains(string(body), "template_id") {
			t.Errorf("应走模板，得 %s", body)
		}
		_, _ = w.Write([]byte(`{"code":0,"msg":"success"}`))
	}))
	defer ok.Close()

	ch := NewSMSChannel(loopbackGuard(), ok.URL, "tk", "签名", "SMS_001", time.Second)
	if err := ch.Send(ctx, testMessage(), []string{"13800000000"}); err != nil {
		t.Fatalf("业务码 0 应成功: %v", err)
	}

	// 与 IM 机器人同一个陷阱：短信厂商也「200 + 码值」。
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":500,"msg":"模板未报备"}`))
	}))
	defer badSrv.Close()
	badCh := NewSMSChannel(loopbackGuard(), badSrv.URL, "tk", "签名", "SMS_001", time.Second)
	if err := badCh.Send(ctx, testMessage(), []string{"13800000000"}); err == nil {
		t.Fatal("业务码非 0 必须报错")
	}
}
