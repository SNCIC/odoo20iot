package notify

import (
	"bufio"
	"context"
	"fmt"
	"mime"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSMTP 是最小的 SMTP 服务器：只够验证「我们到底发出去了什么」。
//
// 刻意**不通告 STARTTLS**：这样测试跑的是明文路径，
// 也顺带覆盖了「服务器不支持加密时怎么办」的分支。
type fakeSMTP struct {
	ln     net.Listener
	reject map[string]bool

	mu    sync.Mutex
	from  string
	rcpts []string
	data  string
}

func newFakeSMTP(t *testing.T) *fakeSMTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("起假 SMTP: %v", err)
	}
	s := &fakeSMTP{ln: ln, reject: map[string]bool{}}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serve(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *fakeSMTP) addr() string { return s.ln.Addr().String() }

func (s *fakeSMTP) message() (string, []string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.from, append([]string(nil), s.rcpts...), s.data
}

func (s *fakeSMTP) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	br := bufio.NewReader(conn)
	reply := func(s string) { _, _ = fmt.Fprintf(conn, "%s\r\n", s) }

	reply("220 fake ESMTP")
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		cmd := strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
			// 多行应答；刻意不含 STARTTLS。
			reply("250-fake")
			reply("250 OK")
		case strings.HasPrefix(cmd, "MAIL FROM:"):
			s.mu.Lock()
			s.from = strings.Trim(strings.TrimPrefix(cmd, "MAIL FROM:"), "<> ")
			s.mu.Unlock()
			reply("250 OK")
		case strings.HasPrefix(cmd, "RCPT TO:"):
			addr := strings.Trim(strings.TrimPrefix(cmd, "RCPT TO:"), "<> ")
			if s.reject[addr] {
				reply("550 no such user")
				continue
			}
			s.mu.Lock()
			s.rcpts = append(s.rcpts, addr)
			s.mu.Unlock()
			reply("250 OK")
		case cmd == "DATA":
			reply("354 end with .")
			var b strings.Builder
			for {
				l, err := br.ReadString('\n')
				if err != nil {
					return
				}
				if strings.TrimRight(l, "\r\n") == "." {
					break
				}
				b.WriteString(l)
			}
			s.mu.Lock()
			s.data = b.String()
			s.mu.Unlock()
			reply("250 queued")
		case cmd == "QUIT":
			reply("221 bye")
			return
		default:
			reply("250 OK")
		}
	}
}

func newEmailChannel(t *testing.T, s *fakeSMTP, requireTLS bool) *EmailChannel {
	t.Helper()
	ch, err := NewEmailChannel(loopbackGuard(), s.addr(),
		"iot@example.com", "iot.example.com", nil, 3*time.Second)
	if err != nil {
		t.Fatalf("NewEmailChannel: %v", err)
	}
	ch.RequireTLS = requireTLS
	return ch
}

func TestEmailSendsRenderedMessage(t *testing.T) {
	s := newFakeSMTP(t)
	ch := newEmailChannel(t, s, false)

	msg := testMessage()
	if err := ch.Send(context.Background(), msg, []string{"oncall@example.com", "leader@example.com"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	from, rcpts, data := s.message()
	if from != "iot@example.com" {
		t.Fatalf("MAIL FROM 得 %q", from)
	}
	if len(rcpts) != 2 {
		t.Fatalf("应有两个收件人，得 %v", rcpts)
	}
	if !strings.Contains(data, "设备 d1 温度 92℃") {
		t.Fatalf("正文缺失: %s", data)
	}
	// 中文主题必须按 RFC 2047 编码，否则收件人看到的是乱码。
	if !strings.Contains(data, "Subject: =?") {
		t.Fatalf("主题应做 RFC 2047 编码: %s", data)
	}
	dec := new(mime.WordDecoder)
	subject, err := dec.DecodeHeader("=?utf-8?q?=E6=B8=A9=E5=BA=A6=E8=BF=87=E9=AB=98?=")
	if err != nil {
		t.Fatalf("解码示例: %v", err)
	}
	if subject != "温度过高" {
		t.Fatalf("编码约定变了：得 %q", subject)
	}
	// 便于从邮件反查到告警与链路（值班的人常常只有那封邮件）。
	if !strings.Contains(data, "X-Alarm-Id: alarm-1") {
		t.Fatalf("应带 X-Alarm-Id: %s", data)
	}
	if !strings.Contains(data, "X-Trace-Id: trace-1") {
		t.Fatalf("应带 X-Trace-Id: %s", data)
	}
}

func TestEmailHeaderInjectionIsBlocked(t *testing.T) {
	// 主题来自**告警规则名**，而规则名是用户可控的。带换行的主题可以注入
	// 任意邮件头（典型是 Bcc），把通知抄送到攻击者指定的地址 ——
	// 于是「告警通知」变成「把设备与告警详情持续外发」的通道。
	s := newFakeSMTP(t)
	ch := newEmailChannel(t, s, false)

	msg := testMessage()
	msg.Subject = "正常标题\r\nBcc: attacker@evil.example.com"
	if err := ch.Send(context.Background(), msg, []string{"oncall@example.com"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	_, _, data := s.message()
	// 换行被抹平后，注入的内容会留在**同一行**（作为标题的一部分），
	// 而绝不会成为独立的一行头。
	for _, line := range strings.Split(data, "\r\n") {
		if strings.HasPrefix(strings.ToLower(line), "bcc:") {
			t.Fatalf("邮件头注入未被拦住：%s", data)
		}
	}
}

func TestEmailPartialRejection(t *testing.T) {
	s := newFakeSMTP(t)
	s.reject["gone@example.com"] = true
	ch := newEmailChannel(t, s, false)

	err := ch.Send(context.Background(), testMessage(),
		[]string{"gone@example.com", "oncall@example.com"})
	if err == nil {
		t.Fatal("有收件人被拒时应报错")
	}
	// 部分投递需要**第三种结果**：不该重试（会给已收到的人重发），
	// 也不该降级（通道其实好好的）。
	if !IsPartial(err) {
		t.Fatalf("应标记为部分投递，得 %v", err)
	}
	if IsPermanent(err) {
		t.Fatal("部分投递不该被标成永久失败")
	}
	_, rcpts, _ := s.message()
	if len(rcpts) != 1 || rcpts[0] != "oncall@example.com" {
		t.Fatalf("被接受的收件人应收到，得 %v", rcpts)
	}
}

func TestEmailAllRejectedIsPermanent(t *testing.T) {
	s := newFakeSMTP(t)
	s.reject["gone@example.com"] = true
	ch := newEmailChannel(t, s, false)

	err := ch.Send(context.Background(), testMessage(), []string{"gone@example.com"})
	if err == nil || !IsPermanent(err) {
		t.Fatalf("全部被拒应永久失败（重试无意义），得 %v", err)
	}
}

func TestEmailRequireTLSRejectsPlaintext(t *testing.T) {
	// 机会式 TLS 有已知弱点：中间人剥掉 STARTTLS 能力声明后双方退回明文，
	// 而两端都察觉不到 —— 凭据与整封告警都会明文上网。
	// 公网 SMTP 必须把 RequireTLS 打开。
	s := newFakeSMTP(t) // 不通告 STARTTLS
	ch := newEmailChannel(t, s, true)

	err := ch.Send(context.Background(), testMessage(), []string{"oncall@example.com"})
	if err == nil || !IsPermanent(err) {
		t.Fatalf("要求加密时应拒绝明文投递，得 %v", err)
	}
	if _, _, data := s.message(); data != "" {
		t.Fatal("拒绝之后不该把正文发出去")
	}
}
