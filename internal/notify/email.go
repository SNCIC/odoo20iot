package notify

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"mime"
	"net"
	"net/smtp"
	"strings"
	"time"
)

// EmailChannel 用 SMTP 投递。
type EmailChannel struct {
	guard   *Guard
	host    string
	port    string
	from    string
	helo    string
	auth    smtp.Auth
	timeout time.Duration
	// RequireTLS 要求服务器必须支持 STARTTLS，否则拒绝投递。
	//
	// 默认 false 是「机会式加密」：服务器支持就升级。机会式 TLS 有个已知弱点 ——
	// 中间人可以剥掉 STARTTLS 能力声明，双方就退回明文，而两端都察觉不到。
	// 公网 SMTP 必须置 true；本地假服务器 / 内网中继可以留 false。
	RequireTLS bool
}

// NewEmailChannel 构造。addr 形如 `smtp.example.com:587`。
func NewEmailChannel(g *Guard, addr, from, helo string, auth smtp.Auth, timeout time.Duration) (*EmailChannel, error) {
	if addr == "" || from == "" {
		return nil, fmt.Errorf("notify: email 通道需要 SMTP 地址与发件人")
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("notify: SMTP 地址应形如 host:port: %w", err)
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &EmailChannel{
		guard: g, host: host, port: port, from: from, helo: helo,
		auth: auth, timeout: timeout,
	}, nil
}

func (c *EmailChannel) Name() string { return ChannelEmail }

// Send 投递一封邮件。
func (c *EmailChannel) Send(ctx context.Context, msg Message, recipients []string) error {
	if len(recipients) == 0 {
		return Permanent("email 通道没有收件人")
	}

	// SMTP 不是 HTTP，但走**同一套**出站校验（白名单 + 禁内网 + 固定解析）：
	// 只给 HTTP 加防护，等于留了一条「拿邮件服务器当跳板」的路 ——
	// 而 SMTP 恰恰是内网里最常被放行的服务。
	t, err := c.guard.CheckHost(ctx, c.host, c.port)
	if err != nil {
		return err
	}

	d := &net.Dialer{Timeout: c.timeout}
	conn, err := d.DialContext(ctx, "tcp", t.Addr())
	if err != nil {
		return fmt.Errorf("连接 SMTP %s: %w", c.host, err)
	}

	// ⚠️ NewClient 传的是**域名**而不是我们固定解析出的 IP：
	// 它拿这个名字做 STARTTLS 的证书校验与 EHLO。传 IP 会让证书域名对不上，
	// 于是「为了安全而固定解析」反而把 TLS 弄坏了 —— 最后有人会去关掉校验，
	// 安全措施就这样被自己人拆掉。
	client, err := smtp.NewClient(conn, t.Host)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("SMTP 握手: %w", err)
	}
	defer func() { _ = client.Close() }()

	if c.helo != "" {
		if err := client.Hello(c.helo); err != nil {
			return fmt.Errorf("SMTP HELO: %w", err)
		}
	}

	// 有机会就升 TLS：不升的话，AUTH 的凭据与整封告警内容都会**明文上网**。
	if ok, _ := client.Extension("STARTTLS"); ok {
		// ServerName 用域名（不是固定解析出的 IP），证书校验才对得上。
		if err := client.StartTLS(&tls.Config{ServerName: t.Host, MinVersion: tls.VersionTLS12}); err != nil {
			return fmt.Errorf("SMTP STARTTLS: %w", err)
		}
	} else if c.RequireTLS {
		return Permanent("SMTP 服务器不支持 STARTTLS，而配置要求必须加密（拒绝明文投递）")
	}
	if c.auth != nil {
		if err := client.Auth(c.auth); err != nil {
			// 认证失败重试无意义（除非凭据轮换，那是人的动作）。
			return Permanent("SMTP 认证失败（检查凭据）: %v", err)
		}
	}
	if err := client.Mail(c.from); err != nil {
		return fmt.Errorf("SMTP MAIL FROM: %w", err)
	}

	var rejected []string
	accepted := 0
	for _, rcpt := range recipients {
		if err := client.Rcpt(rcpt); err != nil {
			rejected = append(rejected, rcpt)
			continue
		}
		accepted++
	}
	if accepted == 0 {
		return Permanent("SMTP 拒绝了全部收件人: %v", rejected)
	}

	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("SMTP DATA: %w", err)
	}
	if _, err := w.Write(c.buildMessage(msg, recipients)); err != nil {
		return fmt.Errorf("写邮件正文: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("提交邮件: %w", err)
	}
	if err := client.Quit(); err != nil {
		// 邮件已经提交成功，QUIT 失败只影响连接回收。
		return nil
	}

	if len(rejected) > 0 {
		return &PartialError{Channel: ChannelEmail, Rejected: rejected, Accepted: accepted}
	}
	return nil
}

// buildMessage 拼一封 MIME 邮件。
func (c *EmailChannel) buildMessage(msg Message, recipients []string) []byte {
	var b bytes.Buffer
	// 中文主题必须按 RFC 2047 编码，否则收件人看到的是乱码或原始编码串。
	subject := mime.QEncoding.Encode("utf-8", headerSafe(msg.Subject))
	fmt.Fprintf(&b, "From: %s\r\n", headerSafe(c.from))
	fmt.Fprintf(&b, "To: %s\r\n", headerSafe(strings.Join(recipients, ", ")))
	fmt.Fprintf(&b, "Subject: %s\r\n", subject)
	fmt.Fprintf(&b, "Date: %s\r\n", msg.At.Format(time.RFC1123Z))
	fmt.Fprintf(&b, "MIME-Version: 1.0\r\n")
	fmt.Fprintf(&b, "Content-Type: text/plain; charset=UTF-8\r\n")
	fmt.Fprintf(&b, "Content-Transfer-Encoding: 8bit\r\n")
	// 便于从邮件反查到告警与链路（值班的人常常只有那封邮件）。
	fmt.Fprintf(&b, "X-Alarm-Id: %s\r\n", headerSafe(msg.AlarmID))
	fmt.Fprintf(&b, "X-Trace-Id: %s\r\n", headerSafe(msg.TraceID))
	fmt.Fprintf(&b, "\r\n%s\r\n", msg.Body)
	return b.Bytes()
}

// headerSafe 去掉邮件头里的换行，防止**头注入**。
//
// 主题来自告警规则名，而规则名是用户可控的。带换行的主题可以注入任意头
// （典型是 Bcc），把通知抄送到攻击者指定的地址 —— 于是「告警通知」
// 变成了「把内部设备与告警详情持续外发」的通道。
func headerSafe(s string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
}
