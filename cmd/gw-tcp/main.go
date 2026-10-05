// Command gw-tcp 提供 DTU 注册与长度帧上行接入。生产默认要求 TLS 证书。
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SNCIC/odoo20iot/internal/gateway"
	"github.com/SNCIC/odoo20iot/internal/protocol"
	"github.com/SNCIC/odoo20iot/internal/protocolruntime"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "TCP/TLS 监听地址")
	cert := flag.String("tls-cert", "", "TLS 服务端证书")
	key := flag.String("tls-key", "", "TLS 服务端私钥")
	allowPlaintext := flag.Bool("allow-plaintext", false, "允许明文 TCP（仅本地联调）")
	authFile := flag.String("auth-file", "tmp/dev-credentials.json", "设备凭据文件")
	natsURL := flag.String("nats-url", "nats://100.64.0.3:28222", "NATS 地址")
	stream := flag.String("nats-stream", "IOT_TELEMETRY", "遥测 Stream")
	project := flag.String("project", "spike", "开发租户标识")
	shards := flag.Int("shards", gateway.DefaultShards, "分片数")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var listener net.Listener
	var err error
	if *allowPlaintext {
		listener, err = net.Listen("tcp", *addr)
	} else {
		if *cert == "" || *key == "" {
			panic("未配置 TLS 证书；仅本地联调可使用 -allow-plaintext")
		}
		pair, loadErr := tls.LoadX509KeyPair(*cert, *key)
		if loadErr != nil {
			panic(loadErr)
		}
		listener, err = protocol.NewTLSListener(*addr, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}})
	}
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	rt, err := protocolruntime.NewFileRuntime(ctx, *authFile, *natsURL, *stream, *project, *shards, slog.Default())
	if err != nil {
		panic(err)
	}
	defer rt.Publisher.Close()
	ingest, err := gateway.NewProtocolIngestHandler(gateway.ProtocolIngestOptions{Authenticator: rt.Authenticator, Publisher: rt.Publisher, Router: gateway.ContractRouter{Project: *project, Shards: *shards}})
	if err != nil {
		panic(err)
	}
	server := &protocol.TCPServer{Listener: listener, Idle: 5 * time.Minute, Handler: func(ctx context.Context, reg protocol.TCPRegistration, raw []byte, _ net.Addr) ([]byte, error) {
		frame, err := protocol.ParseTCPIngress(reg.DeviceKey, raw)
		if err != nil {
			return nil, err
		}
		if _, err := ingest.Handle(ctx, frame, reg.Secret, nil); err != nil {
			return nil, err
		}
		return []byte(`{"ok":true}`), nil
	}}
	if err := server.Serve(ctx); err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
