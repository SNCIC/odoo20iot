// Command gw-coap 提供 CoAP UDP 上行接入。默认只监听回环，生产必须配置 DTLS 入口代理或后续 DTLS listener。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/SNCIC/odoo20iot/internal/gateway"
	"github.com/SNCIC/odoo20iot/internal/protocol"
	"github.com/SNCIC/odoo20iot/internal/protocolruntime"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:5683", "CoAP UDP 监听地址")
	authFile := flag.String("auth-file", "tmp/dev-credentials.json", "设备凭据文件")
	natsURL := flag.String("nats-url", "nats://100.64.0.3:28222", "NATS 地址")
	stream := flag.String("nats-stream", "IOT_TELEMETRY", "遥测 Stream")
	project := flag.String("project", "spike", "开发租户标识")
	shards := flag.Int("shards", gateway.DefaultShards, "分片数")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	conn, err := net.ListenPacket("udp", *addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	rt, err := protocolruntime.NewFileRuntime(ctx, *authFile, *natsURL, *stream, *project, *shards, slog.Default())
	if err != nil {
		panic(err)
	}
	defer rt.Publisher.Close()
	ingest, err := gateway.NewProtocolIngestHandler(gateway.ProtocolIngestOptions{Authenticator: rt.Authenticator, Publisher: rt.Publisher, Router: gateway.ContractRouter{Project: *project, Shards: *shards}})
	if err != nil {
		panic(err)
	}
	server := &protocol.CoAPServer{Conn: conn, Handler: func(ctx context.Context, msg protocol.CoAPMessage, _ net.Addr) ([]byte, error) {
		frame, err := protocol.ParseCoAPIngress(msg)
		if err != nil {
			return nil, err
		}
		if _, err := ingest.Handle(ctx, frame, msg.Credential(), nil); err != nil {
			return nil, err
		}
		return protocol.BuildAck(msg), nil
	}}
	if err := server.Serve(ctx); err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
