package protocol

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"
)

// CoAPHandler 负责认证、持久化和生成业务响应；监听器只处理 UDP 生命周期。
type CoAPHandler func(context.Context, CoAPMessage, net.Addr) ([]byte, error)

type CoAPServer struct {
	Conn    net.PacketConn
	Handler CoAPHandler
	Logger  *slog.Logger
	MaxSize int
}

func (s *CoAPServer) Serve(ctx context.Context) error {
	if s.Conn == nil || s.Handler == nil {
		return fmt.Errorf("coap server 缺少 conn 或 handler")
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	if s.MaxSize <= 0 {
		s.MaxSize = MaxTCPFrame
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		buf := make([]byte, s.MaxSize)
		_ = s.Conn.SetReadDeadline(time.Now().Add(time.Second))
		n, addr, err := s.Conn.ReadFrom(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
		msg, err := ParseCoAP(buf[:n])
		if err != nil {
			s.Logger.Warn("拒绝非法 CoAP 报文", "remote", addr.String(), "error", err)
			continue
		}
		response, err := s.Handler(ctx, msg, addr)
		if err != nil {
			s.Logger.Warn("CoAP 上报处理失败", "remote", addr.String(), "error", err)
			continue
		}
		if len(response) > 0 {
			if _, err := s.Conn.WriteTo(response, addr); err != nil {
				s.Logger.Warn("CoAP 响应发送失败", "error", err)
			}
		}
	}
}

// TCPHandler 在注册完成后处理每个数据帧；返回值作为长度帧回复，nil 表示不回复。
type TCPHandler func(context.Context, TCPRegistration, []byte, net.Addr) ([]byte, error)

type TCPServer struct {
	Listener net.Listener
	Handler  TCPHandler
	Logger   *slog.Logger
	Idle     time.Duration
}

func (s *TCPServer) Serve(ctx context.Context) error {
	if s.Listener == nil || s.Handler == nil {
		return fmt.Errorf("tcp server 缺少 listener 或 handler")
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			s.Logger.Warn("TCP 接受连接失败", "error", err)
			continue
		}
		go s.serveConn(ctx, conn)
	}
}

func (s *TCPServer) serveConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	reg, err := ReadTCPRegistration(r)
	if err != nil {
		s.Logger.Warn("TCP 注册失败", "remote", conn.RemoteAddr().String(), "error", err)
		return
	}
	for {
		if s.Idle > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.Idle))
		}
		frame, err := ReadTCPFrame(r)
		if err != nil {
			if err != io.EOF {
				s.Logger.Warn("TCP 读取帧失败", "device_key", reg.DeviceKey, "error", err)
			}
			return
		}
		if frame == nil {
			continue
		}
		response, err := s.Handler(ctx, reg, frame, conn.RemoteAddr())
		if err != nil {
			s.Logger.Warn("TCP 上报处理失败", "device_key", reg.DeviceKey, "error", err)
			continue
		}
		if len(response) > 0 {
			if _, err := conn.Write(EncodeTCPFrame(response)); err != nil {
				return
			}
		}
	}
}

func NewTLSListener(addr string, config *tls.Config) (net.Listener, error) {
	if config == nil {
		return nil, fmt.Errorf("TLS 配置不能为空")
	}
	return tls.Listen("tcp", addr, config)
}
