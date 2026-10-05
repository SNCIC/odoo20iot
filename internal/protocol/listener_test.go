package protocol

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestTCPServerRegistrationAndFrame(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	called := make(chan string, 1)
	server := &TCPServer{Listener: listener, Handler: func(_ context.Context, reg TCPRegistration, frame []byte, _ net.Addr) ([]byte, error) {
		called <- reg.DeviceKey + ":" + string(frame)
		return []byte(`{"ok":true}`), nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.Serve(ctx) }()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte(`{"device_key":"d1","secret":"s"}` + "\n"))
	_, _ = conn.Write(EncodeTCPFrame([]byte(`{"x":1}`)))
	select {
	case got := <-called:
		if got != `d1:{"x":1}` {
			t.Fatalf("handler got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("handler 未调用")
	}
}
