package modbusgw

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/SNCIC/odoo20iot/internal/protocol"
)

type pipeTransport struct{ net.Conn }

func (p pipeTransport) SetDeadline(deadline time.Time) error { return p.Conn.SetDeadline(deadline) }

var _ io.ReadWriteCloser = pipeTransport{}

func TestDecode(t *testing.T) {
	values := []uint16{25, 3, 0x4148, 0}
	if got, err := Decode(values, Point{Name: "temperature", Address: 0, Type: "uint16", Scale: 0.1}); err != nil || got != 2.5 {
		t.Fatalf("uint16=%v err=%v", got, err)
	}
	if got, err := Decode(values, Point{Name: "float", Address: 2, Type: "float32"}); err != nil || got != 12.5 {
		t.Fatalf("float=%v err=%v", got, err)
	}
}

func TestReadHoldingTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, _ := ln.Accept()
		defer conn.Close()
		request := make([]byte, 12)
		_, _ = readFull(conn, request)
		response := []byte{0, 1, 0, 0, 0, 7, 1, 3, 4, 0, 10, 0, 20}
		_, _ = conn.Write(response)
	}()
	client := &Client{Endpoint: ln.Addr().String(), Timeout: time.Second}
	values, err := client.ReadHolding(context.Background(), 1, 0, 2)
	if err != nil || len(values) != 2 || values[1] != 20 {
		t.Fatalf("values=%v err=%v", values, err)
	}
}

func TestReadHoldingRTU(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() {
		request := make([]byte, 8)
		_, _ = io.ReadFull(server, request)
		response := []byte{1, 3, 4, 0, 10, 0, 20}
		crc := protocol.ModbusCRC16(response)
		response = append(response, byte(crc), byte(crc>>8))
		_, _ = server.Write(response)
	}()
	values, err := ReadHoldingRTU(context.Background(), pipeTransport{client}, 1, 0, 2, time.Second)
	if err != nil || len(values) != 2 || values[1] != 20 {
		t.Fatalf("values=%v err=%v", values, err)
	}
}

func TestNormalizeConfigDefaultsAndValidation(t *testing.T) {
	cfg, err := NormalizeConfig(Config{
		Enabled:   true,
		ProjectID: 1,
		DeviceKey: "plc-1",
		Endpoint:  "127.0.0.1:502",
		Points:    []Point{{Name: "temperature", Address: 0, Type: "int16"}},
	})
	if err != nil {
		t.Fatalf("默认配置应通过校验: %v", err)
	}
	if cfg.Transport != "tcp" || cfg.UnitID != 1 || cfg.BaudRate != 9600 || cfg.DataBits != 8 || cfg.StopBits != 1 || cfg.Parity != "none" || cfg.Interval != 10*time.Second || cfg.Timeout != 5*time.Second {
		t.Fatalf("默认值未补齐: %+v", cfg)
	}

	bad := cfg
	bad.Points = []Point{
		{Name: "a", Address: 0, Type: "uint32"},
		{Name: "b", Address: 1, Type: "uint16"},
	}
	if _, err := NormalizeConfig(bad); err == nil {
		t.Fatal("重叠寄存器应被拒绝")
	}

	bad = cfg
	bad.Points = []Point{{Name: "far", Address: 0, Type: "uint16"}, {Name: "too_far", Address: 125, Type: "uint16"}}
	if _, err := NormalizeConfig(bad); err == nil {
		t.Fatal("超过单次读取跨度的点位应被拒绝")
	}
}

func TestNormalizeRTURequiresAbsoluteSerialPath(t *testing.T) {
	_, err := NormalizeConfig(Config{
		Enabled: true, ProjectID: 1, DeviceKey: "rtu-1", Transport: "rtu",
		SerialPath: "ttyUSB0", Points: []Point{{Name: "value", Address: 0, Type: "uint16"}},
	})
	if err == nil {
		t.Fatal("相对串口路径应被拒绝")
	}
}
