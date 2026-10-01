package gateway

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
)

// testDevice 是最小化的 MQTT 3.1.1 客户端，只覆盖 A2 验证所需的报文：
// CONNECT / CONNACK / PUBLISH(QoS1) / PUBACK。
//
// 刻意不复用第三方客户端库：A2 要观察的恰恰是「PUBACK 到底有没有来、什么时候来」，
// 需要一个能精确控制读超时、并能手工置 DUP 位的裸客户端。
type testDevice struct {
	t      *testing.T
	conn   net.Conn
	r      *bufio.Reader
	nextID uint16
}

func dialTestDevice(t *testing.T, addr, clientID string) *testDevice {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接 broker %s 失败: %v", addr, err)
	}

	d := &testDevice{t: t, conn: conn, r: bufio.NewReader(conn), nextID: 1}
	t.Cleanup(func() { _ = conn.Close() })
	return d
}

// connect 发送 CONNECT 并校验 CONNACK。
func (d *testDevice) connect(clientID string) {
	d.t.Helper()

	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 4,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: clientID,
			Keepalive:        60,
			Clean:            true,
		},
	}
	d.write(&pk)

	got, err := d.read(3 * time.Second)
	if err != nil {
		d.t.Fatalf("等待 CONNACK 失败: %v", err)
	}
	if got.FixedHeader.Type != packets.Connack {
		d.t.Fatalf("期望 CONNACK，得到报文类型 %d", got.FixedHeader.Type)
	}
	if got.ReasonCode != packets.CodeSuccess.Code {
		d.t.Fatalf("CONNACK 拒绝连接，reason=%d", got.ReasonCode)
	}
}

// publish 发送一条 QoS1 PUBLISH，返回使用的 packet id。
func (d *testDevice) publish(topic string, payload []byte, dup bool) uint16 {
	d.t.Helper()

	id := d.nextID
	d.nextID++

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Publish,
			Qos:  1,
			Dup:  dup,
		},
		ProtocolVersion: 4,
		TopicName:       topic,
		PacketID:        id,
		Payload:         payload,
	}
	d.write(&pk)
	return id
}

// sendQos2 发送一条 QoS2 PUBLISH（端侧契约之外的报文，用于验证拒绝分支）。
func (d *testDevice) sendQos2(topic string, payload []byte, id uint16) {
	d.t.Helper()

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Publish,
			Qos:  2,
		},
		ProtocolVersion: 4,
		TopicName:       topic,
		PacketID:        id,
		Payload:         payload,
	}
	d.write(&pk)
}

// packetWithTopic 构造一个仅用于路由单测的最小报文。
func packetWithTopic(topic string) packets.Packet {
	return packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName:   topic,
	}
}

// awaitPuback 在给定超时内等待指定 packet id 的 PUBACK。
func (d *testDevice) awaitPuback(id uint16, timeout time.Duration) error {
	d.t.Helper()

	got, err := d.read(timeout)
	if err != nil {
		return err
	}
	if got.FixedHeader.Type != packets.Puback {
		d.t.Fatalf("期望 PUBACK，得到报文类型 %d", got.FixedHeader.Type)
	}
	if got.PacketID != id {
		d.t.Fatalf("PUBACK packet id 不匹配：期望 %d，得到 %d", id, got.PacketID)
	}
	return nil
}

// errNoPacket 表示在超时窗口内没有收到任何报文（即「未回 PUBACK」）。
var errNoPacket = errors.New("超时窗口内未收到 PUBACK")

// assertNoPuback 断言在窗口内**没有**收到 PUBACK。
//
// 注意：这里必须区分「没收到」与「连接被关」—— 后者意味着设备会走重连重传，
// 而不是靠 inflight 窗口重传，两者在 A2 中的含义不同。
func (d *testDevice) assertNoPuback(window time.Duration) {
	d.t.Helper()

	_, err := d.read(window)
	switch {
	case errors.Is(err, errNoPacket):
		return
	case err != nil:
		d.t.Fatalf("期望「未收到 PUBACK」，但读取报错: %v", err)
	default:
		d.t.Fatalf("期望「未收到 PUBACK」，但窗口 %s 内收到了报文", window)
	}
}

func (d *testDevice) write(pk *packets.Packet) {
	d.t.Helper()

	var buf bytes.Buffer
	var err error
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(&buf)
	case packets.Publish:
		err = pk.PublishEncode(&buf)
	default:
		d.t.Fatalf("测试客户端不支持发送报文类型 %d", pk.FixedHeader.Type)
	}
	if err != nil {
		d.t.Fatalf("编码报文失败: %v", err)
	}
	if _, err := d.conn.Write(buf.Bytes()); err != nil {
		d.t.Fatalf("写入报文失败: %v", err)
	}
}

// read 读取一个报文；超时返回 errNoPacket。
func (d *testDevice) read(timeout time.Duration) (packets.Packet, error) {
	if err := d.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return packets.Packet{}, err
	}

	hb, err := d.r.ReadByte()
	if err != nil {
		var nerr net.Error
		if errors.As(err, &nerr) && nerr.Timeout() {
			return packets.Packet{}, errNoPacket
		}
		return packets.Packet{}, err
	}

	var fh packets.FixedHeader
	if err := fh.Decode(hb); err != nil {
		return packets.Packet{}, err
	}

	n, _, err := packets.DecodeLength(d.r)
	if err != nil {
		return packets.Packet{}, err
	}

	body := make([]byte, n)
	if _, err := io.ReadFull(d.r, body); err != nil {
		return packets.Packet{}, err
	}

	pk := packets.Packet{FixedHeader: fh}
	switch fh.Type {
	case packets.Connack:
		err = pk.ConnackDecode(body)
	case packets.Puback:
		err = pk.PubackDecode(body)
	default:
		// 其他类型在本测试中不解码，保留 FixedHeader 供断言使用。
	}
	return pk, err
}
