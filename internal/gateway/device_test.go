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

// close 主动断开连接（模拟设备下线）。
func (d *testDevice) close() {
	_ = d.conn.Close()
}

// connect 发送不带凭据的 CONNECT 并校验 CONNACK 成功。
func (d *testDevice) connect(clientID string) {
	d.t.Helper()

	code := d.connectWith(clientID, "", "")
	if code != packets.CodeSuccess.Code {
		d.t.Fatalf("CONNACK 拒绝连接，reason=%d", code)
	}
}

// connectWith 发送带凭据的 CONNECT，返回 CONNACK 的返回码（成功为 0）。
//
// 返回码而不是直接 Fatal，是为了让「应当被拒绝」的用例能断言具体码值。
func (d *testDevice) connectWith(clientID, username, password string) byte {
	d.t.Helper()

	params := packets.ConnectParams{
		ProtocolName:     []byte("MQTT"),
		ClientIdentifier: clientID,
		Keepalive:        60,
		Clean:            true,
	}
	if username != "" {
		params.Username = []byte(username)
		params.UsernameFlag = true
	}
	if password != "" {
		params.Password = []byte(password)
		params.PasswordFlag = true
	}

	d.write(&packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 4,
		Connect:         params,
	})

	got, err := d.read(3 * time.Second)
	if err != nil {
		d.t.Fatalf("等待 CONNACK 失败: %v", err)
	}
	if got.FixedHeader.Type != packets.Connack {
		d.t.Fatalf("期望 CONNACK，得到报文类型 %d", got.FixedHeader.Type)
	}
	return got.ReasonCode
}

// subscribe 发送 SUBSCRIBE，返回 SUBACK 里该过滤器的返回码。
//
// ⚠️ MQTT 3.1.1 的 SUBACK 返回码语义容易看错：
//
//	0x00/0x01/0x02 = **授予的 QoS**（成功），0x80 = 失败。
//
// 因此「成功」的判据是 `< 0x80`，不是 `== 0`。
func (d *testDevice) subscribe(filter string, id uint16) byte {
	d.t.Helper()

	d.write(&packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		ProtocolVersion: 4,
		PacketID:        id,
		Filters: packets.Subscriptions{
			{Filter: filter, Qos: 1, Identifier: 1},
		},
	})

	got, err := d.read(3 * time.Second)
	if err != nil {
		d.t.Fatalf("等待 SUBACK 失败: %v", err)
	}
	if got.FixedHeader.Type != packets.Suback {
		d.t.Fatalf("期望 SUBACK，得到报文类型 %d", got.FixedHeader.Type)
	}
	if len(got.ReasonCodes) == 0 {
		d.t.Fatal("SUBACK 未带返回码")
	}
	return got.ReasonCodes[0]
}

// expectClosed 断言连接被服务端关闭（或在关闭前收到了 DISCONNECT）。
//
// 用于验证「越权发布」这类必须立即断开的场景。
func (d *testDevice) expectClosed(window time.Duration) {
	d.t.Helper()

	pk, err := d.read(window)
	if err == nil && pk.FixedHeader.Type == packets.Disconnect {
		return // v5 会先发 DISCONNECT 再断开
	}
	if err == nil {
		d.t.Fatalf("期望连接被关闭，却收到了报文类型 %d", pk.FixedHeader.Type)
	}
	if errors.Is(err, errNoPacket) {
		d.t.Fatal("期望连接被关闭，但连接仍然存活（读取超时）")
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
	case packets.Subscribe:
		err = pk.SubscribeEncode(&buf)
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
	case packets.Suback:
		err = pk.SubackDecode(body)
	default:
		// 其他类型在本测试中不解码，保留 FixedHeader 供断言使用。
	}
	return pk, err
}
