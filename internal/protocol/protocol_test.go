package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"
)

func TestParseCoAP(t *testing.T) {
	// CON POST id=0x1234, token=ab, Uri-Path=v1/telemetry, payload JSON。
	b := []byte{0x41, CoAPPost, 0x12, 0x34, 0xab, 0xb2, 'v', '1', 0x09, 't', 'e', 'l', 'e', 'm', 'e', 't', 'r', 'y', 0xff, '{', '"', 'x', '"', ':', '1', '}'}
	m, err := ParseCoAP(b)
	if err != nil || m.URIPath != "/v1/telemetry" || string(m.Payload) != `{"x":1}` || m.MessageID != 0x1234 {
		t.Fatalf("parse = %#v, err=%v", m, err)
	}
}

func TestIngressFrames(t *testing.T) {
	frame, err := ParseCoAPIngress(CoAPMessage{URIPath: "/v1/gateways/gw-1/devices/sub-7/telemetry", Payload: []byte(`{"temperature":25}`)})
	if err != nil || frame.GatewayKey != "gw-1" || frame.SubKey != "sub-7" || frame.Stream != "telemetry" {
		t.Fatalf("coap ingress = %#v, err=%v", frame, err)
	}
	frame, err = ParseTCPIngress("d1", []byte(`{"stream":"attributes","payload":{"mode":"auto"}}`))
	if err != nil || frame.Stream != "attributes" || string(frame.Payload) != `{"mode":"auto"}` {
		t.Fatalf("tcp ingress = %#v, err=%v", frame, err)
	}
}

func TestBuildAckEchoesToken(t *testing.T) {
	ack := BuildAck(CoAPMessage{MessageID: 9, Token: []byte{1, 2}})
	if len(ack) != 6 || ack[4] != 1 || ack[5] != 2 {
		t.Fatalf("ack = %x", ack)
	}
}

func TestTCPFrames(t *testing.T) {
	frame := EncodeTCPFrame([]byte(`{"x":1}`))
	got, err := ReadTCPFrame(bytes.NewReader(frame))
	if err != nil || string(got) != `{"x":1}` {
		t.Fatalf("frame = %q, err=%v", got, err)
	}
	reg, err := ReadTCPRegistration(bufio.NewReader(bytes.NewBufferString(`{"device_key":"d1","secret":"s"}` + "\n")))
	if err != nil || reg.DeviceKey != "d1" {
		t.Fatalf("reg = %#v, err=%v", reg, err)
	}
}

func TestModbus(t *testing.T) {
	data := []byte{1, 3, 4, 0, 10, 0, 20}
	crc := ModbusCRC16(data)
	frame := append(data, byte(crc), byte(crc>>8))
	if err := VerifyModbusRTU(frame); err != nil {
		t.Fatal(err)
	}
	values, err := DecodeReadHoldingResponse(data[0:])
	if err != nil || len(values) != 2 || values[1] != 20 {
		t.Fatalf("values=%v err=%v", values, err)
	}
	tcp := make([]byte, 8)
	binary.BigEndian.PutUint16(tcp[0:2], 7)
	binary.BigEndian.PutUint16(tcp[4:6], 2)
	tcp[6] = 1
	tcp[7] = 3
	if tx, unit, pdu, err := ParseModbusTCP(tcp); err != nil || tx != 7 || unit != 1 || len(pdu) != 1 {
		t.Fatalf("tcp tx=%d unit=%d pdu=%v err=%v", tx, unit, pdu, err)
	}
}
