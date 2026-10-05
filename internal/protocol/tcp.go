package protocol

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// TCPRegistration 是 DTU 长连接的首帧。注册后所有上报帧使用 4 字节大端长度前缀。
type TCPRegistration struct {
	DeviceKey string `json:"device_key"`
	Secret    string `json:"secret"`
}

const MaxTCPFrame = 32 << 10

// ReadTCPRegistration 读取一行 JSON 注册包，拒绝超长或缺少凭据的首包。
func ReadTCPRegistration(r *bufio.Reader) (TCPRegistration, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return TCPRegistration{}, fmt.Errorf("读取 tcp 注册包: %w", err)
	}
	if len(line) > 4096 {
		return TCPRegistration{}, fmt.Errorf("tcp 注册包过长")
	}
	var reg TCPRegistration
	if err := json.Unmarshal(line, &reg); err != nil || reg.DeviceKey == "" || reg.Secret == "" {
		return TCPRegistration{}, fmt.Errorf("tcp 注册包非法")
	}
	return reg, nil
}

// ReadTCPFrame 读取一个长度前缀 JSON/二进制帧。零长度保留给心跳。
func ReadTCPFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 {
		return nil, nil
	}
	if length > MaxTCPFrame {
		return nil, fmt.Errorf("tcp 帧过长: %d", length)
	}
	frame := make([]byte, length)
	if _, err := io.ReadFull(r, frame); err != nil {
		return nil, fmt.Errorf("读取 tcp 帧: %w", err)
	}
	return frame, nil
}

func EncodeTCPFrame(payload []byte) []byte {
	if len(payload) > MaxTCPFrame {
		return nil
	}
	out := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(out, uint32(len(payload)))
	copy(out[4:], payload)
	return out
}
