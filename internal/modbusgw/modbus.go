package modbusgw

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync/atomic"
	"time"

	"github.com/SNCIC/odoo20iot/internal/protocol"
)

type Point struct {
	Name    string  `json:"name"`
	Address uint16  `json:"address"`
	Type    string  `json:"type"`
	Scale   float64 `json:"scale,omitempty"`
	Offset  float64 `json:"offset,omitempty"`
	Unit    string  `json:"unit,omitempty"`
}

type Reader interface {
	ReadHolding(context.Context, byte, uint16, uint16) ([]uint16, error)
}

// RTUTransport 是串口或串口转网关连接的最小读写契约。
// 串口库只负责打开设备，这里负责 Modbus RTU 帧、超时和 CRC。
type RTUTransport interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
}

type RTUClient struct {
	Transport RTUTransport
	Timeout   time.Duration
}

func (c *RTUClient) ReadHolding(ctx context.Context, unit byte, address, count uint16) ([]uint16, error) {
	return ReadHoldingRTU(ctx, c.Transport, unit, address, count, c.Timeout)
}

// ReadHoldingRTU 读取 RTU 保持寄存器。响应严格校验 unit、功能码、字节数和 CRC。
func ReadHoldingRTU(ctx context.Context, transport RTUTransport, unit byte, address, count uint16, timeout time.Duration) ([]uint16, error) {
	if transport == nil {
		return nil, fmt.Errorf("modbus rtu transport 为空")
	}
	if count == 0 || count > 125 {
		return nil, fmt.Errorf("modbus 读取数量必须为 1..125")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.Now().Add(timeout)
	if err := transport.SetDeadline(deadline); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	request := []byte{unit, 3, byte(address >> 8), byte(address), byte(count >> 8), byte(count)}
	crc := protocol.ModbusCRC16(request)
	request = append(request, byte(crc), byte(crc>>8))
	if _, err := transport.Write(request); err != nil {
		return nil, fmt.Errorf("发送 Modbus RTU 请求: %w", err)
	}
	header := make([]byte, 3)
	if _, err := io.ReadFull(transport, header); err != nil {
		return nil, fmt.Errorf("读取 Modbus RTU 响应: %w", err)
	}
	if header[0] != unit {
		return nil, fmt.Errorf("Modbus RTU unit 不匹配")
	}
	if header[1]&0x80 != 0 {
		tail := make([]byte, 2)
		_, _ = io.ReadFull(transport, tail)
		return nil, fmt.Errorf("Modbus RTU 异常码: %d", header[2])
	}
	if header[1] != 3 || int(header[2]) != int(count)*2 {
		return nil, fmt.Errorf("Modbus RTU 响应字节数非法")
	}
	body := make([]byte, int(header[2])+2)
	if _, err := io.ReadFull(transport, body); err != nil {
		return nil, fmt.Errorf("读取 Modbus RTU 数据: %w", err)
	}
	frame := append(header, body...)
	if err := protocol.VerifyModbusRTU(frame); err != nil {
		return nil, err
	}
	values := make([]uint16, count)
	for i := range values {
		values[i] = binary.BigEndian.Uint16(frame[3+i*2:])
	}
	return values, nil
}

type Config struct {
	ID           int64         `json:"id,omitempty"`
	Enabled      bool          `json:"enabled"`
	Transport    string        `json:"transport"`
	ProjectID    int64         `json:"project_id"`
	DeviceKey    string        `json:"device_key"`
	DeviceID     int64         `json:"device_id"`
	DeviceTypeID int64         `json:"device_type_id"`
	Endpoint     string        `json:"endpoint"`
	SerialPath   string        `json:"serial_path,omitempty"`
	BaudRate     int           `json:"baud_rate,omitempty"`
	DataBits     int           `json:"data_bits,omitempty"`
	StopBits     int           `json:"stop_bits,omitempty"`
	Parity       string        `json:"parity,omitempty"`
	UnitID       byte          `json:"unit_id"`
	Interval     time.Duration `json:"interval"`
	Timeout      time.Duration `json:"timeout,omitempty"`
	Points       []Point       `json:"points"`
}

type Client struct {
	Endpoint string
	Timeout  time.Duration
	seq      atomic.Uint32
}

func (c *Client) ReadHolding(ctx context.Context, unit byte, address, count uint16) ([]uint16, error) {
	if count == 0 || count > 125 {
		return nil, fmt.Errorf("modbus 读取数量必须为 1..125")
	}
	if c.Endpoint == "" {
		return nil, fmt.Errorf("modbus endpoint 为空")
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	conn, err := (&net.Dialer{Timeout: timeout}).DialContext(ctx, "tcp", c.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("连接 Modbus TCP %s: %w", c.Endpoint, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	tx := uint16(c.seq.Add(1))
	request := make([]byte, 12)
	binary.BigEndian.PutUint16(request[0:2], tx)
	binary.BigEndian.PutUint16(request[4:6], 6)
	request[6], request[7] = unit, 3
	binary.BigEndian.PutUint16(request[8:10], address)
	binary.BigEndian.PutUint16(request[10:12], count)
	if _, err := conn.Write(request); err != nil {
		return nil, fmt.Errorf("发送 Modbus 请求: %w", err)
	}
	header := make([]byte, 7)
	if _, err := readFull(conn, header); err != nil {
		return nil, fmt.Errorf("读取 Modbus 响应: %w", err)
	}
	if binary.BigEndian.Uint16(header[0:2]) != tx || binary.BigEndian.Uint16(header[2:4]) != 0 || header[6] != unit {
		return nil, fmt.Errorf("Modbus 响应事务/协议/单元不匹配")
	}
	length := int(binary.BigEndian.Uint16(header[4:6]))
	if length < 2 || length > 252 {
		return nil, fmt.Errorf("Modbus 响应长度非法: %d", length)
	}
	pdu := make([]byte, length-1)
	if _, err := readFull(conn, pdu); err != nil {
		return nil, fmt.Errorf("读取 Modbus PDU: %w", err)
	}
	frame := append([]byte{unit}, pdu...)
	return decodeResponse(frame, count)
}

func decodeResponse(frame []byte, count uint16) ([]uint16, error) {
	if len(frame) < 3 {
		return nil, fmt.Errorf("Modbus PDU 过短")
	}
	if frame[1]&0x80 != 0 {
		return nil, fmt.Errorf("Modbus 异常码: %d", frame[2])
	}
	if frame[1] != 3 {
		return nil, fmt.Errorf("Modbus 功能码不支持: %d", frame[1])
	}
	if int(frame[2]) != int(count)*2 || len(frame) != 3+int(frame[2]) {
		return nil, fmt.Errorf("Modbus 字节数与请求数量不匹配")
	}
	values := make([]uint16, count)
	for i := range values {
		values[i] = binary.BigEndian.Uint16(frame[3+i*2:])
	}
	return values, nil
}

func Decode(values []uint16, point Point) (float64, error) {
	if point.Name == "" {
		return 0, fmt.Errorf("Modbus 点位缺少 name")
	}
	width := 1
	switch point.Type {
	case "uint16", "int16":
		width = 1
	case "uint32", "int32", "float32":
		width = 2
	default:
		return 0, fmt.Errorf("不支持的 Modbus 类型 %q", point.Type)
	}
	if int(point.Address)+width > len(values) {
		return 0, fmt.Errorf("点位 %s 超出响应范围", point.Name)
	}
	var value float64
	if width == 1 {
		if point.Type == "int16" {
			value = float64(int16(values[point.Address]))
		} else {
			value = float64(values[point.Address])
		}
	} else {
		var raw [4]byte
		binary.BigEndian.PutUint16(raw[:2], values[point.Address])
		binary.BigEndian.PutUint16(raw[2:], values[point.Address+1])
		switch point.Type {
		case "uint32":
			value = float64(binary.BigEndian.Uint32(raw[:]))
		case "int32":
			value = float64(int32(binary.BigEndian.Uint32(raw[:])))
		case "float32":
			value = float64(math.Float32frombits(binary.BigEndian.Uint32(raw[:])))
		}
	}
	if point.Scale == 0 {
		point.Scale = 1
	}
	return value*point.Scale + point.Offset, nil
}

func Poll(ctx context.Context, cfg Config, reader Reader, emit func(context.Context, map[string]any) error) error {
	if cfg.ProjectID <= 0 || cfg.DeviceKey == "" || len(cfg.Points) == 0 {
		return fmt.Errorf("Modbus 配置缺少 project_id/device_key/points")
	}
	if cfg.UnitID == 0 {
		cfg.UnitID = 1
	}
	if reader == nil {
		return fmt.Errorf("Modbus reader 为空")
	}
	if emit == nil {
		return fmt.Errorf("Modbus emit 为空")
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	for {
		values, err := reader.ReadHolding(ctx, cfg.UnitID, minAddress(cfg.Points), maxAddress(cfg.Points)-minAddress(cfg.Points)+1)
		if err == nil {
			payload := make(map[string]any, len(cfg.Points))
			base := minAddress(cfg.Points)
			for _, point := range cfg.Points {
				point.Address -= base
				value, decodeErr := Decode(values, point)
				if decodeErr != nil {
					err = decodeErr
					break
				}
				payload[point.Name] = value
			}
			if err == nil {
				err = emit(ctx, payload)
			}
		}
		if err != nil {
			return err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func minAddress(points []Point) uint16 {
	out := points[0].Address
	for _, p := range points[1:] {
		if p.Address < out {
			out = p.Address
		}
	}
	return out
}
func maxAddress(points []Point) uint16 {
	out := points[0].Address
	for _, p := range points {
		width := uint16(1)
		if p.Type == "uint32" || p.Type == "int32" || p.Type == "float32" {
			width = 2
		}
		if p.Address+width-1 > out {
			out = p.Address + width - 1
		}
	}
	return out
}
func readFull(conn net.Conn, buf []byte) (int, error) { return io.ReadFull(conn, buf) }
