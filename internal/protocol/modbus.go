package protocol

import (
	"encoding/binary"
	"fmt"
)

// ModbusCRC16 计算 Modbus RTU CRC-16 (poly 0xA001, init 0xFFFF)。
func ModbusCRC16(data []byte) uint16 {
	crc := uint16(0xffff)
	for _, b := range data {
		crc ^= uint16(b)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xa001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}

func VerifyModbusRTU(frame []byte) error {
	if len(frame) < 4 {
		return fmt.Errorf("modbus rtu 帧过短")
	}
	want := binary.LittleEndian.Uint16(frame[len(frame)-2:])
	if got := ModbusCRC16(frame[:len(frame)-2]); got != want {
		return fmt.Errorf("modbus rtu CRC 错误: got=%04x want=%04x", got, want)
	}
	return nil
}

// DecodeReadHoldingResponse 解码功能码 03/04 的寄存器响应。
func DecodeReadHoldingResponse(frame []byte) ([]uint16, error) {
	if len(frame) < 3 {
		return nil, fmt.Errorf("modbus 响应过短")
	}
	if frame[1]&0x80 != 0 {
		return nil, fmt.Errorf("modbus 异常响应: code=%d", frame[2])
	}
	if frame[1] != 3 && frame[1] != 4 {
		return nil, fmt.Errorf("modbus 功能码不支持: %d", frame[1])
	}
	count := int(frame[2])
	if count == 0 || count%2 != 0 || len(frame) < 3+count {
		return nil, fmt.Errorf("modbus 字节数非法")
	}
	values := make([]uint16, count/2)
	for i := range values {
		values[i] = binary.BigEndian.Uint16(frame[3+i*2:])
	}
	return values, nil
}

// ParseModbusTCP 校验 MBAP 头并返回 PDU（去掉事务/协议/长度/单元标识）。
func ParseModbusTCP(frame []byte) (transaction uint16, unit byte, pdu []byte, err error) {
	if len(frame) < 8 {
		return 0, 0, nil, fmt.Errorf("modbus tcp 帧过短")
	}
	if binary.BigEndian.Uint16(frame[2:4]) != 0 {
		return 0, 0, nil, fmt.Errorf("modbus tcp 协议标识非法")
	}
	length := int(binary.BigEndian.Uint16(frame[4:6]))
	if length < 2 || 6+length != len(frame) {
		return 0, 0, nil, fmt.Errorf("modbus tcp 长度非法")
	}
	return binary.BigEndian.Uint16(frame[:2]), frame[6], append([]byte(nil), frame[7:]...), nil
}
