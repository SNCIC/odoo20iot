//go:build !linux

package modbusgw

import (
	"fmt"
)

type SerialConfig struct {
	Path     string
	BaudRate int
	DataBits int
	StopBits int
	Parity   string
}

func OpenSerial(SerialConfig) (RTUTransport, error) {
	return nil, fmt.Errorf("当前平台不支持原生串口")
}
