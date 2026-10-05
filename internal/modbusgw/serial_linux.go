//go:build linux

package modbusgw

import (
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type SerialConfig struct {
	Path     string
	BaudRate int
	DataBits int
	StopBits int
	Parity   string
}

func OpenSerial(cfg SerialConfig) (RTUTransport, error) {
	if strings.TrimSpace(cfg.Path) == "" {
		return nil, fmt.Errorf("串口路径不能为空")
	}
	if cfg.BaudRate == 0 {
		cfg.BaudRate = 9600
	}
	if cfg.DataBits == 0 {
		cfg.DataBits = 8
	}
	if cfg.StopBits == 0 {
		cfg.StopBits = 1
	}
	if cfg.Parity == "" {
		cfg.Parity = "none"
	}
	speed, ok := serialSpeed(cfg.BaudRate)
	if !ok {
		return nil, fmt.Errorf("不支持的波特率: %d", cfg.BaudRate)
	}
	if cfg.DataBits < 5 || cfg.DataBits > 8 || (cfg.StopBits != 1 && cfg.StopBits != 2) {
		return nil, fmt.Errorf("串口数据位/停止位非法")
	}
	if cfg.Parity != "none" && cfg.Parity != "even" && cfg.Parity != "odd" {
		return nil, fmt.Errorf("串口校验位非法: %s", cfg.Parity)
	}
	file, err := os.OpenFile(cfg.Path, os.O_RDWR|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("打开串口 %s: %w", cfg.Path, err)
	}
	term, err := unix.IoctlGetTermios(int(file.Fd()), unix.TCGETS)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("读取串口参数: %w", err)
	}
	term.Iflag = 0
	term.Oflag = 0
	term.Lflag = 0
	term.Cflag = unix.CLOCAL | unix.CREAD | serialDataBits(cfg.DataBits)
	if cfg.StopBits == 2 {
		term.Cflag |= unix.CSTOPB
	}
	if cfg.Parity != "none" {
		term.Cflag |= unix.PARENB
		if cfg.Parity == "odd" {
			term.Cflag |= unix.PARODD
		}
	}
	term.Ispeed, term.Ospeed = uint32(speed), uint32(speed)
	term.Cc[unix.VMIN], term.Cc[unix.VTIME] = 0, 0
	if err := unix.IoctlSetTermios(int(file.Fd()), unix.TCSETS, term); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("设置串口参数: %w", err)
	}
	return file, nil
}

func serialDataBits(bits int) uint32 {
	switch bits {
	case 5:
		return unix.CS5
	case 6:
		return unix.CS6
	case 7:
		return unix.CS7
	default:
		return unix.CS8
	}
}

func serialSpeed(value int) (uint32, bool) {
	switch value {
	case 1200:
		return unix.B1200, true
	case 2400:
		return unix.B2400, true
	case 4800:
		return unix.B4800, true
	case 9600:
		return unix.B9600, true
	case 19200:
		return unix.B19200, true
	case 38400:
		return unix.B38400, true
	case 57600:
		return unix.B57600, true
	case 115200:
		return unix.B115200, true
	default:
		return 0, false
	}
}

var _ RTUTransport = (interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
	SetDeadline(time.Time) error
})(nil)
