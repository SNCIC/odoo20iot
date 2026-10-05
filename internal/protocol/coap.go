package protocol

import (
	"encoding/binary"
	"fmt"
	"net/url"
	"strings"
)

// CoAPMessage 是受限但完整的 CoAP 上行解析结果。当前接入只接受 confirmable/non-confirmable
// POST，URI-Path 用于选择 stream，Payload 原样交给统一 JSON 管道。
type CoAPMessage struct {
	Type      byte
	Code      byte
	MessageID uint16
	Token     []byte
	URIPath   string
	Queries   []string
	Payload   []byte
}

const (
	CoAPTypeConfirmable     = 0
	CoAPTypeNonConfirmable  = 1
	CoAPTypeAcknowledgement = 2
	CoAPPost                = 2
	CoAPOptionURIPath       = 11
)

// ParseCoAP 解析 RFC 7252 基本头、token、选项与 payload marker。
func ParseCoAP(data []byte) (CoAPMessage, error) {
	if len(data) < 4 {
		return CoAPMessage{}, fmt.Errorf("coap 报文过短")
	}
	first := data[0]
	if first>>6 != 1 {
		return CoAPMessage{}, fmt.Errorf("coap 版本不支持: %d", first>>6)
	}
	tkl := int(first & 0xf)
	if tkl > 8 || len(data) < 4+tkl {
		return CoAPMessage{}, fmt.Errorf("coap token 长度非法")
	}
	msg := CoAPMessage{Type: (first >> 4) & 3, Token: append([]byte(nil), data[4:4+tkl]...)}
	msg.Code, msg.MessageID = data[1], binary.BigEndian.Uint16(data[2:4])
	if msg.Code != CoAPPost {
		return CoAPMessage{}, fmt.Errorf("coap 仅支持 POST，上行 code=%d", msg.Code)
	}
	idx, optionNumber := 4+tkl, 0
	var paths []string
	for idx < len(data) {
		if data[idx] == 0xff {
			msg.Payload = append([]byte(nil), data[idx+1:]...)
			break
		}
		delta, length, consumed, err := coapOptionHeader(data[idx:])
		if err != nil {
			return CoAPMessage{}, err
		}
		idx += consumed
		if idx+length > len(data) {
			return CoAPMessage{}, fmt.Errorf("coap 选项越界")
		}
		optionNumber += delta
		value := data[idx : idx+length]
		if optionNumber == CoAPOptionURIPath {
			paths = append(paths, string(value))
		} else if optionNumber == 15 {
			msg.Queries = append(msg.Queries, string(value))
		}
		idx += length
	}
	msg.URIPath = "/" + strings.Join(paths, "/")
	return msg, nil
}

// Credential 返回 Uri-Query 中的 token；短 Token 仅作为本地联调兼容方案。
func (m CoAPMessage) Credential() string {
	for _, raw := range m.Queries {
		key, value, ok := strings.Cut(raw, "=")
		if ok && key == "token" {
			if decoded, err := url.QueryUnescape(value); err == nil {
				return decoded
			}
			return value
		}
	}
	return string(m.Token)
}

func coapOptionHeader(data []byte) (delta, length, consumed int, err error) {
	if len(data) == 0 {
		return 0, 0, 0, fmt.Errorf("coap 选项为空")
	}
	delta, length = int(data[0]>>4), int(data[0]&0xf)
	consumed = 1
	var extra func(int) (int, error)
	extra = func(v int) (int, error) {
		switch v {
		case 13:
			if len(data) <= consumed {
				return 0, fmt.Errorf("coap 选项扩展缺失")
			}
			x := int(data[consumed]) + 13
			consumed++
			return x, nil
		case 14:
			if len(data) < consumed+2 {
				return 0, fmt.Errorf("coap 选项扩展缺失")
			}
			x := int(binary.BigEndian.Uint16(data[consumed:consumed+2])) + 269
			consumed += 2
			return x, nil
		case 15:
			return 0, fmt.Errorf("coap 保留选项扩展")
		default:
			return v, nil
		}
	}
	if delta, err = extra(delta); err != nil {
		return 0, 0, 0, err
	}
	if length, err = extra(length); err != nil {
		return 0, 0, 0, err
	}
	return delta, length, consumed, nil
}

// BuildAck 为 confirmable 请求生成空 ACK；上层在持久化后再发送业务响应。
func BuildAck(msg CoAPMessage) []byte {
	if len(msg.Token) > 8 {
		return nil
	}
	out := []byte{byte(1<<6) | byte(CoAPTypeAcknowledgement<<4) | byte(len(msg.Token)), 0, byte(msg.MessageID >> 8), byte(msg.MessageID)}
	return append(out, msg.Token...)
}
