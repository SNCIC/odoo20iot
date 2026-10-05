package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const (
	HTTPFormatJSON    = "json"
	HTTPFormatScanner = "scanner-text"
	HTTPFormatDTUKV   = "dtu-kv"
)

func normalizeHTTPPayload(format, contentType string, body []byte) ([]byte, error) {
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = HTTPFormatJSON
	}
	switch format {
	case HTTPFormatJSON:
		if !json.Valid(body) {
			return nil, fmt.Errorf("JSON 载荷非法")
		}
		return bytes.Clone(body), nil
	case HTTPFormatScanner:
		if len(body) == 0 || !isTextContentType(contentType) {
			return nil, fmt.Errorf("扫码枪载荷必须是文本")
		}
		value := strings.TrimSpace(string(body))
		if value == "" || strings.ContainsAny(value, "\r\n") {
			return nil, fmt.Errorf("扫码值为空或包含多行")
		}
		return json.Marshal(map[string]any{"barcode": value, "raw": value})
	case HTTPFormatDTUKV:
		if !isTextContentType(contentType) {
			return nil, fmt.Errorf("DTU 键值载荷必须是文本")
		}
		return parseDTUKV(body)
	default:
		return nil, fmt.Errorf("不支持的 HTTP 设备格式 %q", format)
	}
}

func parseDTUKV(body []byte) ([]byte, error) {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return nil, fmt.Errorf("DTU 载荷为空")
	}
	text = strings.ReplaceAll(text, "\r\n", "&")
	text = strings.ReplaceAll(text, "\n", "&")
	text = strings.ReplaceAll(text, ";", "&")
	values, err := url.ParseQuery(text)
	if err != nil {
		return nil, fmt.Errorf("DTU 键值载荷非法: %w", err)
	}
	result := make(map[string]any, len(values))
	for key, list := range values {
		key = strings.TrimSpace(key)
		if key == "" || strings.ContainsAny(key, "{}[]\"\x00") || len(list) != 1 {
			return nil, fmt.Errorf("DTU 字段名或值非法")
		}
		result[key] = parseScalar(list[0])
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("DTU 载荷没有字段")
	}
	return json.Marshal(result)
}

func parseScalar(value string) any {
	value = strings.TrimSpace(value)
	if integer, err := strconv.ParseInt(value, 10, 64); err == nil {
		return integer
	}
	if number, err := strconv.ParseFloat(value, 64); err == nil {
		return number
	}
	if value == "true" {
		return true
	}
	if value == "false" {
		return false
	}
	return value
}

func isTextContentType(contentType string) bool {
	contentType = strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	return contentType == "" || contentType == "text/plain" || contentType == "application/x-www-form-urlencoded"
}
