package notify

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestVoiceChannelSendsGatewayPayload(t *testing.T) {
	var got string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
		}
		buf, _ := io.ReadAll(r.Body)
		got = string(buf)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"message":"accepted"}`))
	}))
	defer server.Close()

	channel := NewVoiceChannel(&Guard{AllowedHosts: []string{"127.0.0.1"}, AllowLoopback: true}, server.URL, "secret", "+8613800000000", 2e9)
	err := channel.Send(context.Background(), Message{AlarmID: "a-1", TraceID: "t-1", ProjectID: "1", DeviceID: "d-1", Level: "critical", Subject: "设备异常", Body: "温度过高"}, []string{"+8613900000000"})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	for _, want := range []string{"a-1", "t-1", "+8613900000000", "温度过高"} {
		if !strings.Contains(got, want) {
			t.Errorf("payload missing %q: %s", want, got)
		}
	}
}

func TestClassifyVoiceBody(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		fail bool
	}{
		{name: "code success", body: `{"code":0}`},
		{name: "success true", body: `{"success":true}`},
		{name: "code failure", body: `{"code":1001,"message":"quota"}`, fail: true},
		{name: "success false", body: `{"success":false,"message":"rejected"}`, fail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := classifyVoiceBody(test.body); (err != nil) != test.fail {
				t.Fatalf("classifyVoiceBody() error = %v, want failure=%v", err, test.fail)
			}
		})
	}
}

func TestVoiceChannelRequiresConfiguration(t *testing.T) {
	channel := NewVoiceChannel(&Guard{}, "", "", "", 0)
	if err := channel.Send(context.Background(), Message{}, []string{"+8613900000000"}); !IsPermanent(err) {
		t.Fatalf("unconfigured voice channel error = %v, want permanent", err)
	}
}
