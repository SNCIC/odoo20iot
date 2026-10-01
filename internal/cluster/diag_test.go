package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// TestDiag_路由通道 是一次性诊断：把「发布到路由 subject」与「durable 消费」
// 这两步拆开验证，定位跨节点投递为何不通。
func TestDiag_路由通道(t *testing.T) {
	url := natsURL(t)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream 初始化失败: %v", err)
	}

	runID := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
	stream := "DIAG_ROUTE_" + runID
	prefix := "diag.route." + runID
	t.Cleanup(func() { _ = js.DeleteStream(stream) })

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:      stream,
		Subjects:  []string{prefix + ".>"},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		MaxAge:    time.Hour,
	}); err != nil {
		t.Fatalf("建流失败: %v", err)
	}

	const nodeID = "n2-diag"
	subject := prefix + "." + nodeID

	// 与生产代码完全一致的订阅参数
	sub, err := js.PullSubscribe(subject, "gw-route-"+nodeID,
		nats.BindStream(stream),
		nats.ManualAck(),
		nats.AckWait(time.Second),
		nats.MaxDeliver(-1),
	)
	if err != nil {
		t.Fatalf("PullSubscribe 失败: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	ci, err := js.ConsumerInfo(stream, "gw-route-"+nodeID)
	if err != nil {
		t.Fatalf("读取消费者信息失败: %v", err)
	}
	t.Logf("消费者：filter=%q deliver=%v ack=%v maxdeliver=%d",
		ci.Config.FilterSubject, ci.Config.DeliverPolicy, ci.Config.AckPolicy, ci.Config.MaxDeliver)

	// 发布一条
	pa, err := js.Publish(subject, []byte(`{"hello":1}`), nats.Context(context.Background()))
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	t.Logf("发布成功：stream=%s seq=%d", pa.Stream, pa.Sequence)

	// 拉取
	msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}
	t.Logf("Fetch 到 %d 条：%s", len(msgs), string(msgs[0].Data))

	// 再看一次：带 MaxDeliver(-1) 的配置是否被服务端接受
	si, err := js.StreamInfo(stream)
	if err != nil {
		t.Fatalf("读取流信息失败: %v", err)
	}
	t.Logf("流消息数=%d 消费者数=%d", si.State.Msgs, si.State.Consumers)
}
