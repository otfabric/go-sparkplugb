// SPDX-License-Identifier: MIT

package host

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/mqtt"
	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

type mockTransport struct {
	mu         sync.Mutex
	connected  bool
	published  []publishedMsg
	subscribed []subscribedTopic
	handlers   map[string]mqtt.MessageHandler
	opts       mqtt.ConnectOpts
}

type publishedMsg struct {
	Topic    string
	QoS      byte
	Retained bool
	Payload  []byte
}

type subscribedTopic struct {
	Topic string
	QoS   byte
}

func newMockTransport() *mockTransport {
	return &mockTransport{handlers: make(map[string]mqtt.MessageHandler)}
}

func (m *mockTransport) Connect(_ context.Context, opts mqtt.ConnectOpts) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connected = true
	m.opts = opts
	return nil
}

func (m *mockTransport) Publish(_ context.Context, t string, qos byte, retained bool, p []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, publishedMsg{t, qos, retained, p})
	return nil
}

func (m *mockTransport) Subscribe(_ context.Context, t string, qos byte, handler mqtt.MessageHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribed = append(m.subscribed, subscribedTopic{t, qos})
	m.handlers[t] = handler
	return nil
}

func (m *mockTransport) Disconnect(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connected = false
	return nil
}

func (m *mockTransport) deliverToAll(topicStr string, data []byte) {
	m.mu.Lock()
	handlers := make([]mqtt.MessageHandler, 0, len(m.handlers))
	for _, h := range m.handlers {
		handlers = append(handlers, h)
	}
	m.mu.Unlock()
	for _, h := range handlers {
		h(topicStr, data)
	}
}

func (m *mockTransport) getPublished() []publishedMsg {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]publishedMsg, len(m.published))
	copy(out, m.published)
	return out
}

func testConfig() Config {
	return Config{
		HostID:    "testHost",
		BrokerURL: "tcp://localhost:1883",
	}
}

func TestHostConnectAndState(t *testing.T) {
	tr := newMockTransport()
	app, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := app.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	if app.State() != Connected {
		t.Errorf("state = %s, want connected", app.State())
	}

	if tr.opts.WillQoS != 1 {
		t.Errorf("Will QoS = %d, want 1", tr.opts.WillQoS)
	}
	if !tr.opts.WillRetained {
		t.Error("Will should be retained")
	}
	if !tr.opts.CleanSession {
		t.Error("clean session should be true")
	}
	expectedWillTopic := "spBv1.0/STATE/testHost"
	if tr.opts.WillTopic != expectedWillTopic {
		t.Errorf("Will topic = %q, want %q", tr.opts.WillTopic, expectedWillTopic)
	}

	msgs := tr.getPublished()
	found := false
	for _, msg := range msgs {
		if msg.Topic == expectedWillTopic && msg.QoS == 1 && msg.Retained {
			found = true
		}
	}
	if !found {
		t.Error("STATE Birth not published with QoS 1 retained")
	}
}

func TestHostDisconnectPublishesStateDeath(t *testing.T) {
	tr := newMockTransport()
	app, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = app.Connect(ctx)
	_ = app.Disconnect(ctx)

	msgs := tr.getPublished()
	last := msgs[len(msgs)-1]
	if last.Topic != "spBv1.0/STATE/testHost" {
		t.Errorf("last topic = %q", last.Topic)
	}
	if last.QoS != 1 {
		t.Errorf("STATE Death QoS = %d, want 1", last.QoS)
	}
	if !last.Retained {
		t.Error("STATE Death should be retained")
	}
}

func TestHostNodeDiscovery(t *testing.T) {
	tr := newMockTransport()
	var birthGroup, birthNode string
	cfg := testConfig()
	cfg.OnNodeBirth = func(g, n string, _ []*pb.Payload_Metric) {
		birthGroup = g
		birthNode = n
	}

	app, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = app.Connect(ctx)

	tempMetric, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(22.5))
	bdSeq := payload.BdSeqMetric(0)
	p := payload.NewPayload(0, bdSeq, tempMetric)
	b, _ := payload.Encode(p)

	nbirthTopic := topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String()
	tr.deliverToAll(nbirthTopic, b)

	if birthGroup != "g1" || birthNode != "n1" {
		t.Errorf("OnNodeBirth called with %s/%s", birthGroup, birthNode)
	}

	ns := app.GetNode("g1", "n1")
	if ns == nil {
		t.Fatal("node not found")
	}
	if !ns.Online {
		t.Error("node should be online")
	}
	if ns.BdSeq != 0 {
		t.Errorf("bdSeq = %d, want 0", ns.BdSeq)
	}
	if _, ok := ns.Metrics["temperature"]; !ok {
		t.Error("temperature metric not found")
	}
}

func TestHostNodeDeath(t *testing.T) {
	tr := newMockTransport()
	var deathCalled bool
	cfg := testConfig()
	cfg.OnNodeDeath = func(g, n string) { deathCalled = true }
	app, _ := New(tr, cfg)

	ctx := context.Background()
	_ = app.Connect(ctx)

	bdSeq := payload.BdSeqMetric(5)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	deathP := &pb.Payload{Metrics: []*pb.Payload_Metric{payload.BdSeqMetric(5)}}
	ts := uint64(0)
	deathP.Timestamp = &ts
	db, _ := payload.Encode(deathP)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDEATH, EdgeNodeID: "n1"}.String(), db)

	if !deathCalled {
		t.Error("OnNodeDeath not called")
	}
	ns := app.GetNode("g1", "n1")
	if ns.Online {
		t.Error("node should be offline after NDEATH")
	}
}

func TestHostSeqValidation(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())

	ctx := context.Background()
	_ = app.Connect(ctx)

	bdSeq := payload.BdSeqMetric(0)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(1))
	p1 := payload.NewPayload(1, m)
	b1, _ := payload.Encode(p1)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "n1"}.String(), b1)

	ns := app.GetNode("g1", "n1")
	if ns.LastSeq != 1 {
		t.Errorf("lastSeq = %d, want 1", ns.LastSeq)
	}

	p5 := payload.NewPayload(5, m)
	b5, _ := payload.Encode(p5)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "n1"}.String(), b5)

	ns = app.GetNode("g1", "n1")
	if ns.LastSeq != 1 {
		t.Errorf("lastSeq after gap = %d, want 1 (message should be rejected)", ns.LastSeq)
	}
}

func TestHostSendCommand(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())

	ctx := context.Background()
	_ = app.Connect(ctx)

	m, _ := payload.NewMetric("setpoint", pb.DataType_Double, float64(42.0))
	if err := app.SendNodeCommand(ctx, "g1", "n1", []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}

	msgs := tr.getPublished()
	var found bool
	for _, msg := range msgs {
		if msg.Topic == "spBv1.0/g1/NCMD/n1" {
			found = true
			if msg.QoS != 0 {
				t.Errorf("NCMD QoS = %d, want 0", msg.QoS)
			}
			if msg.Retained {
				t.Error("NCMD should not be retained")
			}
		}
	}
	if !found {
		t.Error("NCMD not published")
	}
}

func TestHostSendDeviceCommand(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())

	ctx := context.Background()
	_ = app.Connect(ctx)

	m, _ := payload.NewMetric("valve", pb.DataType_Boolean, true)
	if err := app.SendDeviceCommand(ctx, "g1", "n1", "d1", []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}

	msgs := tr.getPublished()
	var found bool
	for _, msg := range msgs {
		if msg.Topic == "spBv1.0/g1/DCMD/n1/d1" {
			found = true
		}
	}
	if !found {
		t.Error("DCMD not published")
	}
}

func TestHostRequestRebirth(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())

	ctx := context.Background()
	_ = app.Connect(ctx)

	if err := app.RequestRebirth(ctx, "g1", "n1"); err != nil {
		t.Fatal(err)
	}

	msgs := tr.getPublished()
	var found bool
	for _, msg := range msgs {
		if msg.Topic == "spBv1.0/g1/NCMD/n1" {
			p, _ := payload.Decode(msg.Payload)
			for _, m := range p.Metrics {
				if m.GetName() == "Node Control/Rebirth" && m.GetBooleanValue() {
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("rebirth command not published")
	}
}

func TestHostCloseIdempotent(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)
	_ = app.Close(ctx)
	_ = app.Close(ctx)

	if app.State() != Closed {
		t.Errorf("state = %s, want closed", app.State())
	}
}

func TestHostSendCommandWhenDisconnected(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	err := app.SendNodeCommand(ctx, "g1", "n1", []*pb.Payload_Metric{m})
	if !errors.Is(err, ErrNotConnected) {
		t.Errorf("expected ErrNotConnected, got %v", err)
	}
}

func TestHostDeviceBirthAndData(t *testing.T) {
	tr := newMockTransport()
	var dbirthCalled, ddataCalled bool
	cfg := testConfig()
	cfg.OnDeviceBirth = func(g, n, d string, _ []*pb.Payload_Metric) { dbirthCalled = true }
	cfg.OnDeviceData = func(g, n, d string, _ []*pb.Payload_Metric) { ddataCalled = true }
	app, _ := New(tr, cfg)

	ctx := context.Background()
	_ = app.Connect(ctx)

	bdSeq := payload.BdSeqMetric(0)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	dm, _ := payload.NewMetric("sensor", pb.DataType_Float, float32(1.5))
	dp := payload.NewPayload(1, dm)
	db, _ := payload.Encode(dp)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DBIRTH, EdgeNodeID: "n1", DeviceID: "d1"}.String(), db)

	if !dbirthCalled {
		t.Error("OnDeviceBirth not called")
	}

	dm2, _ := payload.NewMetric("sensor", pb.DataType_Float, float32(2.5))
	dp2 := payload.NewPayload(2, dm2)
	db2, _ := payload.Encode(dp2)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DDATA, EdgeNodeID: "n1", DeviceID: "d1"}.String(), db2)

	if !ddataCalled {
		t.Error("OnDeviceData not called")
	}

	ns := app.GetNode("g1", "n1")
	ds := ns.Devices["d1"]
	if ds == nil {
		t.Fatal("device not found")
	}
	if !ds.Online {
		t.Error("device should be online")
	}
}

func TestHostNodes(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)

	for _, nodeID := range []string{"n1", "n2"} {
		bdSeq := payload.BdSeqMetric(0)
		p := payload.NewPayload(0, bdSeq)
		b, _ := payload.Encode(p)
		tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: nodeID}.String(), b)
	}

	nodes := app.Nodes()
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}
}

func TestHostNewValidation(t *testing.T) {
	tr := newMockTransport()

	_, err := New(tr, Config{})
	if err == nil {
		t.Error("expected error for empty HostID")
	}

	_, err = New(tr, Config{HostID: "h"})
	if err == nil {
		t.Error("expected error for empty BrokerURL")
	}
}

func TestHostNDeathBdSeqMismatch(t *testing.T) {
	tr := newMockTransport()
	var deathCalled bool
	cfg := testConfig()
	cfg.OnNodeDeath = func(g, n string) { deathCalled = true }
	app, _ := New(tr, cfg)

	ctx := context.Background()
	_ = app.Connect(ctx)

	bdSeq := payload.BdSeqMetric(5)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	deathP := &pb.Payload{Metrics: []*pb.Payload_Metric{payload.BdSeqMetric(99)}}
	ts := uint64(0)
	deathP.Timestamp = &ts
	db, _ := payload.Encode(deathP)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDEATH, EdgeNodeID: "n1"}.String(), db)

	if deathCalled {
		t.Error("OnNodeDeath should NOT be called for mismatched bdSeq")
	}
	ns := app.GetNode("g1", "n1")
	if !ns.Online {
		t.Error("node should still be online after mismatched NDEATH")
	}
}

func TestHostStateStringUnknown(t *testing.T) {
	s := State(99)
	if s.String() != "unknown" {
		t.Errorf("State(99).String() = %q, want unknown", s.String())
	}
}

func TestHostConnectAfterClose(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Close(ctx)
	err := app.Connect(ctx)
	if !errors.Is(err, ErrClosed) {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestHostConnectFailure(t *testing.T) {
	tr := &failConnectTransport{mockTransport: newMockTransport()}
	app, _ := New(tr, testConfig())
	err := app.Connect(context.Background())
	if err == nil {
		t.Error("expected connect error")
	}
	if app.State() != Disconnected {
		t.Errorf("state = %s, want disconnected", app.State())
	}
}

type failConnectTransport struct {
	*mockTransport
}

func (f *failConnectTransport) Connect(_ context.Context, _ mqtt.ConnectOpts) error {
	return errors.New("connection refused")
}

func TestHostSubscribeSTATEFailure(t *testing.T) {
	tr := &failSubscribeTransport{mockTransport: newMockTransport(), failTopic: "spBv1.0/STATE/testHost"}
	app, _ := New(tr, testConfig())
	err := app.Connect(context.Background())
	if err == nil {
		t.Error("expected subscribe STATE error")
	}
}

func TestHostSubscribeNamespaceFailure(t *testing.T) {
	tr := &failSubscribeTransport{mockTransport: newMockTransport(), failTopic: "spBv1.0/#"}
	app, _ := New(tr, testConfig())
	err := app.Connect(context.Background())
	if err == nil {
		t.Error("expected subscribe spBv1.0/# error")
	}
}

func TestHostPublishSTATEBirthFailure(t *testing.T) {
	tr := &failPublishTransport{mockTransport: newMockTransport(), failTopic: "spBv1.0/STATE/testHost"}
	app, _ := New(tr, testConfig())
	err := app.Connect(context.Background())
	if err == nil {
		t.Error("expected publish STATE birth error")
	}
}

type failSubscribeTransport struct {
	*mockTransport
	failTopic string
}

func (f *failSubscribeTransport) Subscribe(_ context.Context, t string, qos byte, handler mqtt.MessageHandler) error {
	if t == f.failTopic {
		return errors.New("subscribe failed")
	}
	return f.mockTransport.Subscribe(context.Background(), t, qos, handler)
}

type failPublishTransport struct {
	*mockTransport
	failTopic string
}

func (f *failPublishTransport) Publish(_ context.Context, t string, qos byte, retained bool, p []byte) error {
	if t == f.failTopic {
		return errors.New("publish failed")
	}
	return f.mockTransport.Publish(context.Background(), t, qos, retained, p)
}

func TestHostSendDeviceCommandWhenDisconnected(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	err := app.SendDeviceCommand(ctx, "g1", "n1", "d1", []*pb.Payload_Metric{m})
	if !errors.Is(err, ErrNotConnected) {
		t.Errorf("expected ErrNotConnected, got %v", err)
	}
}

func TestHostDisconnectWhenAlreadyDisconnected(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	// Never connected, so disconnect should be a no-op
	if err := app.Disconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHostHandleMalformedPayloads(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)

	badData := []byte{0xFF, 0xFF, 0xFF}
	// All these should not panic — they hit decode error paths
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), badData)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDEATH, EdgeNodeID: "n1"}.String(), badData)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DBIRTH, EdgeNodeID: "n1", DeviceID: "d1"}.String(), badData)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DDEATH, EdgeNodeID: "n1", DeviceID: "d1"}.String(), badData)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "n1"}.String(), badData)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DDATA, EdgeNodeID: "n1", DeviceID: "d1"}.String(), badData)
}

func TestHostHandleMessagesWithoutNode(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)

	// Send data messages for nodes that have never sent a BIRTH — should not panic
	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	p := payload.NewPayload(1, m)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "unknown"}.String(), b)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DDATA, EdgeNodeID: "unknown", DeviceID: "d1"}.String(), b)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DBIRTH, EdgeNodeID: "unknown", DeviceID: "d1"}.String(), b)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DDEATH, EdgeNodeID: "unknown", DeviceID: "d1"}.String(), b)

	// NDEATH for unknown node
	dp := &pb.Payload{Metrics: []*pb.Payload_Metric{payload.BdSeqMetric(0)}}
	ts := uint64(0)
	dp.Timestamp = &ts
	db, _ := payload.Encode(dp)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDEATH, EdgeNodeID: "unknown"}.String(), db)
}

func TestHostWithLogger(t *testing.T) {
	tr := newMockTransport()
	cfg := testConfig()
	cfg.Logger = &testLogger{}
	app, _ := New(tr, cfg)
	ctx := context.Background()
	_ = app.Connect(ctx)

	// Trigger logWarn with malformed payloads
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), []byte{0xFF})
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "n1"}.String(), []byte{0xFF})

	_ = app.Disconnect(ctx)
}

type testLogger struct{}

func (l *testLogger) Debugf(format string, args ...any) {}
func (l *testLogger) Infof(format string, args ...any)  {}
func (l *testLogger) Warnf(format string, args ...any)  {}
func (l *testLogger) Errorf(format string, args ...any) {}

func TestHostSTATEMessageIgnored(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)
	// STATE messages are silently returned from handleMessage
	tr.deliverToAll("spBv1.0/STATE/testHost", []byte(`{"online":true,"timestamp":1}`))
	// Should not panic or change state
	if app.State() != Connected {
		t.Errorf("state = %s, want connected", app.State())
	}
}

func TestHostNDATAUpdatesMetrics(t *testing.T) {
	tr := newMockTransport()
	var dataCalled bool
	cfg := testConfig()
	cfg.OnNodeData = func(g, n string, metrics []*pb.Payload_Metric) { dataCalled = true }
	app, _ := New(tr, cfg)
	ctx := context.Background()
	_ = app.Connect(ctx)

	// NBIRTH
	bdSeq := payload.BdSeqMetric(0)
	tempMetric, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(22.5))
	p := payload.NewPayload(0, bdSeq, tempMetric)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	// NDATA updates the metric
	newTemp, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(25.0))
	dp := payload.NewPayload(1, newTemp)
	db, _ := payload.Encode(dp)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "n1"}.String(), db)

	if !dataCalled {
		t.Error("OnNodeData not called")
	}
	ns := app.GetNode("g1", "n1")
	if m, ok := ns.Metrics["temperature"]; !ok {
		t.Error("temperature metric missing")
	} else if m.GetDoubleValue() != 25.0 {
		t.Errorf("temperature = %f, want 25.0", m.GetDoubleValue())
	}
}

func TestHostDDATAUpdatesDeviceMetrics(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)

	// NBIRTH
	bdSeq := payload.BdSeqMetric(0)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	// DBIRTH
	dm, _ := payload.NewMetric("sensor", pb.DataType_Float, float32(1.5))
	dp := payload.NewPayload(1, dm)
	db, _ := payload.Encode(dp)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DBIRTH, EdgeNodeID: "n1", DeviceID: "d1"}.String(), db)

	// DDATA updates device metric
	dm2, _ := payload.NewMetric("sensor", pb.DataType_Float, float32(3.0))
	dp2 := payload.NewPayload(2, dm2)
	db2, _ := payload.Encode(dp2)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DDATA, EdgeNodeID: "n1", DeviceID: "d1"}.String(), db2)

	ns := app.GetNode("g1", "n1")
	ds := ns.Devices["d1"]
	if m, ok := ds.Metrics["sensor"]; !ok {
		t.Error("sensor metric missing")
	} else if m.GetFloatValue() != 3.0 {
		t.Errorf("sensor = %f, want 3.0", m.GetFloatValue())
	}
}

func TestHostDDATAUnknownDevice(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)

	// NBIRTH
	bdSeq := payload.BdSeqMetric(0)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	// DDATA for unknown device — should not panic
	dm, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	dp := payload.NewPayload(1, dm)
	db, _ := payload.Encode(dp)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DDATA, EdgeNodeID: "n1", DeviceID: "unknown"}.String(), db)
}

func TestHostNDeathDevicesGoOffline(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)

	// NBIRTH
	bdSeq := payload.BdSeqMetric(5)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	// DBIRTH
	dm, _ := payload.NewMetric("s", pb.DataType_Float, float32(1.0))
	dp := payload.NewPayload(1, dm)
	db, _ := payload.Encode(dp)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DBIRTH, EdgeNodeID: "n1", DeviceID: "d1"}.String(), db)

	// NDEATH
	deathP := &pb.Payload{Metrics: []*pb.Payload_Metric{payload.BdSeqMetric(5)}}
	ts := uint64(0)
	deathP.Timestamp = &ts
	ddb, _ := payload.Encode(deathP)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDEATH, EdgeNodeID: "n1"}.String(), ddb)

	ns := app.GetNode("g1", "n1")
	if ns.Devices["d1"].Online {
		t.Error("device should be offline after NDEATH")
	}
}

func TestHostDeviceDeath(t *testing.T) {
	tr := newMockTransport()
	var deathCalled bool
	cfg := testConfig()
	cfg.OnDeviceDeath = func(g, n, d string) { deathCalled = true }
	app, _ := New(tr, cfg)

	ctx := context.Background()
	_ = app.Connect(ctx)

	bdSeq := payload.BdSeqMetric(0)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	dm, _ := payload.NewMetric("sensor", pb.DataType_Float, float32(1.5))
	dp := payload.NewPayload(1, dm)
	db, _ := payload.Encode(dp)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DBIRTH, EdgeNodeID: "n1", DeviceID: "d1"}.String(), db)

	ddp := payload.NewPayload(2)
	ddb, _ := payload.Encode(ddp)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DDEATH, EdgeNodeID: "n1", DeviceID: "d1"}.String(), ddb)

	if !deathCalled {
		t.Error("OnDeviceDeath not called")
	}
	ns := app.GetNode("g1", "n1")
	if !ns.Online {
		t.Error("node should still be online after device death")
	}
	ds := ns.Devices["d1"]
	if ds.Online {
		t.Error("device should be offline after DDEATH")
	}
	if !ds.Stale {
		t.Error("device metrics should be stale after DDEATH")
	}
}

func TestHostStaleOnNDEATH(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)

	bdSeq := payload.BdSeqMetric(5)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	ns := app.GetNode("g1", "n1")
	if ns.Stale {
		t.Error("should not be stale after birth")
	}

	deathP := &pb.Payload{Metrics: []*pb.Payload_Metric{payload.BdSeqMetric(5)}}
	ts := uint64(0)
	deathP.Timestamp = &ts
	db, _ := payload.Encode(deathP)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDEATH, EdgeNodeID: "n1"}.String(), db)

	ns = app.GetNode("g1", "n1")
	if !ns.Stale {
		t.Error("node metrics should be stale after NDEATH")
	}
}

func TestHostStaleClearedOnRebirth(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)

	// First birth
	bdSeq := payload.BdSeqMetric(5)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	// Death
	deathP := &pb.Payload{Metrics: []*pb.Payload_Metric{payload.BdSeqMetric(5)}}
	ts := uint64(0)
	deathP.Timestamp = &ts
	db, _ := payload.Encode(deathP)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDEATH, EdgeNodeID: "n1"}.String(), db)

	// Rebirth
	bdSeq2 := payload.BdSeqMetric(6)
	p2 := payload.NewPayload(0, bdSeq2)
	b2, _ := payload.Encode(p2)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b2)

	ns := app.GetNode("g1", "n1")
	if ns.Stale {
		t.Error("stale should be cleared after rebirth")
	}
}

func TestHostAliasResolution(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)

	// NBIRTH with alias
	alias := uint64(1)
	m := &pb.Payload_Metric{
		Name:     strPtr("temperature"),
		Alias:    &alias,
		Datatype: dtPtr(pb.DataType_Double),
	}
	bdSeq := payload.BdSeqMetric(0)
	seq := uint64(0)
	p := &pb.Payload{
		Seq:     &seq,
		Metrics: []*pb.Payload_Metric{bdSeq, m},
	}
	tsVal := uint64(time.Now().UnixMilli())
	p.Timestamp = &tsVal
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	// Resolve alias
	name, ok := app.ResolveAlias("g1", "n1", 1)
	if !ok || name != "temperature" {
		t.Errorf("ResolveAlias = %q, %v; want temperature, true", name, ok)
	}

	// Unknown alias
	_, ok = app.ResolveAlias("g1", "n1", 999)
	if ok {
		t.Error("expected false for unknown alias")
	}

	// Unknown node
	_, ok = app.ResolveAlias("g1", "unknown", 1)
	if ok {
		t.Error("expected false for unknown node")
	}
}

func TestHostNDATAAliasResolution(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)

	// NBIRTH with alias
	alias := uint64(1)
	m := &pb.Payload_Metric{
		Name:     strPtr("temperature"),
		Alias:    &alias,
		Datatype: dtPtr(pb.DataType_Double),
	}
	bdSeq := payload.BdSeqMetric(0)
	seq := uint64(0)
	p := &pb.Payload{
		Seq:     &seq,
		Metrics: []*pb.Payload_Metric{bdSeq, m},
	}
	tsVal := uint64(time.Now().UnixMilli())
	p.Timestamp = &tsVal
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	// NDATA with alias only (no name) — should resolve and update metric
	dataAlias := uint64(1)
	dm := &pb.Payload_Metric{
		Alias:    &dataAlias,
		Datatype: dtPtr(pb.DataType_Double),
	}
	payload.SetValue(dm, pb.DataType_Double, float64(25.0))
	seq1 := uint64(1)
	dp := &pb.Payload{Seq: &seq1, Metrics: []*pb.Payload_Metric{dm}}
	dp.Timestamp = &tsVal
	db, _ := payload.Encode(dp)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "n1"}.String(), db)

	ns := app.GetNode("g1", "n1")
	if tm, ok := ns.Metrics["temperature"]; !ok {
		t.Error("temperature metric not found after alias-based NDATA")
	} else if tm.GetDoubleValue() != 25.0 {
		t.Errorf("temperature = %f, want 25.0", tm.GetDoubleValue())
	}
}

func TestHostDeviceAliasResolution(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)

	// NBIRTH
	bdSeq := payload.BdSeqMetric(0)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	// DBIRTH with alias
	alias := uint64(10)
	dm := &pb.Payload_Metric{
		Name:     strPtr("rpm"),
		Alias:    &alias,
		Datatype: dtPtr(pb.DataType_UInt32),
	}
	seq1 := uint64(1)
	dp := &pb.Payload{Seq: &seq1, Metrics: []*pb.Payload_Metric{dm}}
	tsVal := uint64(time.Now().UnixMilli())
	dp.Timestamp = &tsVal
	db, _ := payload.Encode(dp)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DBIRTH, EdgeNodeID: "n1", DeviceID: "d1"}.String(), db)

	// Resolve via node
	name, ok := app.ResolveAlias("g1", "n1", 10)
	if !ok || name != "rpm" {
		t.Errorf("ResolveAlias = %q, %v; want rpm, true", name, ok)
	}

	// DDATA with alias only
	dataAlias := uint64(10)
	ddm := &pb.Payload_Metric{
		Alias:    &dataAlias,
		Datatype: dtPtr(pb.DataType_UInt32),
	}
	payload.SetValue(ddm, pb.DataType_UInt32, uint32(3000))
	seq2 := uint64(2)
	ddp := &pb.Payload{Seq: &seq2, Metrics: []*pb.Payload_Metric{ddm}}
	ddp.Timestamp = &tsVal
	ddb, _ := payload.Encode(ddp)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DDATA, EdgeNodeID: "n1", DeviceID: "d1"}.String(), ddb)

	ns := app.GetNode("g1", "n1")
	ds := ns.Devices["d1"]
	if m, ok := ds.Metrics["rpm"]; !ok {
		t.Error("rpm metric not found after alias-based DDATA")
	} else if m.GetIntValue() != 3000 {
		t.Errorf("rpm = %d, want 3000", m.GetIntValue())
	}
}

func strPtr(s string) *string       { return &s }
func dtPtr(dt pb.DataType) *uint32   { v := uint32(dt); return &v }
