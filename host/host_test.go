// SPDX-License-Identifier: MIT

package host

import (
	"context"
	"errors"
	"strings"
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
	var handler mqtt.MessageHandler
	for sub, h := range m.handlers {
		if mqttTopicMatch(sub, topicStr) {
			handler = h
			break
		}
	}
	m.mu.Unlock()
	if handler != nil {
		handler(topicStr, data)
	}
}

func mqttTopicMatch(filter, t string) bool {
	if filter == t {
		return true
	}
	if strings.HasSuffix(filter, "/#") {
		prefix := filter[:len(filter)-2]
		return t == prefix || strings.HasPrefix(t, prefix+"/")
	}
	return false
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

func TestHostServerURLsFallback(t *testing.T) {
	tr := newMockTransport()
	cfg := Config{
		HostID:     "testHost",
		ServerURLs: []string{"tcp://broker1:1883", "tcp://broker2:1883"},
	}
	app, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tr.opts.BrokerURL != "tcp://broker1:1883" {
		t.Errorf("BrokerURL = %q, want tcp://broker1:1883", tr.opts.BrokerURL)
	}
}

func TestHostBrokerURLTakesPrecedence(t *testing.T) {
	tr := newMockTransport()
	cfg := Config{
		HostID:     "testHost",
		BrokerURL:  "tcp://primary:1883",
		ServerURLs: []string{"tcp://backup:1883"},
	}
	app, _ := New(tr, cfg)
	_ = app.Connect(context.Background())
	if tr.opts.BrokerURL != "tcp://primary:1883" {
		t.Errorf("BrokerURL = %q, want tcp://primary:1883", tr.opts.BrokerURL)
	}
}

func TestHostReorderTimeout(t *testing.T) {
	tr := newMockTransport()
	cfg := testConfig()
	cfg.ReorderTimeout = 50 * time.Millisecond
	app, _ := New(tr, cfg)
	ctx := context.Background()
	_ = app.Connect(ctx)

	// NBIRTH
	bdSeq := payload.BdSeqMetric(0)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	// Send seq=5 (gap from 0) — should start timer, not immediately rebirth
	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(1))
	p5 := payload.NewPayload(5, m)
	b5, _ := payload.Encode(p5)
	pubCountBefore := len(tr.getPublished())
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "n1"}.String(), b5)

	// No immediate rebirth
	time.Sleep(10 * time.Millisecond)
	pubCountAfter := len(tr.getPublished())
	if pubCountAfter != pubCountBefore {
		t.Error("rebirth should not be sent immediately with ReorderTimeout")
	}

	// Wait for timer to fire
	time.Sleep(100 * time.Millisecond)
	msgs := tr.getPublished()
	var rebirthFound bool
	for _, msg := range msgs[pubCountBefore:] {
		if msg.Topic == "spBv1.0/g1/NCMD/n1" {
			rebirthFound = true
		}
	}
	if !rebirthFound {
		t.Error("rebirth should be sent after ReorderTimeout expires")
	}
}

func TestHostReorderTimeoutCancelledByCorrectSeq(t *testing.T) {
	tr := newMockTransport()
	cfg := testConfig()
	cfg.ReorderTimeout = 200 * time.Millisecond
	app, _ := New(tr, cfg)
	ctx := context.Background()
	_ = app.Connect(ctx)

	// NBIRTH
	bdSeq := payload.BdSeqMetric(0)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	// Send seq=5 (gap) — starts reorder timer
	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(1))
	p5 := payload.NewPayload(5, m)
	b5, _ := payload.Encode(p5)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "n1"}.String(), b5)

	// Send correct seq=1 — should cancel the timer
	p1 := payload.NewPayload(1, m)
	b1, _ := payload.Encode(p1)
	pubCountBefore := len(tr.getPublished())
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "n1"}.String(), b1)

	// Wait past the timeout — no rebirth should fire
	time.Sleep(300 * time.Millisecond)
	msgs := tr.getPublished()
	for _, msg := range msgs[pubCountBefore:] {
		if msg.Topic == "spBv1.0/g1/NCMD/n1" {
			t.Error("rebirth should NOT be sent when correct seq cancels reorder timer")
		}
	}
}

func TestHostReorderTimeoutCancelledByRebirth(t *testing.T) {
	tr := newMockTransport()
	cfg := testConfig()
	cfg.ReorderTimeout = 200 * time.Millisecond
	app, _ := New(tr, cfg)
	ctx := context.Background()
	_ = app.Connect(ctx)

	// NBIRTH
	bdSeq := payload.BdSeqMetric(0)
	p := payload.NewPayload(0, bdSeq)
	b, _ := payload.Encode(p)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)

	// Send seq gap
	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(1))
	p5 := payload.NewPayload(5, m)
	b5, _ := payload.Encode(p5)
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "n1"}.String(), b5)

	// New NBIRTH (rebirth) — should cancel timer
	bdSeq2 := payload.BdSeqMetric(1)
	p2 := payload.NewPayload(0, bdSeq2)
	b2, _ := payload.Encode(p2)
	pubCountBefore := len(tr.getPublished())
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b2)

	time.Sleep(300 * time.Millisecond)
	msgs := tr.getPublished()
	for _, msg := range msgs[pubCountBefore:] {
		if msg.Topic == "spBv1.0/g1/NCMD/n1" {
			t.Error("rebirth request should NOT fire after node re-birthed")
		}
	}
}

func strPtr(s string) *string      { return &s }
func dtPtr(dt pb.DataType) *uint32 { v := uint32(dt); return &v }

func TestHostConnectionLost(t *testing.T) {
	tr := newMockTransport()
	app, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := app.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	app.mu.Lock()
	app.reorderTimers["g/n"] = time.AfterFunc(time.Hour, func() {})
	app.mu.Unlock()

	tr.opts.OnConnectionLost(mqtt.ErrConnectionLost)
	if app.State() != Disconnected {
		t.Fatalf("state = %s, want disconnected", app.State())
	}
	if len(app.reorderTimers) != 0 {
		t.Fatal("reorder timers not cleared")
	}
	tr.opts.OnConnectionLost(mqtt.ErrConnectionLost)
	if app.State() != Disconnected {
		t.Fatalf("state = %s, want disconnected", app.State())
	}
}

func TestHostAliasDataKeepsBirthNameAndDatatype(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	ctx := context.Background()
	_ = app.Connect(ctx)

	temp, _ := payload.NewMetricWithAlias("temperature", 7, pb.DataType_Double, float64(22.5))
	b, _ := payload.Encode(payload.NewPayload(0, payload.BdSeqMetric(0), temp))
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), b)
	pump, _ := payload.NewMetricWithAlias("speed", 8, pb.DataType_Float, float32(1))
	b, _ = payload.Encode(payload.NewPayload(1, pump))
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DBIRTH, EdgeNodeID: "n1", DeviceID: "d1"}.String(), b)

	before := app.GetNode("g1", "n1")

	// Data by alias only, without a datatype, as Sparkplug allows after the birth.
	alias := func(a uint64, v *pb.Payload_Metric) *pb.Payload_Metric {
		v.Name, v.Alias, v.Datatype = nil, &a, nil
		return v
	}
	nd, _ := payload.NewMetric("x", pb.DataType_Double, float64(23.5))
	b, _ = payload.Encode(payload.NewPayload(2, alias(7, nd)))
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "n1"}.String(), b)
	dd, _ := payload.NewMetric("x", pb.DataType_Float, float32(2.5))
	b, _ = payload.Encode(payload.NewPayload(3, alias(8, dd)))
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DDATA, EdgeNodeID: "n1", DeviceID: "d1"}.String(), b)

	ns := app.GetNode("g1", "n1")
	m := ns.Metrics["temperature"]
	if m.GetName() != "temperature" || pb.DataType(m.GetDatatype()) != pb.DataType_Double {
		t.Fatalf("node metric kept as %v", m)
	}
	if v, err := payload.GetValue(m); err != nil || v != float64(23.5) {
		t.Errorf("temperature = %v, %v; want 23.5", v, err)
	}
	dm := ns.Devices["d1"].Metrics["speed"]
	if v, err := payload.GetValue(dm); err != nil || v != float32(2.5) || dm.GetName() != "speed" {
		t.Errorf("speed = %v (%v), %v; want 2.5", v, dm, err)
	}
	if v, _ := payload.GetValue(before.Metrics["temperature"]); v != float64(22.5) {
		t.Errorf("an earlier snapshot changed: temperature = %v", v)
	}
}

func TestHostRequestsRebirthFromUnknownNode(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	_ = app.Connect(context.Background())

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(1))
	b, _ := payload.Encode(payload.NewPayload(1, m))
	dbirth := topic.Topic{GroupID: "g1", Type: topic.DBIRTH, EdgeNodeID: "n1", DeviceID: "d1"}.String()
	// A DBIRTH before any NBIRTH, twice: one rebirth request.
	tr.deliverToAll(dbirth, b)
	tr.deliverToAll(dbirth, b)

	ncmd := topic.ForNCMD("g1", "n1")
	deadline := time.Now().Add(2 * time.Second)
	for {
		var reqs int
		for _, p := range tr.getPublished() {
			if p.Topic == ncmd {
				reqs++
				pl, _ := payload.Decode(p.Payload)
				if r := pl.Metrics[0]; r.GetName() != "Node Control/Rebirth" || !r.GetBooleanValue() {
					t.Fatalf("NCMD %v, want Node Control/Rebirth true", r)
				}
			}
		}
		if reqs == 1 {
			time.Sleep(50 * time.Millisecond)
			break
		}
		if reqs > 1 || time.Now().After(deadline) {
			t.Fatalf("%d rebirth requests, want 1", reqs)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if app.GetNode("g1", "n1") != nil {
		t.Error("a node without NBIRTH must stay unknown")
	}
}

func TestHostThrottlesSeqGapRebirths(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	_ = app.Connect(context.Background())

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(1))
	enc := func(seq uint64, ms ...*pb.Payload_Metric) []byte {
		b, _ := payload.Encode(payload.NewPayload(seq, ms...))
		return b
	}
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), enc(0, payload.BdSeqMetric(0), m))
	// Several messages out of sequence in a row, as a broker that reorders
	// across topics delivers them: one rebirth request, not one each.
	for seq := uint64(5); seq < 10; seq++ {
		tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "n1"}.String(), enc(seq, m))
	}

	ncmd := topic.ForNCMD("g1", "n1")
	expect := func(want int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			var reqs int
			for _, p := range tr.getPublished() {
				if p.Topic == ncmd {
					reqs++
				}
			}
			if reqs == want {
				time.Sleep(50 * time.Millisecond)
				return
			}
			if reqs > want || time.Now().After(deadline) {
				t.Fatalf("%d rebirth requests, want %d", reqs, want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	expect(1)
	// The NBIRTH answers the request; the messages throttled while it was
	// pending may have been of this new session: one more request.
	nbirth := topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String()
	tr.deliverToAll(nbirth, enc(0, payload.BdSeqMetric(1), m))
	expect(2)
	// Nothing was throttled since: no further request.
	tr.deliverToAll(nbirth, enc(0, payload.BdSeqMetric(2), m))
	expect(2)
}

func TestHostAppliesReorderedMessagesInSequence(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	_ = app.Connect(context.Background())

	enc := func(seq uint64, ms ...*pb.Payload_Metric) []byte {
		b, _ := payload.Encode(payload.NewPayload(seq, ms...))
		return b
	}
	x := func(v int32) *pb.Payload_Metric { m, _ := payload.NewMetric("x", pb.DataType_Int32, v); return m }
	y := func(v int32) *pb.Payload_Metric { m, _ := payload.NewMetric("y", pb.DataType_Int32, v); return m }
	tp := func(mt topic.MessageType, dev string) string {
		return topic.Topic{GroupID: "g1", Type: mt, EdgeNodeID: "n1", DeviceID: dev}.String()
	}

	// Published as NBIRTH 0, DBIRTH 1, NDATA 2, DDATA 3, NDATA 4; delivered
	// with the DBIRTH before the NBIRTH and NDATA 4 before NDATA 2.
	tr.deliverToAll(tp(topic.DBIRTH, "d1"), enc(1, y(10)))
	tr.deliverToAll(tp(topic.NBIRTH, ""), enc(0, payload.BdSeqMetric(0), x(1)))
	tr.deliverToAll(tp(topic.NDATA, ""), enc(4, x(3)))
	tr.deliverToAll(tp(topic.DDATA, "d1"), enc(3, y(11)))
	tr.deliverToAll(tp(topic.NDATA, ""), enc(2, x(2)))

	ns := app.GetNode("g1", "n1")
	if ns == nil || ns.Devices["d1"] == nil {
		t.Fatalf("node or device not tracked: %+v", ns)
	}
	if ns.LastSeq != 4 {
		t.Errorf("LastSeq = %d, want 4: every message applied", ns.LastSeq)
	}
	if v, _ := payload.GetValue(ns.Metrics["x"]); v != int32(3) {
		t.Errorf("x = %v, want 3 (the value of the last NDATA in sequence)", v)
	}
	if v, _ := payload.GetValue(ns.Devices["d1"].Metrics["y"]); v != int32(11) {
		t.Errorf("y = %v, want 11", v)
	}
}

func TestHostDropsHeldMessagesThatWaitedTooLong(t *testing.T) {
	tr := newMockTransport()
	app, _ := New(tr, testConfig())
	_ = app.Connect(context.Background())

	enc := func(seq uint64, ms ...*pb.Payload_Metric) []byte {
		b, _ := payload.Encode(payload.NewPayload(seq, ms...))
		return b
	}
	x := func(v int32) *pb.Payload_Metric { m, _ := payload.NewMetric("x", pb.DataType_Int32, v); return m }
	ndata := topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: "n1"}.String()

	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: "n1"}.String(), enc(0, payload.BdSeqMetric(0), x(1)))
	tr.deliverToAll(ndata, enc(3, x(3))) // ahead: held
	tr.deliverToAll(ndata, enc(5, x(5))) // ahead: held

	app.mu.Lock()
	held := app.held[nodeKey("g1", "n1")]
	if len(held) != 2 {
		app.mu.Unlock()
		t.Fatalf("held %d messages, want 2", len(held))
	}
	held[1].at = time.Now().Add(-2 * heldMaxAge)
	app.mu.Unlock()

	tr.deliverToAll(ndata, enc(1, x(10)))
	tr.deliverToAll(ndata, enc(2, x(2)))

	ns := app.GetNode("g1", "n1")
	if ns.LastSeq != 3 {
		t.Errorf("LastSeq = %d, want 3: seq 3 applied, seq 5 dropped", ns.LastSeq)
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if n := len(app.held[nodeKey("g1", "n1")]); n != 0 {
		t.Errorf("%d messages still held, want 0", n)
	}
}
