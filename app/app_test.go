// SPDX-License-Identifier: MIT

package app

import (
	"context"
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
	connectErr error
}

type publishedMsg struct {
	Topic   string
	Payload []byte
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
	if m.connectErr != nil {
		return m.connectErr
	}
	m.connected = true
	m.opts = opts
	return nil
}

func (m *mockTransport) Publish(_ context.Context, t string, qos byte, retained bool, p []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, publishedMsg{t, p})
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

func (m *mockTransport) deliver(topicStr string, data []byte) {
	m.mu.Lock()
	var handler mqtt.MessageHandler
	for sub, h := range m.handlers {
		if sub == topic.Namespace+"/#" {
			handler = h
			break
		}
		_ = sub
	}
	m.mu.Unlock()
	if handler != nil {
		handler(topicStr, data)
	}
}

func (m *mockTransport) getPublished() []publishedMsg {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]publishedMsg, len(m.published))
	copy(out, m.published)
	return out
}

func encodeBirth(seq uint64, bdSeq uint64, metrics ...*pb.Payload_Metric) []byte {
	p := &pb.Payload{Seq: &seq}
	ts := uint64(time.Now().UnixMilli())
	p.Timestamp = &ts
	bdMetric := &pb.Payload_Metric{}
	bdMetric.Name = strPtr("bdSeq")
	dt := uint32(pb.DataType_Int64)
	bdMetric.Datatype = &dt
	bdMetric.Value = &pb.Payload_Metric_LongValue{LongValue: bdSeq}
	p.Metrics = append([]*pb.Payload_Metric{bdMetric}, metrics...)
	b, _ := payload.Encode(p)
	return b
}

func encodeData(seq uint64, metrics ...*pb.Payload_Metric) []byte {
	p := &pb.Payload{Seq: &seq, Metrics: metrics}
	ts := uint64(time.Now().UnixMilli())
	p.Timestamp = &ts
	b, _ := payload.Encode(p)
	return b
}

func encodeDeath(bdSeq uint64) []byte {
	p := &pb.Payload{}
	ts := uint64(time.Now().UnixMilli())
	p.Timestamp = &ts
	m := &pb.Payload_Metric{}
	m.Name = strPtr("bdSeq")
	dt := uint32(pb.DataType_Int64)
	m.Datatype = &dt
	m.Value = &pb.Payload_Metric_LongValue{LongValue: bdSeq}
	p.Metrics = []*pb.Payload_Metric{m}
	b, _ := payload.Encode(p)
	return b
}

func strPtr(s string) *string { return &s }

func TestNewApplicationRequiresBroker(t *testing.T) {
	_, err := New(newMockTransport(), Config{})
	if err == nil {
		t.Fatal("expected error for missing broker")
	}
}

func TestApplicationConnectDisconnect(t *testing.T) {
	mt := newMockTransport()
	a, err := New(mt, Config{BrokerURL: "tcp://localhost:1883"})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := a.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if a.State() != Connected {
		t.Fatalf("expected Connected, got %s", a.State())
	}

	if err := a.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if a.State() != Disconnected {
		t.Fatalf("expected Disconnected, got %s", a.State())
	}
}

func TestApplicationServerURLsFallback(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{ServerURLs: []string{"tcp://fallback:1883"}})

	ctx := context.Background()
	if err := a.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if mt.opts.BrokerURL != "tcp://fallback:1883" {
		t.Fatalf("expected fallback URL, got %s", mt.opts.BrokerURL)
	}
}

func TestApplicationNoSTATEPublished(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{BrokerURL: "tcp://localhost:1883"})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	for _, msg := range mt.getPublished() {
		parsed, err := topic.Parse(msg.Topic)
		if err == nil && parsed.Type == topic.STATE {
			t.Fatal("Application must not publish STATE messages")
		}
	}
}

func TestApplicationSubscribesSparkplugNamespace(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{BrokerURL: "tcp://localhost:1883"})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	found := false
	for _, sub := range mt.subscribed {
		if sub.Topic == topic.Namespace+"/#" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected subscription to spBv1.0/#")
	}
}

func TestApplicationTracksNodeBirth(t *testing.T) {
	mt := newMockTransport()
	var birthGroup, birthNode string
	a, _ := New(mt, Config{
		BrokerURL: "tcp://localhost:1883",
		OnNodeBirth: func(g, n string, _ []*pb.Payload_Metric) {
			birthGroup = g
			birthNode = n
		},
	})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	tempMetric, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(22.5))
	mt.deliver("spBv1.0/plant/NBIRTH/gw1", encodeBirth(0, 1, tempMetric))

	ns := a.GetNode("plant", "gw1")
	if ns == nil {
		t.Fatal("node not tracked")
	}
	if !ns.Online {
		t.Fatal("node should be online")
	}
	if ns.BdSeq != 1 {
		t.Fatalf("expected bdSeq=1, got %d", ns.BdSeq)
	}
	if _, ok := ns.Metrics["temperature"]; !ok {
		t.Fatal("temperature metric not stored")
	}
	if birthGroup != "plant" || birthNode != "gw1" {
		t.Fatalf("callback got group=%s node=%s", birthGroup, birthNode)
	}
}

func TestApplicationTracksNodeDeath(t *testing.T) {
	mt := newMockTransport()
	var deathCalled bool
	a, _ := New(mt, Config{
		BrokerURL: "tcp://localhost:1883",
		OnNodeDeath: func(g, n string) {
			deathCalled = true
		},
	})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	mt.deliver("spBv1.0/plant/NBIRTH/gw1", encodeBirth(0, 5))
	mt.deliver("spBv1.0/plant/NDEATH/gw1", encodeDeath(5))

	ns := a.GetNode("plant", "gw1")
	if ns == nil {
		t.Fatal("node not tracked")
	}
	if ns.Online {
		t.Fatal("node should be offline")
	}
	if !ns.Stale {
		t.Fatal("node should be stale")
	}
	if !deathCalled {
		t.Fatal("death callback not called")
	}
}

func TestApplicationNDEATHBdSeqMismatch(t *testing.T) {
	mt := newMockTransport()
	var deathCalled bool
	a, _ := New(mt, Config{
		BrokerURL: "tcp://localhost:1883",
		OnNodeDeath: func(g, n string) {
			deathCalled = true
		},
	})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	mt.deliver("spBv1.0/plant/NBIRTH/gw1", encodeBirth(0, 5))
	mt.deliver("spBv1.0/plant/NDEATH/gw1", encodeDeath(99))

	ns := a.GetNode("plant", "gw1")
	if !ns.Online {
		t.Fatal("node should still be online after bdSeq mismatch")
	}
	if deathCalled {
		t.Fatal("death callback should not be called for bdSeq mismatch")
	}
}

func TestApplicationTracksDeviceBirthDeath(t *testing.T) {
	mt := newMockTransport()
	var dbirthDev, ddeathDev string
	a, _ := New(mt, Config{
		BrokerURL: "tcp://localhost:1883",
		OnDeviceBirth: func(g, n, d string, _ []*pb.Payload_Metric) {
			dbirthDev = d
		},
		OnDeviceDeath: func(g, n, d string) {
			ddeathDev = d
		},
	})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	mt.deliver("spBv1.0/plant/NBIRTH/gw1", encodeBirth(0, 1))

	sensorMetric, _ := payload.NewMetric("rpm", pb.DataType_Int32, int32(1500))
	mt.deliver("spBv1.0/plant/DBIRTH/gw1/motor1", encodeData(1, sensorMetric))

	ns := a.GetNode("plant", "gw1")
	ds := ns.Devices["motor1"]
	if ds == nil {
		t.Fatal("device not tracked")
	}
	if !ds.Online {
		t.Fatal("device should be online")
	}
	if dbirthDev != "motor1" {
		t.Fatalf("expected motor1, got %s", dbirthDev)
	}

	mt.deliver("spBv1.0/plant/DDEATH/gw1/motor1", encodeData(2))
	ds = ns.Devices["motor1"]
	if ds.Online {
		t.Fatal("device should be offline")
	}
	if ddeathDev != "motor1" {
		t.Fatalf("expected motor1 death, got %s", ddeathDev)
	}
}

func TestApplicationNodeData(t *testing.T) {
	mt := newMockTransport()
	var dataCalled bool
	a, _ := New(mt, Config{
		BrokerURL: "tcp://localhost:1883",
		OnNodeData: func(g, n string, _ []*pb.Payload_Metric) {
			dataCalled = true
		},
	})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	tempMetric, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(22.5))
	mt.deliver("spBv1.0/plant/NBIRTH/gw1", encodeBirth(0, 1, tempMetric))

	updated, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(25.0))
	mt.deliver("spBv1.0/plant/NDATA/gw1", encodeData(1, updated))

	ns := a.GetNode("plant", "gw1")
	m := ns.Metrics["temperature"]
	if m.GetDoubleValue() != 25.0 {
		t.Fatalf("expected 25.0, got %f", m.GetDoubleValue())
	}
	if !dataCalled {
		t.Fatal("data callback not called")
	}
}

func TestApplicationDeviceData(t *testing.T) {
	mt := newMockTransport()
	var dataCalled bool
	a, _ := New(mt, Config{
		BrokerURL: "tcp://localhost:1883",
		OnDeviceData: func(g, n, d string, _ []*pb.Payload_Metric) {
			dataCalled = true
		},
	})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	mt.deliver("spBv1.0/plant/NBIRTH/gw1", encodeBirth(0, 1))

	rpmMetric, _ := payload.NewMetric("rpm", pb.DataType_Int32, int32(1500))
	mt.deliver("spBv1.0/plant/DBIRTH/gw1/motor1", encodeData(1, rpmMetric))

	updated, _ := payload.NewMetric("rpm", pb.DataType_Int32, int32(1600))
	mt.deliver("spBv1.0/plant/DDATA/gw1/motor1", encodeData(2, updated))

	ns := a.GetNode("plant", "gw1")
	m := ns.Devices["motor1"].Metrics["rpm"]
	if m.GetIntValue() != 1600 {
		t.Fatalf("expected 1600, got %d", m.GetIntValue())
	}
	if !dataCalled {
		t.Fatal("data callback not called")
	}
}

func TestApplicationSeqGapRequestsRebirth(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{BrokerURL: "tcp://localhost:1883"})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	mt.deliver("spBv1.0/plant/NBIRTH/gw1", encodeBirth(0, 1))
	mt.deliver("spBv1.0/plant/NDATA/gw1", encodeData(5))

	time.Sleep(200 * time.Millisecond)

	pubs := mt.getPublished()
	found := false
	for _, p := range pubs {
		if p.Topic == "spBv1.0/plant/NCMD/gw1" {
			decoded, err := payload.Decode(p.Payload)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range decoded.Metrics {
				if m.GetName() == "Node Control/Rebirth" && m.GetBooleanValue() {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("expected rebirth request on seq gap")
	}
}

func TestApplicationAliasResolution(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{BrokerURL: "tcp://localhost:1883"})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	alias := uint64(100)
	tempMetric := &pb.Payload_Metric{}
	tempMetric.Name = strPtr("temperature")
	dt := uint32(pb.DataType_Double)
	tempMetric.Datatype = &dt
	tempMetric.Alias = &alias
	tempMetric.Value = &pb.Payload_Metric_DoubleValue{DoubleValue: 22.5}

	mt.deliver("spBv1.0/plant/NBIRTH/gw1", encodeBirth(0, 1, tempMetric))

	name, ok := a.ResolveAlias("plant", "gw1", 100)
	if !ok || name != "temperature" {
		t.Fatalf("expected temperature, got %s (ok=%v)", name, ok)
	}

	_, ok = a.ResolveAlias("plant", "gw1", 999)
	if ok {
		t.Fatal("expected false for unknown alias")
	}
}

func TestApplicationSendNodeCommand(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{BrokerURL: "tcp://localhost:1883"})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	m, _ := payload.NewMetric("Node Control/Rebirth", pb.DataType_Boolean, true)
	if err := a.SendNodeCommand(ctx, "plant", "gw1", []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}

	pubs := mt.getPublished()
	if len(pubs) == 0 {
		t.Fatal("expected publish")
	}
	if pubs[0].Topic != "spBv1.0/plant/NCMD/gw1" {
		t.Fatalf("expected NCMD topic, got %s", pubs[0].Topic)
	}
}

func TestApplicationSendDeviceCommand(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{BrokerURL: "tcp://localhost:1883"})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	m, _ := payload.NewMetric("reset", pb.DataType_Boolean, true)
	if err := a.SendDeviceCommand(ctx, "plant", "gw1", "motor1", []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}

	pubs := mt.getPublished()
	if pubs[0].Topic != "spBv1.0/plant/DCMD/gw1/motor1" {
		t.Fatalf("expected DCMD topic, got %s", pubs[0].Topic)
	}
}

func TestApplicationNotConnectedCommand(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{BrokerURL: "tcp://localhost:1883"})

	m, _ := payload.NewMetric("test", pb.DataType_Boolean, true)
	err := a.SendNodeCommand(context.Background(), "g", "n", []*pb.Payload_Metric{m})
	if err != ErrNotConnected {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}
}

func TestApplicationCloseIdempotent(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{BrokerURL: "tcp://localhost:1883"})

	ctx := context.Background()
	a.Connect(ctx)
	a.Close(ctx)
	a.Close(ctx)

	if a.State() != Closed {
		t.Fatalf("expected Closed, got %s", a.State())
	}
}

func TestApplicationConnectAfterClose(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{BrokerURL: "tcp://localhost:1883"})

	ctx := context.Background()
	a.Connect(ctx)
	a.Close(ctx)

	err := a.Connect(ctx)
	if err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestApplicationNodes(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{BrokerURL: "tcp://localhost:1883"})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	mt.deliver("spBv1.0/plant/NBIRTH/gw1", encodeBirth(0, 1))
	mt.deliver("spBv1.0/plant/NBIRTH/gw2", encodeBirth(0, 2))

	nodes := a.Nodes()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
}

func TestApplicationStateCallback(t *testing.T) {
	mt := newMockTransport()
	var transitions []string
	a, _ := New(mt, Config{
		BrokerURL: "tcp://localhost:1883",
		OnState: func(old, new State) {
			transitions = append(transitions, old.String()+"->"+new.String())
		},
	})

	ctx := context.Background()
	a.Connect(ctx)
	a.Disconnect(ctx)

	expected := []string{
		"disconnected->connecting",
		"connecting->connected",
		"connected->disconnected",
	}
	if len(transitions) != len(expected) {
		t.Fatalf("expected %d transitions, got %d: %v", len(expected), len(transitions), transitions)
	}
	for i, e := range expected {
		if transitions[i] != e {
			t.Fatalf("transition %d: expected %s, got %s", i, e, transitions[i])
		}
	}
}

func TestApplicationSTATEIgnored(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{BrokerURL: "tcp://localhost:1883"})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	mt.deliver("spBv1.0/STATE/scada", []byte(`{"online":true,"timestamp":1234}`))
	// Should not panic or alter state
	if len(a.Nodes()) != 0 {
		t.Fatal("STATE should not create nodes")
	}
}

func TestApplicationRequestRebirth(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{BrokerURL: "tcp://localhost:1883"})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	if err := a.RequestRebirth(ctx, "plant", "gw1"); err != nil {
		t.Fatal(err)
	}

	pubs := mt.getPublished()
	if len(pubs) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(pubs))
	}
	decoded, _ := payload.Decode(pubs[0].Payload)
	for _, m := range decoded.Metrics {
		if m.GetName() == "Node Control/Rebirth" && m.GetBooleanValue() {
			return
		}
	}
	t.Fatal("rebirth metric not found")
}

func TestApplicationNodeReplacement(t *testing.T) {
	mt := newMockTransport()
	a, _ := New(mt, Config{BrokerURL: "tcp://localhost:1883"})

	ctx := context.Background()
	a.Connect(ctx)
	defer a.Close(ctx)

	m1, _ := payload.NewMetric("temp", pb.DataType_Double, float64(22.5))
	mt.deliver("spBv1.0/plant/NBIRTH/gw1", encodeBirth(0, 1, m1))

	m2, _ := payload.NewMetric("pressure", pb.DataType_Double, float64(1013.0))
	mt.deliver("spBv1.0/plant/NBIRTH/gw1", encodeBirth(0, 2, m2))

	ns := a.GetNode("plant", "gw1")
	if ns.BdSeq != 2 {
		t.Fatalf("expected bdSeq=2 after rebirth, got %d", ns.BdSeq)
	}
	if _, ok := ns.Metrics["temp"]; ok {
		t.Fatal("old metrics should be cleared on rebirth")
	}
	if _, ok := ns.Metrics["pressure"]; !ok {
		t.Fatal("new metric should be present")
	}
}

func TestStateString(t *testing.T) {
	tests := []struct {
		s    State
		want string
	}{
		{Disconnected, "disconnected"},
		{Connecting, "connecting"},
		{Connected, "connected"},
		{Closed, "closed"},
		{State(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("State(%d).String() = %s, want %s", tt.s, got, tt.want)
		}
	}
}
