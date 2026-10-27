// SPDX-License-Identifier: MIT

package edge

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/otfabric/go-sparkplugb/mqtt"
	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

type mockTransport struct {
	mu          sync.Mutex
	connected   bool
	published   []publishedMsg
	subscribed  []subscribedTopic
	handlers    map[string]mqtt.MessageHandler
	connectOpts mqtt.ConnectOpts
	connectErr  error
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
	if m.connectErr != nil {
		return m.connectErr
	}
	m.connected = true
	m.connectOpts = opts
	return nil
}

func (m *mockTransport) Publish(_ context.Context, t string, qos byte, retained bool, p []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.connected {
		return errors.New("not connected")
	}
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

func (m *mockTransport) deliver(topic string, data []byte) {
	m.mu.Lock()
	h := m.handlers[topic]
	m.mu.Unlock()
	if h != nil {
		h(topic, data)
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
		GroupID:    "testGroup",
		EdgeNodeID: "testNode",
		BrokerURL:  "tcp://localhost:1883",
	}
}

func TestConnectAndBirth(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	if n.State() != Online {
		t.Errorf("state = %s, want online", n.State())
	}

	if tr.connectOpts.WillQoS != 1 {
		t.Errorf("will QoS = %d, want 1", tr.connectOpts.WillQoS)
	}
	if tr.connectOpts.WillRetained {
		t.Error("will should not be retained")
	}
	if !tr.connectOpts.CleanSession {
		t.Error("clean session should be true")
	}

	willPayload, err := payload.Decode(tr.connectOpts.WillPayload)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range willPayload.Metrics {
		if m.GetName() == "bdSeq" {
			found = true
		}
	}
	if !found {
		t.Error("Will payload missing bdSeq metric")
	}

	tempMetric, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(22.5))
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{tempMetric}); err != nil {
		t.Fatal(err)
	}

	msgs := tr.getPublished()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(msgs))
	}

	expectedTopic := "spBv1.0/testGroup/NBIRTH/testNode"
	if msgs[0].Topic != expectedTopic {
		t.Errorf("topic = %q, want %q", msgs[0].Topic, expectedTopic)
	}
	if msgs[0].QoS != 0 {
		t.Errorf("QoS = %d, want 0", msgs[0].QoS)
	}
	if msgs[0].Retained {
		t.Error("NBIRTH should not be retained")
	}

	p, err := payload.Decode(msgs[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if p.GetSeq() != 0 {
		t.Errorf("NBIRTH seq = %d, want 0", p.GetSeq())
	}
	if len(p.Metrics) != 2 {
		t.Fatalf("expected 2 metrics (bdSeq + temperature), got %d", len(p.Metrics))
	}
}

func TestSequenceNumbering(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}

	dm, _ := payload.NewMetric("y", pb.DataType_Int32, int32(1))
	if err := n.PublishDeviceBirth(ctx, "dev1", []*pb.Payload_Metric{dm}); err != nil {
		t.Fatal(err)
	}
	if err := n.PublishNodeData(ctx, []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}
	if err := n.PublishDeviceData(ctx, "dev1", []*pb.Payload_Metric{dm}); err != nil {
		t.Fatal(err)
	}

	msgs := tr.getPublished()
	expectedSeqs := []uint64{0, 1, 2, 3}
	for i, expected := range expectedSeqs {
		p, err := payload.Decode(msgs[i].Payload)
		if err != nil {
			t.Fatal(err)
		}
		if p.GetSeq() != expected {
			t.Errorf("msg[%d] seq = %d, want %d", i, p.GetSeq(), expected)
		}
	}
}

func TestSequenceWrapping(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 255; i++ {
		if err := n.PublishNodeData(ctx, []*pb.Payload_Metric{m}); err != nil {
			t.Fatal(err)
		}
	}

	if err := n.PublishNodeData(ctx, []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}

	if n.Seq() != 0 {
		t.Errorf("seq after wrap = %d, want 0", n.Seq())
	}
}

func TestBdSeqIncrementsOnReconnect(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	first := n.BdSeq()

	if err := n.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}

	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	second := n.BdSeq()

	if second != (first+1)%256 {
		t.Errorf("bdSeq = %d, want %d", second, (first+1)%256)
	}
}

func TestDisconnectPublishesNDEATH(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}

	if err := n.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}

	msgs := tr.getPublished()
	last := msgs[len(msgs)-1]
	expectedTopic := "spBv1.0/testGroup/NDEATH/testNode"
	if last.Topic != expectedTopic {
		t.Errorf("last published topic = %q, want %q", last.Topic, expectedTopic)
	}
	if last.QoS != 1 {
		t.Errorf("NDEATH QoS = %d, want 1", last.QoS)
	}
}

func TestRebirthCommand(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}

	rebirthPayload := &pb.Payload{
		Metrics: []*pb.Payload_Metric{payload.RebirthMetric(true)},
	}
	b, _ := payload.Encode(rebirthPayload)

	ncmdTopic := topic.ForNCMD("testGroup", "testNode")
	tr.deliver(ncmdTopic, b)

	n.mu.Lock()
	born := n.born
	n.mu.Unlock()
	if born {
		t.Error("born should be false after rebirth command")
	}
}

func TestPublishBeforeBirth(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	err = n.PublishNodeData(ctx, []*pb.Payload_Metric{m})
	if !errors.Is(err, ErrNoBirth) {
		t.Errorf("expected ErrNoBirth, got %v", err)
	}
}

func TestPrimaryHostWait(t *testing.T) {
	tr := newMockTransport()
	cfg := testConfig()
	cfg.PrimaryHostID = "myHost"
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	if n.State() != WaitingForPrimaryHost {
		t.Errorf("state = %s, want waiting_for_primary_host", n.State())
	}

	stateTopic := topic.ForState("myHost")
	sp := statePayload{Online: true, Timestamp: 1234567890}
	b, _ := json.Marshal(sp)
	tr.deliver(stateTopic, b)

	if n.State() != Online {
		t.Errorf("state = %s, want online", n.State())
	}
}

func TestDeviceDeath(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}

	dm, _ := payload.NewMetric("y", pb.DataType_Int32, int32(1))
	if err := n.PublishDeviceBirth(ctx, "dev1", []*pb.Payload_Metric{dm}); err != nil {
		t.Fatal(err)
	}

	if err := n.PublishDeviceDeath(ctx, "dev1"); err != nil {
		t.Fatal(err)
	}

	msgs := tr.getPublished()
	last := msgs[len(msgs)-1]
	if last.Topic != "spBv1.0/testGroup/DDEATH/testNode/dev1" {
		t.Errorf("topic = %q", last.Topic)
	}
}

func TestStateTransitions(t *testing.T) {
	var transitions []string
	tr := newMockTransport()
	cfg := testConfig()
	cfg.OnState = func(old, new State) {
		transitions = append(transitions, old.String()+"->"+new.String())
	}
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = n.Connect(ctx)
	_ = n.Disconnect(ctx)

	expected := []string{
		"disconnected->connecting",
		"connecting->subscribing",
		"subscribing->online",
		"online->disconnected",
	}
	if len(transitions) != len(expected) {
		t.Fatalf("transitions = %v, want %v", transitions, expected)
	}
	for i, e := range expected {
		if transitions[i] != e {
			t.Errorf("transition[%d] = %s, want %s", i, transitions[i], e)
		}
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = n.Connect(ctx)
	_ = n.Close(ctx)
	_ = n.Close(ctx)

	if n.State() != Closed {
		t.Errorf("state = %s, want closed", n.State())
	}
}

func TestConnectAfterClose(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = n.Close(ctx)

	err = n.Connect(ctx)
	if !errors.Is(err, ErrClosed) {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestSubscriptions(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	if len(tr.subscribed) < 2 {
		t.Fatalf("expected at least 2 subscriptions, got %d", len(tr.subscribed))
	}

	ncmd := tr.subscribed[0]
	if ncmd.Topic != "spBv1.0/testGroup/NCMD/testNode" {
		t.Errorf("NCMD subscription = %q", ncmd.Topic)
	}
	if ncmd.QoS != 1 {
		t.Errorf("NCMD QoS = %d, want 1", ncmd.QoS)
	}
}

func TestConnectFailure(t *testing.T) {
	tr := newMockTransport()
	tr.connectErr = errors.New("connection refused")
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	err = n.Connect(ctx)
	if err == nil {
		t.Fatal("expected error on connect failure")
	}
	if n.State() != Disconnected {
		t.Errorf("state = %s, want disconnected after failure", n.State())
	}
}

func TestBdSeqMatchesBetweenWillAndBirth(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	willP, err := payload.Decode(tr.connectOpts.WillPayload)
	if err != nil {
		t.Fatal(err)
	}
	var willBdSeq uint64
	for _, m := range willP.Metrics {
		if m.GetName() == "bdSeq" {
			willBdSeq = m.GetLongValue()
		}
	}

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}

	msgs := tr.getPublished()
	birthP, err := payload.Decode(msgs[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	var birthBdSeq uint64
	for _, m := range birthP.Metrics {
		if m.GetName() == "bdSeq" {
			birthBdSeq = m.GetLongValue()
		}
	}

	if willBdSeq != birthBdSeq {
		t.Errorf("Will bdSeq=%d != NBIRTH bdSeq=%d", willBdSeq, birthBdSeq)
	}
}

func TestNDeathHasNoSeq(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	willP, err := payload.Decode(tr.connectOpts.WillPayload)
	if err != nil {
		t.Fatal(err)
	}
	if willP.Seq != nil {
		t.Errorf("NDEATH should not contain seq, got %d", willP.GetSeq())
	}
}

func TestNewValidation(t *testing.T) {
	tr := newMockTransport()

	_, err := New(tr, Config{})
	if err == nil {
		t.Error("expected error for empty config")
	}

	_, err = New(tr, Config{GroupID: "g", EdgeNodeID: "n"})
	if err == nil {
		t.Error("expected error for missing broker URL")
	}
}

func TestNewWithServerURLs(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, Config{
		GroupID:    "g",
		EdgeNodeID: "n",
		ServerURLs: []string{"tcp://broker1:1883", "tcp://broker2:1883"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if tr.connectOpts.BrokerURL != "tcp://broker1:1883" {
		t.Errorf("broker = %q, want first ServerURL", tr.connectOpts.BrokerURL)
	}
}

func TestStateStringUnknown(t *testing.T) {
	s := State(99)
	if s.String() != "unknown" {
		t.Errorf("State(99).String() = %q, want unknown", s.String())
	}
}

func TestConnectBdSeqLoadError(t *testing.T) {
	tr := newMockTransport()
	cfg := testConfig()
	cfg.BdSeqStore = &failBdSeqStore{loadErr: errors.New("disk fail")}
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = n.Connect(context.Background())
	if err == nil || n.State() != Disconnected {
		t.Errorf("expected error and Disconnected state, got err=%v state=%s", err, n.State())
	}
}

func TestConnectBdSeqStoreError(t *testing.T) {
	tr := newMockTransport()
	cfg := testConfig()
	cfg.BdSeqStore = &failBdSeqStore{storeErr: errors.New("disk fail")}
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = n.Connect(context.Background())
	if err == nil || n.State() != Disconnected {
		t.Errorf("expected error and Disconnected state, got err=%v state=%s", err, n.State())
	}
}

type failBdSeqStore struct {
	loadErr  error
	storeErr error
}

func (f *failBdSeqStore) Load() (uint64, error)  { return 0, f.loadErr }
func (f *failBdSeqStore) Store(uint64) error      { return f.storeErr }

func TestSubscribeNCMDFailure(t *testing.T) {
	tr := &failSubscribeTransport{mockTransport: newMockTransport(), failTopic: "spBv1.0/testGroup/NCMD/testNode"}
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	err = n.Connect(context.Background())
	if err == nil {
		t.Error("expected subscribe NCMD error")
	}
}

func TestSubscribeDCMDFailure(t *testing.T) {
	tr := &failSubscribeTransport{mockTransport: newMockTransport(), failTopic: "spBv1.0/testGroup/DCMD/testNode/+"}
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	err = n.Connect(context.Background())
	if err == nil {
		t.Error("expected subscribe DCMD error")
	}
}

func TestSubscribeSTATEFailure(t *testing.T) {
	tr := &failSubscribeTransport{mockTransport: newMockTransport(), failTopic: "spBv1.0/STATE/myHost"}
	cfg := testConfig()
	cfg.PrimaryHostID = "myHost"
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = n.Connect(context.Background())
	if err == nil {
		t.Error("expected subscribe STATE error")
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

func TestHandleUnparseableTopic(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	// Deliver to NCMD handler with a bad topic string — should not panic
	ncmdTopic := topic.ForNCMD("testGroup", "testNode")
	tr.mu.Lock()
	h := tr.handlers[ncmdTopic]
	tr.mu.Unlock()
	h("bad-topic", []byte("not-protobuf"))
}

func TestHandleMalformedPayload(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	ncmdTopic := topic.ForNCMD("testGroup", "testNode")
	tr.deliver(ncmdTopic, []byte{0xFF, 0xFF, 0xFF})
}

func TestHandleDCMD(t *testing.T) {
	tr := newMockTransport()
	var dcmdCalled bool
	cfg := testConfig()
	cfg.OnDeviceCmd = func(deviceID string, metrics []*pb.Payload_Metric) {
		dcmdCalled = true
		if deviceID != "dev1" {
			t.Errorf("deviceID = %q, want dev1", deviceID)
		}
	}
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	m, _ := payload.NewMetric("setpoint", pb.DataType_Double, float64(42.0))
	p := &pb.Payload{Metrics: []*pb.Payload_Metric{m}}
	b, _ := payload.Encode(p)

	dcmdTopic := topic.Topic{GroupID: "testGroup", Type: topic.DCMD, EdgeNodeID: "testNode", DeviceID: "dev1"}.String()
	tr.mu.Lock()
	h := tr.handlers["spBv1.0/testGroup/DCMD/testNode/+"]
	tr.mu.Unlock()
	h(dcmdTopic, b)

	if !dcmdCalled {
		t.Error("OnDeviceCmd not called")
	}
}

func TestNCMDNonRebirth(t *testing.T) {
	tr := newMockTransport()
	var cmdCalled bool
	cfg := testConfig()
	cfg.OnNodeCmd = func(deviceID string, metrics []*pb.Payload_Metric) {
		cmdCalled = true
	}
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	m, _ := payload.NewMetric("setpoint", pb.DataType_Double, float64(42.0))
	p := &pb.Payload{Metrics: []*pb.Payload_Metric{m}}
	b, _ := payload.Encode(p)
	ncmdTopic := topic.ForNCMD("testGroup", "testNode")
	tr.deliver(ncmdTopic, b)

	if !cmdCalled {
		t.Error("OnNodeCmd not called for non-rebirth NCMD")
	}
}

func TestStaleConnectionGeneration(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	// Capture old handler
	ncmdTopic := topic.ForNCMD("testGroup", "testNode")
	tr.mu.Lock()
	oldHandler := tr.handlers[ncmdTopic]
	tr.mu.Unlock()

	// Reconnect — bumps generation
	if err := n.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}

	// Deliver via old handler — should be ignored (stale gen)
	rebirthPayload := &pb.Payload{
		Metrics: []*pb.Payload_Metric{payload.RebirthMetric(true)},
	}
	b, _ := payload.Encode(rebirthPayload)
	oldHandler(ncmdTopic, b)

	n.mu.Lock()
	born := n.born
	n.mu.Unlock()
	if !born {
		t.Error("stale handler should not have reset born flag")
	}
}

func TestPrimaryHostGoesOffline(t *testing.T) {
	tr := newMockTransport()
	cfg := testConfig()
	cfg.PrimaryHostID = "myHost"
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	stateTopic := topic.ForState("myHost")
	// Go online
	b, _ := json.Marshal(statePayload{Online: true, Timestamp: 1})
	tr.deliver(stateTopic, b)
	if n.State() != Online {
		t.Fatalf("state = %s, want online", n.State())
	}

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}

	// Host goes offline
	b, _ = json.Marshal(statePayload{Online: false, Timestamp: 2})
	tr.deliver(stateTopic, b)
	if n.State() != Disconnected {
		t.Errorf("state = %s, want disconnected after host offline", n.State())
	}
	n.mu.Lock()
	born := n.born
	n.mu.Unlock()
	if born {
		t.Error("born should be false after host goes offline")
	}
}

func TestHandleMalformedStatePayload(t *testing.T) {
	tr := newMockTransport()
	cfg := testConfig()
	cfg.PrimaryHostID = "myHost"
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	stateTopic := topic.ForState("myHost")
	tr.deliver(stateTopic, []byte("not-json"))

	if n.State() != WaitingForPrimaryHost {
		t.Errorf("state = %s, want waiting_for_primary_host after malformed STATE", n.State())
	}
}

func TestPublishBirthNotOnline(t *testing.T) {
	tr := newMockTransport()
	cfg := testConfig()
	cfg.PrimaryHostID = "myHost"
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	err = n.PublishBirth(ctx, []*pb.Payload_Metric{m})
	if !errors.Is(err, ErrNotConnected) {
		t.Errorf("expected ErrNotConnected while waiting for host, got %v", err)
	}
}

func TestPublishDeviceDataBeforeBirth(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	err = n.PublishDeviceData(ctx, "dev1", []*pb.Payload_Metric{m})
	if !errors.Is(err, ErrNoBirth) {
		t.Errorf("expected ErrNoBirth, got %v", err)
	}
}

func TestPublishDeviceDeathBeforeBirth(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	err = n.PublishDeviceDeath(ctx, "dev1")
	if !errors.Is(err, ErrNoBirth) {
		t.Errorf("expected ErrNoBirth, got %v", err)
	}
}

func TestPublishDeviceBirthBeforeBirth(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	err = n.PublishDeviceBirth(ctx, "dev1", []*pb.Payload_Metric{m})
	if !errors.Is(err, ErrNoBirth) {
		t.Errorf("expected ErrNoBirth, got %v", err)
	}
}

func TestDisconnectWhenAlreadyDisconnected(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Disconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWithLogger(t *testing.T) {
	tr := newMockTransport()
	cfg := testConfig()
	cfg.Logger = &testLogger{}
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	// Trigger logger paths: malformed topic, malformed payload, rebirth
	ncmdTopic := topic.ForNCMD("testGroup", "testNode")
	tr.mu.Lock()
	h := tr.handlers[ncmdTopic]
	tr.mu.Unlock()
	h("bad-topic", []byte{})
	h(ncmdTopic, []byte{0xFF, 0xFF})

	rebirthPayload := &pb.Payload{Metrics: []*pb.Payload_Metric{payload.RebirthMetric(true)}}
	b, _ := payload.Encode(rebirthPayload)
	h(ncmdTopic, b)
}

type testLogger struct{}

func (l *testLogger) Debugf(format string, args ...any) {}
func (l *testLogger) Infof(format string, args ...any)  {}
func (l *testLogger) Warnf(format string, args ...any)  {}
func (l *testLogger) Errorf(format string, args ...any) {}

func TestDeviceDeathUnknownDevice(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	m, _ := payload.NewMetric("x", pb.DataType_Int32, int32(0))
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{m}); err != nil {
		t.Fatal(err)
	}
	// Publish device death for a device that was never born — should not panic
	if err := n.PublishDeviceDeath(ctx, "unknown_device"); err != nil {
		t.Fatal(err)
	}
}
