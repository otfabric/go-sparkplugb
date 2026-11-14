// SPDX-License-Identifier: MIT

package edge

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/mqtt"
	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

// gate holds the calls of a transport method until it is opened, and tells
// when the first call arrived.
type gate struct {
	arrived chan struct{}
	open    chan struct{}
	once    sync.Once
}

func newGate() *gate { return &gate{arrived: make(chan struct{}), open: make(chan struct{})} }

func (g *gate) pass() {
	if g == nil {
		return
	}
	g.once.Do(func() { close(g.arrived) })
	<-g.open
}

// eventTransport is the mock transport with gates on some of its methods,
// and the topics it publishes and subscribes to sent to channels.
type eventTransport struct {
	*mockTransport
	mu                             sync.Mutex
	connect, subscribe, disconnect *gate
	subscribeTopic                 string // the subscribe gate holds this topic only
	disconnects                    atomic.Int32
	published                      chan publishedOn
	subscribed                     chan string
}

type publishedOn struct {
	topic, broker string
	payload       []byte
}

func newEventTransport() *eventTransport {
	return &eventTransport{
		mockTransport: newMockTransport(),
		published:     make(chan publishedOn, 256),
		subscribed:    make(chan string, 256),
	}
}

func (e *eventTransport) gates() (connect, subscribe, disconnect *gate, subscribeTopic string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.connect, e.subscribe, e.disconnect, e.subscribeTopic
}

func (e *eventTransport) Connect(ctx context.Context, opts mqtt.ConnectOpts) error {
	g, _, _, _ := e.gates()
	g.pass()
	return e.mockTransport.Connect(ctx, opts)
}

func (e *eventTransport) Subscribe(ctx context.Context, t string, qos byte, h mqtt.MessageHandler) error {
	if _, g, _, gt := e.gates(); t == gt {
		g.pass()
	}
	err := e.mockTransport.Subscribe(ctx, t, qos, h)
	e.subscribed <- t
	return err
}

func (e *eventTransport) Publish(ctx context.Context, t string, qos byte, retained bool, p []byte) error {
	err := e.mockTransport.Publish(ctx, t, qos, retained, p)
	if err == nil {
		e.mockTransport.mu.Lock()
		broker := e.connectOpts.BrokerURL
		e.mockTransport.mu.Unlock()
		e.published <- publishedOn{t, broker, p}
	}
	return err
}

func (e *eventTransport) Disconnect(ctx context.Context) error {
	e.disconnects.Add(1)
	_, _, g, _ := e.gates()
	g.pass()
	return e.mockTransport.Disconnect(ctx)
}

// wait fails the test when ch is not closed, or does not deliver, within a
// bound far above the time the event takes.
func wait[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: not within 10s", what)
		panic("unreachable")
	}
}

// within runs f and fails the test when it does not return in the bound of
// wait.
func within(t *testing.T, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { f(); close(done) }()
	wait(t, what, done)
}

// nextPublished returns the next message the transport publishes on a topic
// of type mt.
func nextPublished(t *testing.T, e *eventTransport, mt topic.MessageType) publishedOn {
	t.Helper()
	for {
		p := wait(t, "a "+mt.String(), e.published)
		if tp, err := topic.Parse(p.topic); err == nil && tp.Type == mt {
			return p
		}
	}
}

// subscribedTo waits until the transport subscribed to want.
func subscribedTo(t *testing.T, e *eventTransport, want string) {
	t.Helper()
	for wait(t, "the subscription to "+want, e.subscribed) != want {
	}
}

func hostState(online bool, ts int64) []byte {
	return fmt.Appendf(nil, `{"online":%t,"timestamp":%d}`, online, ts)
}

func intMetric(t *testing.T, name string, v int32) *pb.Payload_Metric {
	t.Helper()
	m, err := payload.NewMetric(name, pb.DataType_Int32, v)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// L2: Close from two goroutines at once.
func TestCloseFromTwoGoroutines(t *testing.T) {
	tr := newEventTransport()
	tr.disconnect = newGate()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{intMetric(t, "x", 1)}); err != nil {
		t.Fatal(err)
	}

	first := make(chan error, 1)
	go func() { first <- n.Close(ctx) }()
	wait(t, "first Close reaching the transport", tr.disconnect.arrived)
	if err := n.Close(ctx); err != nil {
		t.Errorf("second Close: %v", err)
	}
	close(tr.disconnect.open)
	if err := wait(t, "first Close", first); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if s := n.State(); s != Closed {
		t.Errorf("state %s, want closed", s)
	}
	if c := tr.disconnects.Load(); c != 1 {
		t.Errorf("transport disconnected %d times, want 1", c)
	}
	if err := n.Connect(ctx); !errors.Is(err, ErrClosed) {
		t.Errorf("Connect after Close: %v, want ErrClosed", err)
	}
}

// L5: Close while Connect waits for the transport.
func TestCloseDuringConnect(t *testing.T) {
	for _, step := range []string{"connect", "subscribe NCMD", "subscribe DCMD", "subscribe STATE"} {
		t.Run(step, func(t *testing.T) {
			tr := newEventTransport()
			g := newGate()
			switch step {
			case "connect":
				tr.connect = g
			case "subscribe NCMD":
				tr.subscribe, tr.subscribeTopic = g, topic.ForNCMD("testGroup", "testNode")
			case "subscribe DCMD":
				tr.subscribe, tr.subscribeTopic = g, "spBv1.0/testGroup/DCMD/testNode/+"
			case "subscribe STATE":
				tr.subscribe, tr.subscribeTopic = g, topic.ForState("host")
			}
			cfg := testConfig()
			cfg.PrimaryHostID = "host"
			n, err := New(tr, cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			connected := make(chan error, 1)
			go func() { connected <- n.Connect(ctx) }()
			wait(t, "Connect reaching the "+step, g.arrived)

			if err := n.Close(ctx); err != nil {
				t.Errorf("Close: %v", err)
			}
			close(g.open)
			if err := wait(t, "Connect", connected); !errors.Is(err, ErrClosed) {
				t.Errorf("Connect during Close: %v, want ErrClosed", err)
			}
			if s := n.State(); s != Closed {
				t.Errorf("state %s after Close, want closed", s)
			}
			if tr.isConnected() {
				t.Error("transport still connected after Close")
			}
			// The primary host's STATE on the connection that ended does not
			// bring the node online.
			tr.deliver(topic.ForState("host"), hostState(true, 1))
			if s := n.State(); s != Closed {
				t.Errorf("state %s after a STATE online, want closed", s)
			}
			if err := n.Close(ctx); err != nil {
				t.Errorf("Close again: %v", err)
			}
		})
	}
}

// L9, L10: a StateHandler that calls the node.
func TestStateHandlerCallsNode(t *testing.T) {
	type tr struct{ old, new State }
	birth := func(n *Node, new State) {
		if new == Online {
			if err := n.PublishBirth(context.Background(), nil); err != nil {
				panic(err)
			}
		}
	}
	for _, tc := range []struct {
		name        string
		primaryHost bool
		react       func(n *Node, old, new State)
		want        []tr
		final       State
		births      int
	}{
		{
			name:  "State",
			react: func(n *Node, _, _ State) { _ = n.State() },
			want:  []tr{{Disconnected, Connecting}, {Connecting, Subscribing}, {Subscribing, Online}, {Online, Disconnected}},
			final: Disconnected,
		},
		{
			name:   "PublishBirth when online",
			react:  func(n *Node, _, new State) { birth(n, new) },
			want:   []tr{{Disconnected, Connecting}, {Connecting, Subscribing}, {Subscribing, Online}, {Online, Disconnected}},
			final:  Disconnected,
			births: 1,
		},
		{
			name:        "PublishBirth when the primary host is online",
			primaryHost: true,
			react:       func(n *Node, _, new State) { birth(n, new) },
			want:        []tr{{Disconnected, Connecting}, {Connecting, Subscribing}, {Subscribing, WaitingForPrimaryHost}, {WaitingForPrimaryHost, Online}, {Online, Disconnected}},
			final:       Disconnected,
			births:      1,
		},
		{
			name: "Close when disconnected",
			react: func(n *Node, _, new State) {
				if new == Disconnected {
					_ = n.Close(context.Background())
				}
			},
			want:  []tr{{Disconnected, Connecting}, {Connecting, Subscribing}, {Subscribing, Online}, {Online, Disconnected}, {Disconnected, Closed}},
			final: Closed,
		},
		{
			name: "Connect when disconnected",
			react: func(n *Node, old, new State) {
				birth(n, new)
				if old == Online && new == Disconnected {
					_ = n.Connect(context.Background())
				}
			},
			want: []tr{{Disconnected, Connecting}, {Connecting, Subscribing}, {Subscribing, Online}, {Online, Disconnected},
				{Disconnected, Connecting}, {Connecting, Subscribing}, {Subscribing, Online}},
			final:  Online,
			births: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEventTransport()
			var n *Node
			var mu sync.Mutex
			var got []tr
			cfg := testConfig()
			if tc.primaryHost {
				cfg.PrimaryHostID = "host"
			}
			cfg.OnState = func(old, new State) {
				mu.Lock()
				got = append(got, tr{old, new})
				mu.Unlock()
				tc.react(n, old, new)
			}
			n, err := New(e, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close(context.Background())

			within(t, "Connect with a StateHandler that calls the node", func() {
				if err := n.Connect(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if tc.primaryHost {
				within(t, "the primary host's STATE with a StateHandler that calls the node", func() {
					e.deliver(topic.ForState("host"), hostState(true, 1))
				})
			}
			lost := e.connectOpts.OnConnectionLost
			within(t, "the connection-lost callback with a StateHandler that calls the node", func() {
				lost(mqtt.ErrConnectionLost)
			})

			if s := n.State(); s != tc.final {
				t.Errorf("state %s, want %s", s, tc.final)
			}
			births := 0
			for _, p := range e.getPublished() {
				if strings.Contains(p.Topic, "/NBIRTH/") {
					births++
				}
			}
			if births != tc.births {
				t.Errorf("%d NBIRTHs published, want %d", births, tc.births)
			}
			mu.Lock()
			defer mu.Unlock()
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("transitions %v, want %v", got, tc.want)
			}
		})
	}
}

// oneSubscribeFails is the mock transport with its first Subscribe failing.
type oneSubscribeFails struct {
	*mockTransport
	failed atomic.Bool
}

func (o *oneSubscribeFails) Subscribe(ctx context.Context, t string, qos byte, h mqtt.MessageHandler) error {
	if o.failed.CompareAndSwap(false, true) {
		return errors.New("subscribe failed")
	}
	return o.mockTransport.Subscribe(ctx, t, qos, h)
}

// A StateHandler that calls Connect when the node is Disconnected is called
// once the node has ended the connection, so what it opens stays up: after
// the caller's Disconnect, and after a Connect that failed at a subscription.
func TestStateHandlerConnectsWhenDisconnected(t *testing.T) {
	for _, cause := range []string{"Disconnect", "Connect that fails"} {
		t.Run(cause, func(t *testing.T) {
			tr := &oneSubscribeFails{mockTransport: newMockTransport()}
			tr.failed.Store(cause == "Disconnect") // then no Subscribe fails
			var n *Node
			var again atomic.Bool
			cfg := testConfig()
			cfg.OnState = func(_, new State) {
				if new == Disconnected && again.CompareAndSwap(true, false) {
					if err := n.Connect(context.Background()); err != nil {
						t.Errorf("Connect from the StateHandler: %v", err)
					}
				}
			}
			n, err := New(tr, cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			defer n.Close(ctx)

			if cause == "Disconnect" {
				if err := n.Connect(ctx); err != nil {
					t.Fatal(err)
				}
				again.Store(true)
				if err := n.Disconnect(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				again.Store(true)
				if err := n.Connect(ctx); err == nil {
					t.Fatal("Connect with a Subscribe that fails succeeded")
				}
			}
			if s := n.State(); s != Online {
				t.Errorf("state %s after the StateHandler connected, want %s", s, Online)
			}
			if !tr.isConnected() {
				t.Error("the connection the StateHandler opened is not up")
			}
		})
	}
}

// walkConfig is a node with two servers and a primary host, that publishes
// its NBIRTH whenever it comes online.
func walkConfig(t *testing.T, n **Node, connects chan<- error) Config {
	cfg := testConfig()
	cfg.BrokerURL = ""
	cfg.ServerURLs = []string{"tcp://a:1883", "tcp://b:1883"}
	cfg.PrimaryHostID = "host"
	cfg.Metrics = connectMetrics(connects)
	cfg.OnState = func(_, new State) {
		if new == Online {
			if err := (*n).PublishBirth(context.Background(), []*pb.Payload_Metric{intMetric(t, "x", 1)}); err != nil {
				t.Errorf("PublishBirth when online: %v", err)
			}
		}
	}
	return cfg
}

// connectMetrics sends the result of every connect of a walk to a channel.
type connectMetrics chan<- error

func (c connectMetrics) OnConnect(_ string, _ time.Duration, err error) { c <- err }
func (connectMetrics) OnPublish(string, time.Duration, error)           {}
func (connectMetrics) OnMessage(string)                                 {}
func (connectMetrics) OnStateChange(State, State)                       {}

// After the primary host goes offline, the node walks to the next server
// and births there once the primary host is online on it.
func TestWalkBirthsOnTheNextServer(t *testing.T) {
	e := newEventTransport()
	var n *Node
	connects := make(chan error, 8)
	n, err := New(e, walkConfig(t, &n, connects))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close(context.Background())
	if err := n.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	subscribedTo(t, e, topic.ForState("host"))
	within(t, "the primary host's STATE", func() { e.deliver(topic.ForState("host"), hostState(true, 1)) })
	if p := nextPublished(t, e, topic.NBIRTH); p.broker != "tcp://a:1883" {
		t.Errorf("first NBIRTH on %s, want tcp://a:1883", p.broker)
	}

	e.deliver(topic.ForState("host"), hostState(false, 2))
	if err := wait(t, "the connect of the walk", connects); err != nil {
		t.Fatalf("walk: %v", err)
	}
	subscribedTo(t, e, topic.ForState("host"))
	within(t, "the primary host's STATE on the next server", func() { e.deliver(topic.ForState("host"), hostState(true, 3)) })
	if p := nextPublished(t, e, topic.NBIRTH); p.broker != "tcp://b:1883" {
		t.Errorf("NBIRTH after the walk on %s, want tcp://b:1883", p.broker)
	}
	if s := n.State(); s != Online {
		t.Errorf("state %s after the walk, want online", s)
	}
}

// L16: Close while the walk connects to the next server.
func TestCloseDuringWalk(t *testing.T) {
	e := newEventTransport()
	var n *Node
	connects := make(chan error, 8)
	n, err := New(e, walkConfig(t, &n, connects))
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	subscribedTo(t, e, topic.ForState("host"))
	within(t, "the primary host's STATE", func() { e.deliver(topic.ForState("host"), hostState(true, 1)) })
	nextPublished(t, e, topic.NBIRTH)

	g := newGate()
	e.mu.Lock()
	e.connect = g
	e.mu.Unlock()
	e.deliver(topic.ForState("host"), hostState(false, 2))
	wait(t, "the walk's connect reaching the transport", g.arrived)
	if err := n.Close(context.Background()); err != nil {
		t.Errorf("Close: %v", err)
	}
	close(g.open)
	if err := wait(t, "the connect of the walk", connects); !errors.Is(err, ErrClosed) {
		t.Errorf("walk's Connect during Close: %v, want ErrClosed", err)
	}
	if s := n.State(); s != Closed {
		t.Errorf("state %s after Close, want closed", s)
	}
	if e.isConnected() {
		t.Error("transport still connected after Close")
	}
}

// errorLog records the entries logged at error level.
type errorLog struct {
	mu      sync.Mutex
	entries []string
}

func (l *errorLog) Debugf(string, ...any) {}
func (l *errorLog) Infof(string, ...any)  {}
func (l *errorLog) Warnf(string, ...any)  {}

func (l *errorLog) Errorf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, fmt.Sprintf(format, args...))
}

func (l *errorLog) count(sub string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := 0
	for _, e := range l.entries {
		if strings.Contains(e, sub) {
			c++
		}
	}
	return c
}

// Application code that does not return normally is logged once, with its
// stack, and the node goes on.
func TestCallbackThatDoesNotReturnNormally(t *testing.T) {
	tr := newMockTransport()
	log := &errorLog{}
	var mu sync.Mutex
	var cmds []string
	cfg := testConfig()
	cfg.Logger = log
	cfg.OnState = func(_, new State) {
		if new == Online {
			panic("state handler")
		}
	}
	cfg.OnNodeCmd = func(_ string, ms []*pb.Payload_Metric) {
		if ms[0].GetName() == "bad" {
			panic("node command handler")
		}
		mu.Lock()
		cmds = append(cmds, ms[0].GetName())
		mu.Unlock()
	}
	cfg.OnDeviceCmd = func(string, []*pb.Payload_Metric) { panic("device command handler") }
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close(context.Background())
	if err := n.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if s := n.State(); s != Online {
		t.Fatalf("state %s, want online", s)
	}

	cmd := func(name string) []byte {
		b, err := payload.Encode(&pb.Payload{Metrics: []*pb.Payload_Metric{intMetric(t, name, 1)}})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	ncmd := topic.ForNCMD("testGroup", "testNode")
	tr.deliver(ncmd, cmd("bad"))
	tr.deliver(ncmd, cmd("good"))
	tr.mu.Lock()
	dcmd := tr.handlers["spBv1.0/testGroup/DCMD/testNode/+"]
	tr.mu.Unlock()
	dcmd("spBv1.0/testGroup/DCMD/testNode/d1", cmd("x"))

	mu.Lock()
	if fmt.Sprint(cmds) != "[good]" {
		t.Errorf("commands delivered %v, want [good]", cmds)
	}
	mu.Unlock()
	for _, cb := range []string{"Config.OnState", "Config.OnNodeCmd", "Config.OnDeviceCmd"} {
		if c := log.count("callback=" + cb); c != 1 {
			t.Errorf("%d errors logged for %s, want 1", c, cb)
		}
	}
	if c := log.count("goroutine "); c != 3 {
		t.Errorf("%d errors with a stack, want 3", c)
	}
	if err := n.PublishBirth(context.Background(), nil); err != nil {
		t.Errorf("PublishBirth after the callbacks: %v", err)
	}
}

// failingStore is a BdSeqStore that does not return normally.
type failingStore struct{ load bool }

func (s failingStore) Load() (uint64, error) {
	if s.load {
		panic("load")
	}
	return 0, nil
}

func (s failingStore) Store(uint64) error { panic("store") }

// A BdSeqStore that does not return normally fails Connect, without the
// node's lock left held.
func TestBdSeqStoreThatDoesNotReturnNormally(t *testing.T) {
	for _, load := range []bool{true, false} {
		log := &errorLog{}
		cfg := testConfig()
		cfg.Logger = log
		cfg.BdSeqStore = failingStore{load: load}
		n, err := New(newMockTransport(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		var cerr error
		within(t, "Connect", func() { cerr = n.Connect(context.Background()) })
		if cerr == nil {
			t.Errorf("Connect with a BdSeqStore that does not return normally (load %t) succeeded", load)
		}
		within(t, "State after Connect", func() {
			if s := n.State(); s != Disconnected {
				t.Errorf("state %s, want disconnected", s)
			}
		})
		if c := log.count("callback=BdSeqStore."); c != 1 {
			t.Errorf("%d errors logged for the BdSeqStore, want 1", c)
		}
	}
}

// A Logger that does not return normally does not stop the node.
func TestLoggerThatDoesNotReturnNormally(t *testing.T) {
	cfg := testConfig()
	cfg.Logger = failingLogger{}
	n, err := New(newMockTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.PublishBirth(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

type failingLogger struct{}

func (failingLogger) Debugf(string, ...any) { panic("logger") }
func (failingLogger) Infof(string, ...any)  { panic("logger") }
func (failingLogger) Warnf(string, ...any)  { panic("logger") }
func (failingLogger) Errorf(string, ...any) { panic("logger") }

// The node keeps copies of the birth metrics: a caller that changes its
// metrics after the birth does not change what a rebirth publishes.
func TestRebirthPublishesTheBirthMetrics(t *testing.T) {
	e := newEventTransport()
	n, err := New(e, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close(context.Background())
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	node, dev := intMetric(t, "x", 1), intMetric(t, "y", 1)
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{node}); err != nil {
		t.Fatal(err)
	}
	if err := n.PublishDeviceBirth(ctx, "d1", []*pb.Payload_Metric{dev}); err != nil {
		t.Fatal(err)
	}
	if node.Timestamp == nil || dev.Timestamp == nil {
		t.Error("births did not stamp the caller's metrics")
	}
	for _, m := range []*pb.Payload_Metric{node, dev} {
		if err := payload.SetValue(m, pb.DataType_Int32, int32(99)); err != nil {
			t.Fatal(err)
		}
	}
	nextPublished(t, e, topic.DBIRTH)

	b, err := payload.Encode(&pb.Payload{Metrics: []*pb.Payload_Metric{payload.RebirthMetric(true)}})
	if err != nil {
		t.Fatal(err)
	}
	e.deliver(topic.ForNCMD("testGroup", "testNode"), b)
	for _, c := range []struct {
		mt   topic.MessageType
		name string
	}{{topic.NBIRTH, "x"}, {topic.DBIRTH, "y"}} {
		p, err := payload.Decode(nextPublished(t, e, c.mt).payload)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range p.Metrics {
			if m.GetName() != c.name {
				continue
			}
			if v, err := payload.GetValue(m); err != nil || v != int32(1) {
				t.Errorf("rebirth %s: %s = %v (%v), want 1, the value of the birth", c.mt, c.name, v, err)
			}
		}
	}
}

// The IDs of the configuration are segments of the node's topics.
func TestNewRefusesInvalidIDs(t *testing.T) {
	for _, field := range []string{"GroupID", "EdgeNodeID", "PrimaryHostID"} {
		for _, tc := range []struct {
			id string
			ok bool
		}{
			{"n", true},
			{"plant-01", true},
			{"Gerät", true},
			{"a/b", false},
			{"+", false},
			{"#", false},
			{"n#", false},
			{"\xff", false},
			{"n\xc3", false},
		} {
			cfg := testConfig()
			switch field {
			case "GroupID":
				cfg.GroupID = tc.id
			case "EdgeNodeID":
				cfg.EdgeNodeID = tc.id
			case "PrimaryHostID":
				cfg.PrimaryHostID = tc.id
			}
			_, err := New(newMockTransport(), cfg)
			if tc.ok && err != nil {
				t.Errorf("%s %q: %v", field, tc.id, err)
			}
			if !tc.ok && !errors.Is(err, topic.ErrInvalidID) {
				t.Errorf("%s %q: %v, want topic.ErrInvalidID", field, tc.id, err)
			}
		}
	}
	for _, field := range []string{"GroupID", "EdgeNodeID"} {
		cfg := testConfig()
		if field == "GroupID" {
			cfg.GroupID = ""
		} else {
			cfg.EdgeNodeID = ""
		}
		if _, err := New(newMockTransport(), cfg); err == nil {
			t.Errorf("empty %s accepted", field)
		}
	}
	cfg := testConfig()
	cfg.PrimaryHostID = ""
	if _, err := New(newMockTransport(), cfg); err != nil {
		t.Errorf("no PrimaryHostID: %v", err)
	}
}
