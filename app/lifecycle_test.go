// SPDX-License-Identifier: MIT

package app

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

// gatedTransport is the mock transport with a gate on some of its methods.
type gatedTransport struct {
	*mockTransport
	connect, subscribe, disconnect *gate
	disconnects                    atomic.Int32
}

func (g *gatedTransport) Connect(ctx context.Context, opts mqtt.ConnectOpts) error {
	g.connect.pass()
	return g.mockTransport.Connect(ctx, opts)
}

func (g *gatedTransport) Subscribe(ctx context.Context, t string, qos byte, h mqtt.MessageHandler) error {
	g.subscribe.pass()
	return g.mockTransport.Subscribe(ctx, t, qos, h)
}

func (g *gatedTransport) Disconnect(ctx context.Context) error {
	g.disconnects.Add(1)
	g.disconnect.pass()
	return g.mockTransport.Disconnect(ctx)
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

// L2: Close from two goroutines at once.
func TestApplicationCloseFromTwoGoroutines(t *testing.T) {
	tr := &gatedTransport{mockTransport: newMockTransport(), disconnect: newGate()}
	a, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := a.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	first := make(chan error, 1)
	go func() { first <- a.Close(ctx) }()
	wait(t, "first Close reaching the transport", tr.disconnect.arrived)
	if err := a.Close(ctx); err != nil {
		t.Errorf("second Close: %v", err)
	}
	close(tr.disconnect.open)
	if err := wait(t, "first Close", first); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if s := a.State(); s != Closed {
		t.Errorf("state %s, want closed", s)
	}
	if n := tr.disconnects.Load(); n != 1 {
		t.Errorf("transport disconnected %d times, want 1", n)
	}
	if err := a.Connect(ctx); !errors.Is(err, ErrClosed) {
		t.Errorf("Connect after Close: %v, want ErrClosed", err)
	}
}

// L5: Close while Connect waits for the transport.
func TestApplicationCloseDuringConnect(t *testing.T) {
	for _, step := range []string{"connect", "subscribe"} {
		t.Run(step, func(t *testing.T) {
			tr := &gatedTransport{mockTransport: newMockTransport()}
			g := newGate()
			if step == "connect" {
				tr.connect = g
			} else {
				tr.subscribe = g
			}
			a, err := New(tr, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			connected := make(chan error, 1)
			go func() { connected <- a.Connect(ctx) }()
			wait(t, "Connect reaching the "+step, g.arrived)

			if err := a.Close(ctx); err != nil {
				t.Errorf("Close: %v", err)
			}
			close(g.open)
			if err := wait(t, "Connect", connected); !errors.Is(err, ErrClosed) {
				t.Errorf("Connect during Close: %v, want ErrClosed", err)
			}
			if s := a.State(); s != Closed {
				t.Errorf("state %s after Close, want closed", s)
			}
			if tr.isConnected() {
				t.Error("transport still connected after Close")
			}
			if err := a.Close(ctx); err != nil {
				t.Errorf("Close again: %v", err)
			}
		})
	}
}

// L9, L10: a StateHandler that calls the application.
func TestApplicationStateHandlerCallsApplication(t *testing.T) {
	type tr struct{ old, new State }
	for _, tc := range []struct {
		name  string
		react func(a *Application, old, new State)
		want  []tr
		final State
	}{
		{
			name:  "State",
			react: func(a *Application, _, new State) { _ = a.State() },
			want:  []tr{{Disconnected, Connecting}, {Connecting, Connected}, {Connected, Disconnected}},
			final: Disconnected,
		},
		{
			name: "Close when disconnected",
			react: func(a *Application, _, new State) {
				if new == Disconnected {
					_ = a.Close(context.Background())
				}
			},
			want:  []tr{{Disconnected, Connecting}, {Connecting, Connected}, {Connected, Disconnected}, {Disconnected, Closed}},
			final: Closed,
		},
		{
			name: "Connect when disconnected",
			react: func(a *Application, old, new State) {
				if old == Connected && new == Disconnected {
					_ = a.Connect(context.Background())
				}
			},
			want:  []tr{{Disconnected, Connecting}, {Connecting, Connected}, {Connected, Disconnected}, {Disconnected, Connecting}, {Connecting, Connected}},
			final: Connected,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMockTransport()
			var a *Application
			var mu sync.Mutex
			var got []tr
			cfg := testConfig()
			cfg.OnState = func(old, new State) {
				mu.Lock()
				got = append(got, tr{old, new})
				mu.Unlock()
				tc.react(a, old, new)
			}
			a, err := New(m, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close(context.Background())

			done := make(chan error, 1)
			go func() { done <- a.Connect(context.Background()) }()
			if err := wait(t, "Connect with a StateHandler that calls the application", done); err != nil {
				t.Fatal(err)
			}
			lost := make(chan struct{})
			go func() { m.opts.OnConnectionLost(mqtt.ErrConnectionLost); close(lost) }()
			wait(t, "the connection-lost callback with a StateHandler that calls the application", lost)

			if s := a.State(); s != tc.final {
				t.Errorf("state %s, want %s", s, tc.final)
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

// A StateHandler that calls Connect when the application is Disconnected is called
// once the application has ended the connection, so what it opens stays up: after
// the caller's Disconnect, and after a Connect that failed at a subscription.
func TestApplicationStateHandlerConnectsWhenDisconnected(t *testing.T) {
	for _, cause := range []string{"Disconnect", "Connect that fails"} {
		t.Run(cause, func(t *testing.T) {
			tr := &oneSubscribeFails{mockTransport: newMockTransport()}
			tr.failed.Store(cause == "Disconnect") // then no Subscribe fails
			var a *Application
			var again atomic.Bool
			cfg := testConfig()
			cfg.OnState = func(_, new State) {
				if new == Disconnected && again.CompareAndSwap(true, false) {
					if err := a.Connect(context.Background()); err != nil {
						t.Errorf("Connect from the StateHandler: %v", err)
					}
				}
			}
			a, err := New(tr, cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			defer a.Close(ctx)

			if cause == "Disconnect" {
				if err := a.Connect(ctx); err != nil {
					t.Fatal(err)
				}
				again.Store(true)
				if err := a.Disconnect(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				again.Store(true)
				if err := a.Connect(ctx); err == nil {
					t.Fatal("Connect with a Subscribe that fails succeeded")
				}
			}
			if s := a.State(); s != Connected {
				t.Errorf("state %s after the StateHandler connected, want %s", s, Connected)
			}
			if !tr.isConnected() {
				t.Error("the connection the StateHandler opened is not up")
			}
		})
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
	n := 0
	for _, e := range l.entries {
		if strings.Contains(e, sub) {
			n++
		}
	}
	return n
}

// Application code that does not return normally is logged once, with its
// stack, and the application goes on with the next message.
func TestApplicationCallbackThatDoesNotReturnNormally(t *testing.T) {
	tr := newMockTransport()
	log := &errorLog{}
	var mu sync.Mutex
	var data []string
	cfg := testConfig()
	cfg.Logger = log
	cfg.OnState = func(_, new State) {
		if new == Connected {
			panic("state handler")
		}
	}
	cfg.OnNodeData = func(_, nodeID string, _ []*pb.Payload_Metric) {
		if nodeID == "bad" {
			panic("node data handler")
		}
		mu.Lock()
		data = append(data, nodeID)
		mu.Unlock()
	}
	a, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	if err := a.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if s := a.State(); s != Connected {
		t.Fatalf("state %s, want connected", s)
	}

	for _, node := range []string{"bad", "good"} {
		tr.deliver(topic.Topic{GroupID: "g1", Type: topic.NBIRTH, EdgeNodeID: node}.String(), sessMsg(0, 1, payload.BdSeqMetric(0), sessInt(t, "x", 1)))
		tr.deliver(topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: node}.String(), sessMsg(1, 2, sessInt(t, "x", 2)))
	}
	mu.Lock()
	if fmt.Sprint(data) != "[good]" {
		t.Errorf("data delivered for %v, want [good]", data)
	}
	mu.Unlock()
	if ns := a.GetNode("g1", "good"); ns == nil || ns.LastSeq != 1 {
		t.Errorf("node good: %+v, want seq 1 applied", ns)
	}
	for _, cb := range []string{"Config.OnState", "Config.OnNodeData"} {
		if n := log.count("callback=" + cb); n != 1 {
			t.Errorf("%d errors logged for %s, want 1", n, cb)
		}
	}
	if n := log.count("goroutine "); n != 2 {
		t.Errorf("%d errors with a stack, want 2", n)
	}
}

// A Logger that does not return normally does not stop the application.
func TestApplicationLoggerThatDoesNotReturnNormally(t *testing.T) {
	cfg := testConfig()
	cfg.Logger = failingLogger{}
	a, err := New(newMockTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type failingLogger struct{}

func (failingLogger) Debugf(string, ...any) { panic("logger") }
func (failingLogger) Infof(string, ...any)  { panic("logger") }
func (failingLogger) Warnf(string, ...any)  { panic("logger") }
func (failingLogger) Errorf(string, ...any) { panic("logger") }

// The metrics a birth callback receives are its own: changing them does not
// change what the application keeps.
func TestApplicationBirthCallbackMetricsAreItsOwn(t *testing.T) {
	tr := newMockTransport()
	change := func(ms []*pb.Payload_Metric) {
		for _, m := range ms {
			if m.GetName() == "x" {
				if err := payload.SetValue(m, pb.DataType_Int32, int32(99)); err != nil {
					t.Error(err)
				}
			}
		}
	}
	cfg := testConfig()
	cfg.OnNodeBirth = func(_, _ string, ms []*pb.Payload_Metric) { change(ms) }
	cfg.OnDeviceBirth = func(_, _, _ string, ms []*pb.Payload_Metric) { change(ms) }
	a, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	if err := a.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 1, payload.BdSeqMetric(0), sessInt(t, "x", 1)))
	tr.deliver(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 2, sessInt(t, "x", 1)))

	ns := a.GetNode("g1", "n1")
	if ns == nil || ns.Devices["d1"] == nil {
		t.Fatalf("node %+v, want n1 with device d1", ns)
	}
	if v := sessValue(t, ns.Metrics, "x"); v != int32(1) {
		t.Errorf("node metric x = %v after the birth callback changed its metrics, want 1", v)
	}
	if v := sessValue(t, ns.Devices["d1"].Metrics, "x"); v != int32(1) {
		t.Errorf("device metric x = %v after the birth callback changed its metrics, want 1", v)
	}
}
