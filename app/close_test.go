// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/otfabric/go-sparkplugb/mqtt"
	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

// Close contract 3: a command called once Close was called, while Close is
// still disconnecting or after it returned, returns an error that matches
// ErrClosed, and ErrNotConnected as before. Connect returns ErrClosed,
// Disconnect nil, and nothing is published.
func TestApplicationCommandsAfterClose(t *testing.T) {
	for _, when := range []string{"during Close", "after Close"} {
		t.Run(when, func(t *testing.T) {
			tr := &gatedTransport{mockTransport: newMockTransport()}
			if when == "during Close" {
				tr.disconnect = newGate()
			}
			a, err := New(tr, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if err := a.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			closed := make(chan error, 1)
			if when == "during Close" {
				go func() { closed <- a.Close(ctx) }()
				wait(t, "Close reaching the transport", tr.disconnect.arrived)
			} else if err := a.Close(ctx); err != nil {
				t.Fatal(err)
			}

			published := len(tr.getPublished())
			ms := []*pb.Payload_Metric{payload.RebirthMetric(true)}
			for _, op := range []struct {
				name string
				call func() error
			}{
				{"SendNodeCommand", func() error { return a.SendNodeCommand(ctx, "g1", "n1", ms) }},
				{"SendDeviceCommand", func() error { return a.SendDeviceCommand(ctx, "g1", "n1", "d1", ms) }},
				{"RequestRebirth", func() error { return a.RequestRebirth(ctx, "g1", "n1") }},
			} {
				if err := op.call(); !errors.Is(err, ErrClosed) || !errors.Is(err, ErrNotConnected) {
					t.Errorf("%s %s: %v, want ErrClosed and ErrNotConnected", op.name, when, err)
				}
			}
			if err := a.Connect(ctx); !errors.Is(err, ErrClosed) {
				t.Errorf("Connect %s: %v, want ErrClosed", when, err)
			}
			if when == "during Close" {
				close(tr.disconnect.open)
				if err := wait(t, "Close", closed); err != nil {
					t.Errorf("Close: %v", err)
				}
			}
			if err := a.Disconnect(ctx); err != nil {
				t.Errorf("Disconnect after Close: %v, want nil", err)
			}
			if p := tr.getPublished()[published:]; len(p) != 0 {
				t.Errorf("published %d messages after Close was called", len(p))
			}
		})
	}
}

// heldRequest is the mock transport with every NCMD publish held until its
// context ends: a rebirth request that waits for the broker.
type heldRequest struct{ *mockTransport }

func (h heldRequest) Publish(ctx context.Context, t string, qos byte, retained bool, p []byte) error {
	if strings.Contains(t, "/NCMD/") {
		<-ctx.Done()
		return ctx.Err()
	}
	return h.mockTransport.Publish(ctx, t, qos, retained, p)
}

// Close contract 4: Close ends the rebirth requests on their way and returns
// once their goroutines have ended. Before, Close returned while they went
// on until their deadline of 5 s.
func TestApplicationCloseEndsRebirthRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := heldRequest{newMockTransport()}
		a, err := New(tr, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Connect(context.Background()); err != nil {
			t.Fatal(err)
		}
		data := sizedData(t, 32)
		for i := range 3 {
			tr.deliver(ndataOf(fmt.Sprintf("n%d", i)), data)
		}
		synctest.Wait() // the requests wait for the broker
		a.mu.RLock()
		inFlight := a.rebirthsInFlight
		a.mu.RUnlock()
		if inFlight != 3 {
			t.Fatalf("%d rebirth requests on their way, want 3", inFlight)
		}

		start := time.Now()
		if err := a.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
		if d := time.Since(start); d != 0 {
			t.Errorf("Close took %v on the clock of the bubble, want it to end the requests at once", d)
		}
		a.mu.RLock()
		inFlight = a.rebirthsInFlight
		a.mu.RUnlock()
		if inFlight != 0 {
			t.Errorf("%d rebirth requests on their way after Close returned, want 0", inFlight)
		}
		if n := a.owned.Len(); n != 0 {
			t.Errorf("%d goroutines and timers of the application after Close returned, want 0", n)
		}
	})
}

// Close stops the timers the application armed once its connection was
// lost: the wait for an NBIRTH. Before, Close disconnected through
// Disconnect, which returns at once when the connection was lost, and the
// timer asked for a rebirth after Close.
func TestApplicationCloseStopsTimersArmedAfterConnectionLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newMockTransport()
		log := &warnLog{}
		cfg := testConfig()
		cfg.Logger = log
		a, err := New(tr, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Connect(context.Background()); err != nil {
			t.Fatal(err)
		}
		tr.deliver(nodeTopic(topic.NBIRTH, "n1"), sessMsg(0, 1000, payload.BdSeqMetric(0)))
		tr.opts.OnConnectionLost(mqtt.ErrConnectionLost)
		// Handled once the connection was lost: seq 200, behind the sequence,
		// arms the wait for an NBIRTH.
		tr.deliver(nodeTopic(topic.NDATA, "n1"), bytesData(t, 200, 2000, 8))

		start := time.Now()
		if err := a.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
		if d := time.Since(start); d != 0 {
			t.Errorf("Close took %v on the clock of the bubble, want it to stop the timers", d)
		}
		time.Sleep(time.Hour) // the clock of the bubble: past every timer
		synctest.Wait()
		log.mu.Lock()
		defer log.mu.Unlock()
		for _, e := range log.entries {
			if strings.Contains(e, "requesting rebirth") {
				t.Errorf("after Close: %s", e)
			}
		}
	})
}

// errHeldCommand is what heldCommand returns for a command it held, once
// the command's context has ended.
var errHeldCommand = errors.New("held command ended")

// heldCommand is the mock transport with every NCMD and DCMD publish held
// until its context ends or, with endWithConnection, until the transport is
// disconnected, which it reports as mqtt.ErrConnectionLost, as
// PahoTransport does. A connection that ended before the context did is
// what it reports.
type heldCommand struct {
	*mockTransport
	endWithConnection bool
	arrived           chan struct{}
	disconnected      chan struct{}
	once              sync.Once
}

func newHeldCommand(endWithConnection bool) *heldCommand {
	return &heldCommand{
		mockTransport:     newMockTransport(),
		endWithConnection: endWithConnection,
		arrived:           make(chan struct{}, 8),
		disconnected:      make(chan struct{}),
	}
}

func (h *heldCommand) Publish(ctx context.Context, t string, qos byte, retained bool, p []byte) error {
	if !strings.Contains(t, "/NCMD/") && !strings.Contains(t, "/DCMD/") {
		return h.mockTransport.Publish(ctx, t, qos, retained, p)
	}
	h.arrived <- struct{}{}
	var ended <-chan struct{} // nil, never ready, unless endWithConnection
	if h.endWithConnection {
		ended = h.disconnected
	}
	select {
	case <-ended:
	case <-ctx.Done():
	}
	select {
	case <-ended:
		return fmt.Errorf("sparkplugb: publish: %w", mqtt.ErrConnectionLost)
	default:
		return fmt.Errorf("%w: %w", errHeldCommand, ctx.Err())
	}
}

func (h *heldCommand) Disconnect(ctx context.Context) error {
	h.once.Do(func() { close(h.disconnected) })
	return h.mockTransport.Disconnect(ctx)
}

// Close contract 2: a command on its way to the transport when Close is
// called ends with Close, and its error matches ErrClosed as well as the
// transport's: mqtt.ErrConnectionLost from a transport that ends its
// publishes when it is disconnected, as PahoTransport does, and its own
// error from one that ends them with their context only. Before, the first
// returned mqtt.ErrConnectionLost alone, and the second did not return.
func TestApplicationCloseEndsCommandsInFlight(t *testing.T) {
	for _, tc := range []struct {
		name              string
		endWithConnection bool
		want              error
	}{
		{"transport ends it with the connection", true, mqtt.ErrConnectionLost},
		{"transport ends it with its context", false, errHeldCommand},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := newHeldCommand(tc.endWithConnection)
			a, err := New(tr, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if err := a.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			ms := []*pb.Payload_Metric{payload.RebirthMetric(true)}
			ops := []struct {
				name string
				call func() error
			}{
				{"SendNodeCommand", func() error { return a.SendNodeCommand(ctx, "g1", "n1", ms) }},
				{"SendDeviceCommand", func() error { return a.SendDeviceCommand(ctx, "g1", "n1", "d1", ms) }},
				{"RequestRebirth", func() error { return a.RequestRebirth(ctx, "g1", "n1") }},
			}
			results := make([]chan error, len(ops))
			for i, op := range ops {
				results[i] = make(chan error, 1)
				go func() { results[i] <- op.call() }()
			}
			for range ops {
				wait(t, "a command reaching the transport", tr.arrived)
			}

			closed := make(chan error, 1)
			go func() { closed <- a.Close(ctx) }()
			for i, op := range ops {
				err := wait(t, op.name+" in flight at Close", results[i])
				if !errors.Is(err, ErrClosed) || !errors.Is(err, tc.want) {
					t.Errorf("%s in flight at Close: %v, want ErrClosed and %v", op.name, err, tc.want)
				}
			}
			if err := wait(t, "Close", closed); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
	}
}

// L4: Close while a command waits for the transport. Close does not wait for
// it; the command returns what the transport returns, and the next one
// ErrClosed.
func TestApplicationCloseDuringCommand(t *testing.T) {
	tr := &slowPublish{mockTransport: newMockTransport(), release: make(chan struct{})}
	a, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := a.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	ms := []*pb.Payload_Metric{payload.RebirthMetric(true)}
	sent := make(chan error, 1)
	go func() { sent <- a.SendNodeCommand(ctx, "g1", "n1", ms) }()
	waitFor(t, func() bool { return tr.count() == 1 })

	closed := make(chan error, 1)
	go func() { closed <- a.Close(ctx) }()
	if err := wait(t, "Close while a command waits for the transport", closed); err != nil {
		t.Errorf("Close: %v", err)
	}
	close(tr.release)
	if err := wait(t, "the command", sent); err != nil {
		t.Errorf("command in progress at Close: %v, want what the transport returned", err)
	}
	if err := a.SendNodeCommand(ctx, "g1", "n1", ms); !errors.Is(err, ErrClosed) {
		t.Errorf("command after Close: %v, want ErrClosed", err)
	}
}

// L3: a callback that runs when Close is called. Close does not wait for it,
// and what it calls on the application afterwards returns ErrClosed.
func TestApplicationCallbackRunningAtClose(t *testing.T) {
	tr := newMockTransport()
	entered, release := make(chan struct{}), make(chan struct{})
	after := make(chan error, 1)
	var a *Application
	cfg := testConfig()
	cfg.OnNodeData = func(g, n string, _ []*pb.Payload_Metric) {
		close(entered)
		<-release
		after <- a.SendNodeCommand(context.Background(), g, n, []*pb.Payload_Metric{payload.RebirthMetric(true)})
	}
	a, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := a.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	tr.deliver(nodeTopic(topic.NBIRTH, "n1"), sessMsg(0, 1000, payload.BdSeqMetric(0)))
	data := bytesData(t, 1, 2000, 8)
	delivered := make(chan struct{})
	go func() { tr.deliver(nodeTopic(topic.NDATA, "n1"), data); close(delivered) }()
	wait(t, "OnNodeData", entered)

	closed := make(chan error, 1)
	go func() { closed <- a.Close(ctx) }()
	if err := wait(t, "Close while OnNodeData runs", closed); err != nil {
		t.Errorf("Close: %v", err)
	}
	close(release)
	if err := wait(t, "the command of OnNodeData", after); !errors.Is(err, ErrClosed) {
		t.Errorf("command from OnNodeData after Close: %v, want ErrClosed", err)
	}
	wait(t, "the end of the delivery", delivered)
}

// L11: while a callback is slow, the application is not held: its state, its nodes
// and its commands go on from other goroutines. The next message waits for
// the callback, on the transport's goroutine.
func TestApplicationSlowCallback(t *testing.T) {
	tr := newMockTransport()
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var handled []string
	cfg := testConfig()
	cfg.OnNodeData = func(_, _ string, ms []*pb.Payload_Metric) {
		name := ms[0].GetName()
		if name == "slow" {
			close(entered)
			<-release
		}
		mu.Lock()
		handled = append(handled, name)
		mu.Unlock()
	}
	a, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	defer a.Close(ctx)
	if err := a.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	tr.deliver(nodeTopic(topic.NBIRTH, "n1"), sessMsg(0, 1000, payload.BdSeqMetric(0)))
	slow, next := sessMsg(1, 2000, sessInt(t, "slow", 1)), sessMsg(2, 2001, sessInt(t, "next", 1))
	delivered := make(chan struct{})
	go func() {
		tr.deliver(nodeTopic(topic.NDATA, "n1"), slow)
		tr.deliver(nodeTopic(topic.NDATA, "n1"), next)
		close(delivered)
	}()
	wait(t, "the slow OnNodeData", entered)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if s := a.State(); s != Connected {
			t.Errorf("state %s, want connected", s)
		}
		if ns := a.GetNode("g1", "n1"); ns == nil || !ns.Online {
			t.Error("GetNode: n1 not online")
		}
		if n := len(a.Nodes()); n != 1 {
			t.Errorf("Nodes: %d, want 1", n)
		}
		if err := a.SendNodeCommand(ctx, "g1", "n1", []*pb.Payload_Metric{payload.RebirthMetric(true)}); err != nil {
			t.Errorf("SendNodeCommand: %v", err)
		}
	}()
	wait(t, "the application while OnNodeData is slow", done)
	mu.Lock()
	if len(handled) != 0 {
		t.Errorf("handled %v while the slow callback runs, want nothing", handled)
	}
	mu.Unlock()
	close(release)
	wait(t, "the messages after the slow one", delivered)
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(handled, []string{"slow", "next"}) {
		t.Errorf("handled %v, want [slow next]", handled)
	}
}

// heldDropMetrics records the OnHeldDrop calls. Each calls the application, which
// would wait for ever were the application's lock held.
type heldDropMetrics struct {
	NopMetrics
	a     *Application
	mu    sync.Mutex
	drops []string
}

func (m *heldDropMetrics) OnHeldDrop(groupID, edgeNodeID, messageType string) {
	_ = m.a.State()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.drops = append(m.drops, groupID+"/"+edgeNodeID+" "+messageType)
}

func (m *heldDropMetrics) get() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.drops)
}

// Metrics that implement sparkplugb.HeldDropMetrics are told of each held
// message dropped to keep the held messages within their bounds, with its
// edge node and type: held for an edge node without an NBIRTH, and taken
// back by an NBIRTH from the session it ends.
func TestApplicationHeldDropMetrics(t *testing.T) {
	run := func(t *testing.T, f func(t *testing.T, a *Application, tr *mockTransport)) *heldDropMetrics {
		m := &heldDropMetrics{}
		synctest.Test(t, func(t *testing.T) {
			tr := newMockTransport()
			cfg := testConfig()
			cfg.Metrics = m
			a, err := New(tr, cfg)
			if err != nil {
				t.Fatal(err)
			}
			m.a = a
			if err := a.Connect(context.Background()); err != nil {
				t.Fatal(err)
			}
			f(t, a, tr)
			a.mu.RLock()
			dropped := a.heldDropped
			a.mu.RUnlock()
			if n := len(m.get()); uint64(n) != dropped {
				t.Errorf("OnHeldDrop called %d times, the application dropped %d held messages", n, dropped)
			}
			synctest.Wait()
			_ = a.Close(context.Background())
		})
		return m
	}
	t.Run("edge nodes without an NBIRTH", func(t *testing.T) {
		var still []string
		m := run(t, func(t *testing.T, a *Application, tr *mockTransport) {
			data := sizedData(t, 32)
			for i := range maxHeldTotal + 3 {
				tr.deliver(ndataOf(fmt.Sprintf("n%d", i)), data)
			}
			_, _, still = held(t, a)
		})
		drops := m.get()
		if len(drops) != 3 {
			t.Fatalf("OnHeldDrop calls %v, want 3", drops)
		}
		for _, d := range drops {
			node, ok := strings.CutSuffix(d, " NDATA")
			if !ok || !strings.HasPrefix(node, "g1/n") || slices.Contains(still, node) {
				t.Errorf("OnHeldDrop(%s), want the NDATA of an edge node of g1 no longer held", d)
			}
		}
	})
	t.Run("taken back by an NBIRTH", func(t *testing.T) {
		nodes := maxHeldTotal/maxApplied + 12
		m := run(t, func(t *testing.T, a *Application, tr *mockTransport) {
			for n := range nodes {
				node := fmt.Sprintf("n%d", n)
				tr.deliver(nodeTopic(topic.NBIRTH, node), sessMsg(0, 1000, payload.BdSeqMetric(0)))
				for seq := uint64(1); seq <= maxApplied; seq++ {
					tr.deliver(nodeTopic(topic.NDATA, node), bytesData(t, seq, 5000, 8))
				}
				tr.deliver(nodeTopic(topic.NBIRTH, node), sessMsg(200, 2000, payload.BdSeqMetric(1)))
			}
		})
		drops := m.get()
		if want := nodes*maxApplied - maxHeldTotal; len(drops) != want {
			t.Errorf("OnHeldDrop called %d times, want %d", len(drops), want)
		}
		for _, d := range drops {
			if !strings.HasPrefix(d, "g1/n") || !strings.HasSuffix(d, " NDATA") {
				t.Errorf("OnHeldDrop(%s), want an NDATA of an edge node of g1", d)
			}
		}
	})
}
