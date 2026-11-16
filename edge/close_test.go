// SPDX-License-Identifier: MIT

package edge

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

// operations calls every operation of the node that needs it open, and
// says which error each returns while the node is open and cannot do it.
func operations(t *testing.T) []struct {
	name string
	call func(*Node) error
	open error
} {
	ctx := context.Background()
	ms := func() []*pb.Payload_Metric { return []*pb.Payload_Metric{intMetric(t, "x", 1)} }
	return []struct {
		name string
		call func(*Node) error
		open error
	}{
		{"PublishBirth", func(n *Node) error { return n.PublishBirth(ctx, ms()) }, ErrNotConnected},
		{"PublishDeviceBirth", func(n *Node) error { return n.PublishDeviceBirth(ctx, "d1", ms()) }, ErrNoBirth},
		{"PublishNodeData", func(n *Node) error { return n.PublishNodeData(ctx, ms()) }, ErrNoBirth},
		{"PublishDeviceData", func(n *Node) error { return n.PublishDeviceData(ctx, "d1", ms()) }, ErrNoBirth},
		{"PublishDeviceDeath", func(n *Node) error { return n.PublishDeviceDeath(ctx, "d1") }, ErrNoBirth},
		{"Rebirth", func(n *Node) error { return n.Rebirth(ctx) }, ErrNotConnected},
	}
}

// Close contract 3: an operation called once Close was called, while Close
// is still disconnecting or after it returned, returns an error that matches
// ErrClosed, and the error it returns while the node is open. Connect
// returns ErrClosed, Disconnect nil.
func TestOperationsAfterClose(t *testing.T) {
	for _, when := range []string{"during Close", "after Close"} {
		t.Run(when, func(t *testing.T) {
			tr := newEventTransport()
			if when == "during Close" {
				tr.disconnect = newGate()
			}
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
			if err := n.PublishDeviceBirth(ctx, "d1", []*pb.Payload_Metric{intMetric(t, "y", 1)}); err != nil {
				t.Fatal(err)
			}
			closed := make(chan error, 1)
			if when == "during Close" {
				go func() { closed <- n.Close(ctx) }()
				wait(t, "Close reaching the transport", tr.disconnect.arrived)
			} else if err := n.Close(ctx); err != nil {
				t.Fatal(err)
			}

			published := len(tr.getPublished())
			for _, op := range operations(t) {
				err := op.call(n)
				if !errors.Is(err, ErrClosed) || !errors.Is(err, op.open) {
					t.Errorf("%s %s: %v, want ErrClosed and %v", op.name, when, err, op.open)
				}
			}
			if err := n.Connect(ctx); !errors.Is(err, ErrClosed) {
				t.Errorf("Connect %s: %v, want ErrClosed", when, err)
			}
			if when == "during Close" {
				close(tr.disconnect.open)
				if err := wait(t, "Close", closed); err != nil {
					t.Errorf("Close: %v", err)
				}
			}
			if err := n.Disconnect(ctx); err != nil {
				t.Errorf("Disconnect after Close: %v, want nil", err)
			}
			if p := tr.getPublished()[published:]; len(p) != 0 {
				t.Errorf("published %d messages after Close was called", len(p))
			}
		})
	}
}

// rebirthHeld is the mock transport with every NBIRTH after the first held
// until its context ends: a rebirth that waits for the broker.
type rebirthHeld struct {
	*mockTransport
	mu     sync.Mutex
	births int
}

func (r *rebirthHeld) Publish(ctx context.Context, t string, qos byte, retained bool, p []byte) error {
	if strings.Contains(t, "/NBIRTH/") {
		r.mu.Lock()
		r.births++
		first := r.births == 1
		r.mu.Unlock()
		if !first {
			<-ctx.Done()
			return ctx.Err()
		}
	}
	return r.mockTransport.Publish(ctx, t, qos, retained, p)
}

// Close contract 4: Close ends the rebirth an NCMD asked for, which waits
// for the broker, and returns once its goroutine has ended. Before, Close
// waited for the rebirth's own deadline of 10 s, and returned before the
// goroutine ended.
func TestCloseEndsTheRebirth(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := &rebirthHeld{mockTransport: newMockTransport()}
		log := &errorLog{}
		cfg := testConfig()
		cfg.Logger = log
		n, err := New(tr, cfg)
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
		b, err := payload.Encode(&pb.Payload{Metrics: []*pb.Payload_Metric{payload.RebirthMetric(true)}})
		if err != nil {
			t.Fatal(err)
		}
		tr.deliver(topic.ForNCMD("testGroup", "testNode"), b)
		// The NBIRTH of the rebirth is stamped a millisecond after the
		// first one: let the clock of the bubble pass it.
		time.Sleep(time.Millisecond)
		synctest.Wait() // the rebirth waits for the broker
		tr.mu.Lock()
		births := tr.births
		tr.mu.Unlock()
		if rebirthDone(n) || births != 2 {
			t.Fatalf("rebirth done %v, %d NBIRTHs published, want the second one waiting", rebirthDone(n), births)
		}

		start := time.Now()
		if err := n.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
		if d := time.Since(start); d != 0 {
			t.Errorf("Close took %v on the clock of the bubble, want it to end the rebirth at once", d)
		}
		if !rebirthDone(n) {
			t.Error("the rebirth still runs after Close returned")
		}
		if c := log.count("rebirth"); c != 0 {
			t.Errorf("%d errors logged for the rebirth that Close ended, want none", c)
		}
	})
}

// L10 on a goroutine of the node: Close called from the application's code
// that the walk to another server runs returns, without waiting for the
// walk, which ends after that code returns.
func TestCloseFromTheWalk(t *testing.T) {
	for _, from := range []string{"OnState", "Metrics.OnConnect", "RetryPolicy"} {
		t.Run(from, func(t *testing.T) {
			e := newEventTransport()
			var n *Node
			closed := make(chan error, 1)
			closeOnce := sync.OnceFunc(func() { closed <- n.Close(context.Background()) })
			walking := make(chan struct{})
			cfg := testConfig()
			cfg.PrimaryHostID = "host"
			cfg.OnState = func(old, new State) {
				select {
				case <-walking:
				default:
					return
				}
				if from == "OnState" && old == Online && new == Disconnected {
					closeOnce()
				}
			}
			cfg.Metrics = closingMetrics{close: func() {
				if from == "Metrics.OnConnect" {
					closeOnce()
				}
			}}
			cfg.RetryPolicy = closingRetry{close: closeOnce}
			n, err := New(e, cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if err := n.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			subscribedTo(t, e, topic.ForState("host"))
			e.deliver(topic.ForState("host"), hostState(true, 1))
			if s := n.State(); s != Online {
				t.Fatalf("state %s, want online", s)
			}
			if from == "RetryPolicy" {
				e.mu.Lock()
				e.connectErr = errors.New("refused")
				e.mu.Unlock()
			}
			close(walking)
			e.deliver(topic.ForState("host"), hostState(false, 2))
			if err := wait(t, "Close called from "+from+" during the walk", closed); err != nil {
				t.Errorf("Close: %v", err)
			}
			eventually(t, "the end of the walk", func() bool { return n.owned.Len() == 0 })
			if s := n.State(); s != Closed {
				t.Errorf("state %s, want closed", s)
			}
		})
	}
}

// closingMetrics calls close from OnConnect.
type closingMetrics struct {
	NopMetrics
	close func()
}

func (c closingMetrics) OnConnect(string, time.Duration, error) { c.close() }

// closingRetry calls close from ShouldRetry and retries at once.
type closingRetry struct{ close func() }

func (c closingRetry) ShouldRetry(int, error) (bool, time.Duration) {
	c.close()
	return true, 0
}

// L3: a command callback that runs when Close is called. Close does not
// wait for it, and what it calls on the node afterwards returns ErrClosed.
func TestCommandHandlerRunningAtClose(t *testing.T) {
	tr := newMockTransport()
	entered, release := make(chan struct{}), make(chan struct{})
	after := make(chan error, 1)
	var n *Node
	cfg := testConfig()
	cfg.OnNodeCmd = func(string, []*pb.Payload_Metric) {
		close(entered)
		<-release
		after <- n.PublishNodeData(context.Background(), []*pb.Payload_Metric{intMetric(t, "x", 2)})
	}
	n, err := New(tr, cfg)
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
	b, err := payload.Encode(&pb.Payload{Metrics: []*pb.Payload_Metric{intMetric(t, "cmd", 1)}})
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(chan struct{})
	go func() { tr.deliver(topic.ForNCMD("testGroup", "testNode"), b); close(delivered) }()
	wait(t, "the command callback", entered)

	within(t, "Close while a command callback runs", func() {
		if err := n.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	close(release)
	if err := wait(t, "the publish of the command callback", after); !errors.Is(err, ErrClosed) {
		t.Errorf("PublishNodeData from the callback after Close: %v, want ErrClosed", err)
	}
	wait(t, "the end of the delivery", delivered)
}

// L11: while a command callback is slow, the node is not held: its state,
// data and rebirth go on from other goroutines. The next message waits for
// the callback, on the transport's goroutine.
func TestSlowCommandHandler(t *testing.T) {
	tr := newMockTransport()
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var handled []string
	cfg := testConfig()
	cfg.OnNodeCmd = func(_ string, ms []*pb.Payload_Metric) {
		if ms[0].GetName() == "slow" {
			close(entered)
			<-release
		}
		mu.Lock()
		handled = append(handled, ms[0].GetName())
		mu.Unlock()
	}
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	defer n.Close(ctx)
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{intMetric(t, "x", 1)}); err != nil {
		t.Fatal(err)
	}
	cmd := func(name string) []byte {
		b, err := payload.Encode(&pb.Payload{Metrics: []*pb.Payload_Metric{intMetric(t, name, 1)}})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	ncmd := topic.ForNCMD("testGroup", "testNode")
	slow, next := cmd("slow"), cmd("next")
	delivered := make(chan struct{})
	go func() {
		tr.deliver(ncmd, slow)
		tr.deliver(ncmd, next)
		close(delivered)
	}()
	wait(t, "the slow command callback", entered)

	within(t, "the node while a command callback is slow", func() {
		if s := n.State(); s != Online {
			t.Errorf("state %s, want online", s)
		}
		if err := n.PublishNodeData(ctx, []*pb.Payload_Metric{intMetric(t, "x", 2)}); err != nil {
			t.Errorf("PublishNodeData: %v", err)
		}
		if err := n.Rebirth(ctx); err != nil {
			t.Errorf("Rebirth: %v", err)
		}
	})
	mu.Lock()
	if len(handled) != 0 {
		t.Errorf("handled %v while the slow callback runs, want nothing", handled)
	}
	mu.Unlock()
	close(release)
	wait(t, "the messages after the slow one", delivered)
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(handled, " ") != "slow next" {
		t.Errorf("handled %v, want [slow next]", handled)
	}
}
