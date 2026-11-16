// SPDX-License-Identifier: MIT

package edge

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

// publishCall is a call of Metrics.OnPublish.
type publishCall struct {
	messageType string
	d           time.Duration
	err         error
}

// publishMetrics records the calls of OnPublish, and the locks of the node
// held during one.
type publishMetrics struct {
	NopMetrics
	node  **Node
	mu    sync.Mutex
	calls []publishCall
	held  []string
}

func (m *publishMetrics) OnPublish(messageType string, d time.Duration, err error) {
	n := *m.node
	var held []string
	for _, l := range []struct {
		name string
		lock interface {
			TryLock() bool
			Unlock()
		}
	}{{"seqMu", &n.seqMu}, {"pubMu", &n.pubMu}, {"mu", &n.mu}} {
		if !l.lock.TryLock() {
			held = append(held, messageType+": "+l.name)
			continue
		}
		l.lock.Unlock()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, publishCall{messageType, d, err})
	m.held = append(m.held, held...)
}

// publishTime is how long slowPublish takes to publish, on the clock of a
// synctest bubble.
const publishTime = 3 * time.Millisecond

// slowPublish is the mock transport whose Publish takes publishTime.
type slowPublish struct{ *mockTransport }

func (s slowPublish) Publish(ctx context.Context, t string, qos byte, retained bool, p []byte) error {
	time.Sleep(publishTime)
	return s.mockTransport.Publish(ctx, t, qos, retained, p)
}

// Metrics.OnPublish is called for each message the node hands to the
// transport, the NDEATH of Disconnect and the births of Rebirth included,
// with its message type, how long the transport's Publish took and the
// error the publish returned, with none of the node's locks held. A message
// the node refuses before the transport is not reported.
func TestMetricsOnPublish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := slowPublish{newMockTransport()}
		var n *Node
		m := &publishMetrics{node: &n}
		cfg := testConfig()
		cfg.Metrics = m
		var err error
		n, err = New(tr, cfg)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		defer n.Close(ctx)
		if err := n.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		node := []*pb.Payload_Metric{intMetric(t, "x", 1)}
		device := []*pb.Payload_Metric{intMetric(t, "y", 1)}
		for _, op := range []func() error{
			func() error { return n.PublishBirth(ctx, node) },
			func() error { return n.PublishDeviceBirth(ctx, "d1", device) },
			func() error { return n.PublishNodeData(ctx, node) },
			func() error { return n.PublishDeviceData(ctx, "d1", device) },
			func() error { return n.PublishDeviceDeath(ctx, "d1") },
			func() error { return n.PublishDeviceBirth(ctx, "d1", device) },
			func() error { return n.Rebirth(ctx) },
		} {
			if err := op(); err != nil {
				t.Fatal(err)
			}
		}

		// Refused before the transport: not reported.
		ended, cancel := context.WithCancel(ctx)
		cancel()
		if err := n.PublishNodeData(ended, node); !errors.Is(err, context.Canceled) {
			t.Errorf("PublishNodeData with an ended context: %v, want context.Canceled", err)
		}
		if err := n.PublishDeviceData(ctx, "a/b", device); !errors.Is(err, topic.ErrInvalidID) {
			t.Errorf("PublishDeviceData to an invalid device ID: %v, want topic.ErrInvalidID", err)
		}

		// The transport's error is reported with the message.
		tr.mu.Lock()
		tr.failTopic = "/NDATA/"
		tr.mu.Unlock()
		failed := n.PublishNodeData(ctx, node)
		if failed == nil {
			t.Fatal("PublishNodeData: nil, want the transport's error")
		}
		tr.mu.Lock()
		tr.failTopic = ""
		tr.mu.Unlock()

		if err := n.Disconnect(ctx); err != nil {
			t.Fatal(err)
		}
		if err := n.PublishNodeData(ctx, node); !errors.Is(err, ErrNoBirth) {
			t.Errorf("PublishNodeData after Disconnect: %v, want ErrNoBirth", err)
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		var types []string
		for _, c := range m.calls {
			types = append(types, c.messageType)
		}
		want := []string{"NBIRTH", "DBIRTH", "NDATA", "DDATA", "DDEATH", "DBIRTH", "NBIRTH", "DBIRTH", "NDATA", "NDEATH"}
		if !slices.Equal(types, want) {
			t.Fatalf("OnPublish called for %v, want %v", types, want)
		}
		for i, c := range m.calls {
			if c.d != publishTime {
				t.Errorf("call %d (%s): %v, want the %v the transport took", i, c.messageType, c.d, publishTime)
			}
			wantErr := error(nil)
			if i == 8 {
				wantErr = failed
			}
			if c.err != wantErr {
				t.Errorf("call %d (%s): error %v, want %v", i, c.messageType, c.err, wantErr)
			}
		}
		if len(m.held) != 0 {
			t.Errorf("locks of the node held during OnPublish: %v", m.held)
		}
	})
}

// closingOnPublish calls close from OnPublish for the NBIRTH of a rebirth.
type closingOnPublish struct {
	NopMetrics
	mu     sync.Mutex
	births int
	close  func()
}

func (c *closingOnPublish) OnPublish(messageType string, _ time.Duration, _ error) {
	if messageType != "NBIRTH" {
		return
	}
	c.mu.Lock()
	c.births++
	rebirth := c.births == 2
	c.mu.Unlock()
	if rebirth {
		c.close()
	}
}

// L10 with Metrics.OnPublish: Close called from OnPublish on the goroutine
// of a rebirth an NCMD asked for returns, without waiting for that
// goroutine, which ends after OnPublish returns.
func TestCloseFromOnPublish(t *testing.T) {
	tr := newMockTransport()
	var n *Node
	closed := make(chan error, 1)
	cfg := testConfig()
	cfg.Metrics = &closingOnPublish{close: func() { closed <- n.Close(context.Background()) }}
	var err error
	n, err = New(tr, cfg)
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
	tr.deliver(topic.ForNCMD("testGroup", "testNode"), ncmd(t, payload.RebirthMetric(true)))
	if err := wait(t, "Close called from OnPublish during a rebirth", closed); err != nil {
		t.Errorf("Close: %v", err)
	}
	eventually(t, "the end of the rebirth", func() bool { return n.owned.Len() == 0 })
	if s := n.State(); s != Closed {
		t.Errorf("state %s, want closed", s)
	}
}
