// SPDX-License-Identifier: MIT

package edge

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/mqtt"
	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

// connTransport is the mock transport that records its connects and
// disconnects, and the most connections it had up at once.
type connTransport struct {
	*mockTransport
	emu     sync.Mutex
	events  []string
	live    int
	maxLive int
}

func newConnTransport() *connTransport { return &connTransport{mockTransport: newMockTransport()} }

func (c *connTransport) Connect(ctx context.Context, opts mqtt.ConnectOpts) error {
	if err := c.mockTransport.Connect(ctx, opts); err != nil {
		return err
	}
	c.emu.Lock()
	defer c.emu.Unlock()
	c.events = append(c.events, "connect "+opts.BrokerURL)
	c.live++
	c.maxLive = max(c.maxLive, c.live)
	return nil
}

func (c *connTransport) Disconnect(ctx context.Context) error {
	c.emu.Lock()
	c.events = append(c.events, "disconnect")
	if c.live > 0 {
		c.live--
	}
	c.emu.Unlock()
	return c.mockTransport.Disconnect(ctx)
}

func (c *connTransport) record() (events []string, live, maxLive int) {
	c.emu.Lock()
	defer c.emu.Unlock()
	return append([]string(nil), c.events...), c.live, c.maxLive
}

// eventually fails the test when cond does not hold within the bound of wait.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within 10s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func rebirthDone(n *Node) bool {
	n.rebirthMu.Lock()
	defer n.rebirthMu.Unlock()
	return !n.rebirthPending
}

func walkDone(n *Node) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return !n.walking
}

func ncmd(t *testing.T, metrics ...*pb.Payload_Metric) []byte {
	t.Helper()
	b, err := payload.Encode(&pb.Payload{Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func publishedTypes(tr *mockTransport) []string {
	var out []string
	for _, p := range tr.getPublished() {
		tp, err := topic.Parse(p.Topic)
		if err != nil {
			continue
		}
		s := tp.Type.String()
		if tp.DeviceID != "" {
			s += " " + tp.DeviceID
		}
		out = append(out, s)
	}
	return out
}

// A rebirth repeats the births the program made on this connection, and
// none before the program's NBIRTH: neither on its own, nor of the devices
// of an earlier connection, whether it ended by Disconnect or was lost.
func TestRebirthNeedsTheBirthOfTheConnection(t *testing.T) {
	for _, end := range []string{"disconnect", "lost"} {
		t.Run(end, func(t *testing.T) {
			tr := newMockTransport()
			n, err := New(tr, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close(context.Background())
			ctx := context.Background()
			if err := n.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			if err := n.Rebirth(ctx); !errors.Is(err, ErrNoBirth) {
				t.Errorf("Rebirth before the birth: %v, want ErrNoBirth", err)
			}
			tr.deliver(topic.ForNCMD("testGroup", "testNode"), ncmd(t, payload.RebirthMetric(true)))
			eventually(t, "the requested rebirth", func() bool { return rebirthDone(n) })
			if got := publishedTypes(tr); len(got) != 0 {
				t.Errorf("published %v before the program's birth, want nothing", got)
			}
			if err := n.PublishNodeData(ctx, []*pb.Payload_Metric{intMetric(t, "x", 2)}); !errors.Is(err, ErrNoBirth) {
				t.Errorf("PublishNodeData after the rebirth request: %v, want ErrNoBirth", err)
			}

			if err := n.PublishBirth(ctx, []*pb.Payload_Metric{intMetric(t, "x", 1)}); err != nil {
				t.Fatal(err)
			}
			if err := n.PublishDeviceBirth(ctx, "d1", []*pb.Payload_Metric{intMetric(t, "y", 1)}); err != nil {
				t.Fatal(err)
			}
			if end == "disconnect" {
				if err := n.Disconnect(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				tr.mu.Lock()
				lost := tr.connectOpts.OnConnectionLost
				tr.mu.Unlock()
				lost(mqtt.ErrConnectionLost)
			}
			if err := n.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			tr.mu.Lock()
			tr.published = nil
			tr.mu.Unlock()

			if err := n.Rebirth(ctx); !errors.Is(err, ErrNoBirth) {
				t.Errorf("Rebirth before the birth of the new connection: %v, want ErrNoBirth", err)
			}
			if err := n.PublishBirth(ctx, []*pb.Payload_Metric{intMetric(t, "x", 3)}); err != nil {
				t.Fatal(err)
			}
			if err := n.Rebirth(ctx); err != nil {
				t.Fatal(err)
			}
			if got, want := strings.Join(publishedTypes(tr), ","), "NBIRTH,NBIRTH"; got != want {
				t.Errorf("published %s, want %s: no DBIRTH of the earlier connection", got, want)
			}
			if err := n.PublishDeviceBirth(ctx, "d1", []*pb.Payload_Metric{intMetric(t, "y", 1)}); err != nil {
				t.Fatal(err)
			}
			if err := n.Rebirth(ctx); err != nil {
				t.Fatal(err)
			}
			if got, want := strings.Join(publishedTypes(tr)[3:], ","), "NBIRTH,DBIRTH d1"; got != want {
				t.Errorf("rebirth published %s, want %s", got, want)
			}
		})
	}
}

// A node has one connection at a time: Connect while connected is refused
// and leaves the connection as it is.
func TestConnectWhenConnected(t *testing.T) {
	tr := newConnTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close(context.Background())
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	bdSeq := n.BdSeq()
	if err := n.Connect(ctx); !errors.Is(err, ErrAlreadyConnected) {
		t.Errorf("second Connect: %v, want ErrAlreadyConnected", err)
	}
	if got := n.BdSeq(); got != bdSeq {
		t.Errorf("bdSeq %d after the refused Connect, want %d", got, bdSeq)
	}
	if s := n.State(); s != Online {
		t.Errorf("state %s, want online", s)
	}
	if events, _, _ := tr.record(); len(events) != 1 {
		t.Errorf("transport events %v, want one connect", events)
	}
	if err := n.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.Connect(ctx); err != nil {
		t.Errorf("Connect after Disconnect: %v", err)
	}
}

// walkNode is a node with two servers and a primary host whose OnState
// calls onDisconnected when the state becomes Disconnected.
func walkNode(t *testing.T, tr mqtt.Transport, onDisconnected func(n *Node), logger *hookLogger) *Node {
	t.Helper()
	cfg := testConfig()
	cfg.BrokerURL = ""
	cfg.ServerURLs = []string{"tcp://a:1883", "tcp://b:1883"}
	cfg.PrimaryHostID = "host"
	if logger != nil {
		cfg.Logger = logger
	}
	var n *Node
	cfg.OnState = func(_, new State) {
		if new == Disconnected && onDisconnected != nil {
			onDisconnected(n)
		}
	}
	n, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// A StateHandler that connects again on Disconnected during a walk ends the
// walk: the node keeps the handler's connection and opens no other.
func TestWalkEndsWhenTheHandlerReconnects(t *testing.T) {
	tr := newConnTransport()
	once := sync.Once{}
	var connectErr error
	n := walkNode(t, tr, func(n *Node) {
		once.Do(func() { connectErr = n.Connect(context.Background()) })
	}, nil)
	defer n.Close(context.Background())
	if err := n.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	tr.deliver(topic.ForState("host"), hostState(true, 1))
	tr.deliver(topic.ForState("host"), hostState(false, 2))
	eventually(t, "the end of the walk", func() bool { return walkDone(n) })
	if connectErr != nil {
		t.Errorf("the handler's Connect: %v", connectErr)
	}
	events, live, maxLive := tr.record()
	if got, want := strings.Join(events, ","), "connect tcp://a:1883,disconnect,connect tcp://a:1883"; got != want {
		t.Errorf("transport events %s, want %s", got, want)
	}
	if live != 1 || maxLive != 1 {
		t.Errorf("connections up %d, at most %d; want 1 and 1", live, maxLive)
	}
	if got := n.BdSeq(); got != 2 {
		t.Errorf("bdSeq %d, want 2: one per connection", got)
	}
}

// hookLogger runs hook, once, on the first entry that starts with prefix.
type hookLogger struct {
	prefix string
	hook   func()
	once   sync.Once
}

func (l *hookLogger) Debugf(string, ...any) {}
func (l *hookLogger) Warnf(string, ...any)  {}
func (l *hookLogger) Errorf(string, ...any) {}
func (l *hookLogger) Infof(format string, _ ...any) {
	if strings.HasPrefix(format, l.prefix) {
		l.once.Do(l.hook)
	}
}

// A Connect that the walk finds refused, because the node was connected
// meanwhile, ends the walk: it is not a failed attempt to retry.
func TestWalkEndsWhenConnectedMeanwhile(t *testing.T) {
	tr := newConnTransport()
	var n *Node
	connects := make(chan error, 8)
	log := &hookLogger{prefix: "walking to server", hook: func() {
		if err := n.Connect(context.Background()); err != nil {
			t.Errorf("Connect during the walk: %v", err)
		}
	}}
	n = walkNode(t, tr, nil, log)
	n.cfg.Metrics = connectMetrics(connects)
	defer n.Close(context.Background())
	if err := n.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	tr.deliver(topic.ForState("host"), hostState(true, 1))
	tr.deliver(topic.ForState("host"), hostState(false, 2))
	eventually(t, "the end of the walk", func() bool { return walkDone(n) })
	select {
	case err := <-connects:
		t.Errorf("the walk reported a connect (%v); want none", err)
	default:
	}
	if _, live, maxLive := tr.record(); live != 1 || maxLive != 1 {
		t.Errorf("connections up %d, at most %d; want 1 and 1", live, maxLive)
	}
}

// The last online STATE of the primary host counts: one stamped ahead does
// not keep a later offline STATE from starting a walk.
func TestWalkAfterOnlineStateStampedAhead(t *testing.T) {
	tr := newConnTransport()
	n := walkNode(t, tr, nil, nil)
	defer n.Close(context.Background())
	if err := n.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	tr.deliver(topic.ForState("host"), hostState(true, 1<<50))
	tr.deliver(topic.ForState("host"), hostState(true, 5))
	tr.deliver(topic.ForState("host"), hostState(false, 6))
	eventually(t, "the walk", func() bool {
		events, _, _ := tr.record()
		return len(events) == 3
	})
	eventually(t, "the end of the walk", func() bool { return walkDone(n) })
	events, _, _ := tr.record()
	if got, want := strings.Join(events, ","), "connect tcp://a:1883,disconnect,connect tcp://b:1883"; got != want {
		t.Errorf("transport events %s, want %s", got, want)
	}

	// An offline STATE older than the last online one is ignored.
	tr.deliver(topic.ForState("host"), hostState(true, 10))
	tr.deliver(topic.ForState("host"), hostState(false, 9))
	if s := n.State(); s != Online {
		t.Errorf("state %s after an older offline STATE, want online", s)
	}
}

// An NCMD with "Node Control/Rebirth" and other metrics starts a rebirth and
// gives the others to OnNodeCmd; the rebirth metric is recognised by the
// alias the NBIRTH gave it, and not by an alias of a device.
func TestNodeCommandWithRebirth(t *testing.T) {
	e := newEventTransport()
	cmds := make(chan []*pb.Payload_Metric, 8)
	cfg := testConfig()
	cfg.OnNodeCmd = func(_ string, m []*pb.Payload_Metric) { cmds <- m }
	n, err := New(e, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close(context.Background())
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	rebirth := payload.RebirthMetric(false)
	rebirth.Alias = new(uint64(7))
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{rebirth}); err != nil {
		t.Fatal(err)
	}
	devMetric := payload.RebirthMetric(false)
	devMetric.Alias = new(uint64(8))
	if err := n.PublishDeviceBirth(ctx, "d1", []*pb.Payload_Metric{devMetric}); err != nil {
		t.Fatal(err)
	}
	nextPublished(t, e, topic.DBIRTH)
	ncmdTopic := topic.ForNCMD("testGroup", "testNode")

	e.deliver(ncmdTopic, ncmd(t, payload.RebirthMetric(true), intMetric(t, "x", 1)))
	nextPublished(t, e, topic.NBIRTH)
	if got := wait(t, "OnNodeCmd", cmds); len(got) != 1 || got[0].GetName() != "x" {
		t.Errorf("OnNodeCmd got %v, want x alone", got)
	}
	eventually(t, "the rebirth", func() bool { return rebirthDone(n) })

	byAlias := &pb.Payload_Metric{Alias: new(uint64(7)), Datatype: new(uint32(pb.DataType_Boolean)),
		Value: &pb.Payload_Metric_BooleanValue{BooleanValue: true}}
	e.deliver(ncmdTopic, ncmd(t, byAlias))
	nextPublished(t, e, topic.NBIRTH)
	eventually(t, "the rebirth by alias", func() bool { return rebirthDone(n) })

	ofDevice := &pb.Payload_Metric{Alias: new(uint64(8)), Datatype: new(uint32(pb.DataType_Boolean)),
		Value: &pb.Payload_Metric_BooleanValue{BooleanValue: true}}
	e.deliver(ncmdTopic, ncmd(t, ofDevice))
	if got := wait(t, "OnNodeCmd", cmds); len(got) != 1 || got[0].GetAlias() != 8 {
		t.Errorf("OnNodeCmd got %v, want the metric of alias 8", got)
	}
	unnamed := &pb.Payload_Metric{Datatype: new(uint32(pb.DataType_Boolean)),
		Value: &pb.Payload_Metric_BooleanValue{BooleanValue: true}}
	e.deliver(ncmdTopic, ncmd(t, unnamed))
	if got := wait(t, "OnNodeCmd", cmds); len(got) != 1 {
		t.Errorf("OnNodeCmd got %v, want the metric without name or alias", got)
	}
	select {
	case cmd := <-cmds:
		t.Errorf("OnNodeCmd called again with %v", cmd)
	default:
	}
}

// A message that is not handed to the transport gives its seq back, and the
// next one takes it.
func TestUnpublishedMessageKeepsTheSequence(t *testing.T) {
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close(context.Background())
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.PublishBirth(ctx, nil); err != nil {
		t.Fatal(err)
	}
	done, cancel := context.WithCancel(ctx)
	cancel()
	m := []*pb.Payload_Metric{intMetric(t, "x", 1)}
	if err := n.PublishNodeData(done, m); !errors.Is(err, context.Canceled) {
		t.Errorf("PublishNodeData with an ended context: %v, want context.Canceled", err)
	}
	if err := n.PublishDeviceData(done, "d1", m); !errors.Is(err, context.Canceled) {
		t.Errorf("PublishDeviceData with an ended context: %v, want context.Canceled", err)
	}
	if err := n.PublishBirth(done, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("PublishBirth with an ended context: %v, want context.Canceled", err)
	}
	if got := n.Seq(); got != 0 {
		t.Errorf("seq %d after unpublished messages, want 0", got)
	}
	if err := n.PublishNodeData(ctx, m); err != nil {
		t.Fatal(err)
	}
	pubs := tr.getPublished()
	p, err := payload.Decode(pubs[len(pubs)-1].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if p.GetSeq() != 1 {
		t.Errorf("NDATA seq %d, want 1", p.GetSeq())
	}
}

// goexitStore ends the goroutine that calls it.
type goexitStore struct{}

func (goexitStore) Load() (uint64, error) { runtime.Goexit(); return 0, nil }
func (goexitStore) Store(uint64) error    { return nil }

// A BdSeqStore that ends its goroutine leaves the node Disconnected, with
// its lock free.
func TestBdSeqStoreThatEndsItsGoroutine(t *testing.T) {
	cfg := testConfig()
	cfg.BdSeqStore = goexitStore{}
	n, err := New(newMockTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = n.Connect(context.Background())
	}()
	wait(t, "the end of Connect", done)
	within(t, "State", func() {
		if s := n.State(); s != Disconnected {
			t.Errorf("state %s, want disconnected", s)
		}
	})
	within(t, "Close", func() { _ = n.Close(context.Background()) })
}
