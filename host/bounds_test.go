// SPDX-License-Identifier: MIT

package host

import (
	"context"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
	"google.golang.org/protobuf/proto"
)

// sizedData is an NDATA payload of seq 5 that encodes to exactly size bytes.
func sizedData(t *testing.T, size int) []byte {
	t.Helper()
	n := max(size-16, 0)
	for range 8 {
		m, err := payload.NewMetric("b", pb.DataType_Bytes, make([]byte, n))
		if err != nil {
			t.Fatal(err)
		}
		b, err := payload.Encode(payload.NewPayload(5, m))
		if err != nil {
			t.Fatal(err)
		}
		if len(b) == size {
			return b
		}
		n += size - len(b)
	}
	t.Fatalf("no payload of %d bytes", size)
	return nil
}

func ndataOf(node string) string {
	return topic.Topic{GroupID: "g1", Type: topic.NDATA, EdgeNodeID: node}.String()
}

// held returns the messages and bytes held over every edge node, counted
// from the map, and checks them against the totals the host keeps.
func held(t *testing.T, a *App) (msgs, bytes int, keys []string) {
	t.Helper()
	a.mu.RLock()
	defer a.mu.RUnlock()
	for key, hm := range a.held {
		if len(hm) == 0 {
			t.Errorf("edge node %s kept in the held messages without any", key)
		}
		keys = append(keys, key)
		msgs += len(hm)
		bytes += heldSize(hm)
	}
	if msgs != a.heldCount || bytes != a.heldBytes {
		t.Errorf("held %d messages of %d bytes, the host counts %d of %d", msgs, bytes, a.heldCount, a.heldBytes)
	}
	slices.Sort(keys)
	return msgs, bytes, keys
}

// The messages held for edge nodes without an NBIRTH are bounded over all
// of them: by count, and by payload bytes. The oldest are dropped and
// counted.
func TestHostHeldMessagesBoundedOverAllNodes(t *testing.T) {
	for _, tc := range []struct {
		nodes, size   int
		held, dropped int
	}{
		{nodes: 1, size: 32, held: 1},
		{nodes: maxHeldTotal - 1, size: 32, held: maxHeldTotal - 1},
		{nodes: maxHeldTotal, size: 32, held: maxHeldTotal},
		{nodes: maxHeldTotal + 1, size: 32, held: maxHeldTotal, dropped: 1},
		{nodes: maxHeldBytes / (64 << 10), size: 64 << 10, held: maxHeldBytes / (64 << 10)},
		{nodes: maxHeldBytes/(64<<10) + 1, size: 64 << 10, held: maxHeldBytes / (64 << 10), dropped: 1},
		{nodes: 1, size: maxHeldBytes - 1, held: 1},
		{nodes: 1, size: maxHeldBytes, held: 1},
		{nodes: 1, size: maxHeldBytes + 1, held: 0},
		{nodes: 2, size: maxHeldBytes/2 + 1, held: 1, dropped: 1},
	} {
		t.Run(fmt.Sprintf("%d nodes of %d bytes", tc.nodes, tc.size), func(t *testing.T) {
			data := sizedData(t, tc.size)
			synctest.Test(t, func(t *testing.T) {
				tr := newMockTransport()
				a, err := New(tr, testConfig())
				if err != nil {
					t.Fatal(err)
				}
				if err := a.Connect(context.Background()); err != nil {
					t.Fatal(err)
				}
				for i := range tc.nodes {
					tr.deliverToAll(ndataOf(fmt.Sprintf("n%d", i)), data)
				}
				msgs, bytes, keys := held(t, a)
				if msgs != tc.held || len(keys) != tc.held {
					t.Errorf("held %d messages for %d edge nodes, want %d", msgs, len(keys), tc.held)
				}
				if bytes > maxHeldBytes {
					t.Errorf("held %d bytes, want at most %d", bytes, maxHeldBytes)
				}
				a.mu.RLock()
				if a.heldDropped != uint64(tc.dropped) {
					t.Errorf("dropped %d held messages, want %d", a.heldDropped, tc.dropped)
				}
				a.mu.RUnlock()
				synctest.Wait() // the rebirth requests
				_ = a.Close(context.Background())
			})
		})
	}
}

// slowPublish is the mock transport with every NCMD publish held until
// release is closed.
type slowPublish struct {
	*mockTransport
	release chan struct{}
	mu      sync.Mutex
	started int
}

func (s *slowPublish) Publish(ctx context.Context, t string, qos byte, retained bool, p []byte) error {
	if strings.Contains(t, "/NCMD/") {
		s.mu.Lock()
		s.started++
		s.mu.Unlock()
		<-s.release
	}
	return s.mockTransport.Publish(ctx, t, qos, retained, p)
}

func (s *slowPublish) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

// The rebirth requests on their way are bounded over all edge nodes, so a
// stream of messages from unknown nodes does not start a goroutine each.
func TestHostRebirthRequestsBoundedOverAllNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := &slowPublish{mockTransport: newMockTransport(), release: make(chan struct{})}
		a, err := New(tr, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Connect(context.Background()); err != nil {
			t.Fatal(err)
		}
		data := sizedData(t, 32)
		for i := range 100 {
			tr.deliverToAll(ndataOf(fmt.Sprintf("n%d", i)), data)
		}
		synctest.Wait()
		if n := tr.count(); n != maxRebirthRequests {
			t.Errorf("%d rebirth requests on their way, want %d", n, maxRebirthRequests)
		}
		a.mu.RLock()
		inFlight := a.rebirthsInFlight
		a.mu.RUnlock()
		if inFlight != maxRebirthRequests {
			t.Errorf("host counts %d requests on their way, want %d", inFlight, maxRebirthRequests)
		}

		close(tr.release)
		synctest.Wait()
		a.mu.RLock()
		inFlight = a.rebirthsInFlight
		a.mu.RUnlock()
		if inFlight != 0 {
			t.Errorf("host counts %d requests on their way after they were sent, want 0", inFlight)
		}
		// Room again: the next unknown node is asked.
		tr.deliverToAll(ndataOf("next"), data)
		synctest.Wait()
		if n := tr.count(); n != maxRebirthRequests+1 {
			t.Errorf("%d rebirth requests sent, want %d", n, maxRebirthRequests+1)
		}
		_ = a.Close(context.Background())
	})
}

// Edge nodes that send nothing more leave nothing behind: their held
// messages, rebirth requests and deaths are forgotten once their time has
// passed, when the host handles the next message of any node.
func TestHostForgetsSilentNodes(t *testing.T) {
	for _, wait := range []time.Duration{heldMaxAge - sweepInterval, heldMaxAge + sweepInterval} {
		t.Run(wait.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				tr := newMockTransport()
				a, err := New(tr, testConfig())
				if err != nil {
					t.Fatal(err)
				}
				if err := a.Connect(context.Background()); err != nil {
					t.Fatal(err)
				}
				data := sizedData(t, 32)
				for i := range 10 {
					tr.deliverToAll(ndataOf(fmt.Sprintf("s%d", i)), data)
				}
				tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.NDEATH, EdgeNodeID: "dead"}.String(), sessDeath(3))
				synctest.Wait()

				time.Sleep(wait) // the clock of the bubble
				tr.deliverToAll(ndataOf("late"), data)
				synctest.Wait()

				_, _, keys := held(t, a)
				a.mu.RLock()
				asked := slices.Sorted(maps.Keys(a.rebirthAsked))
				missed := len(a.rebirthMissed)
				_, dead := a.deaths[nodeKey("g1", "dead")]
				a.mu.RUnlock()
				if wait < heldMaxAge {
					if len(keys) != 11 {
						t.Errorf("held messages of %d edge nodes before heldMaxAge, want 11", len(keys))
					}
				} else {
					if want := []string{nodeKey("g1", "late")}; !slices.Equal(keys, want) {
						t.Errorf("held messages of %v after heldMaxAge, want %v", keys, want)
					}
				}
				if want := []string{nodeKey("g1", "late")}; !slices.Equal(asked, want) {
					t.Errorf("rebirth requests remembered for %v, want %v", asked, want)
				}
				if missed != 0 {
					t.Errorf("%d throttled requests remembered, want 0", missed)
				}
				if dead {
					t.Error("death of an edge node remembered after lateBirthWait")
				}
				_ = a.Close(context.Background())
			})
		})
	}
}

// deepBirth is an NBIRTH with a template definition nested depth deep, with
// members named by 100 bytes.
func deepBirth(t *testing.T, depth int) []byte {
	t.Helper()
	name := strings.Repeat("n", 100)
	dt := uint32(pb.DataType_Template)
	tpl := &pb.Payload_Template{IsDefinition: proto.Bool(true)}
	for range depth - 1 {
		member := &pb.Payload_Metric{Name: proto.String(name), Datatype: &dt, Value: &pb.Payload_Metric_TemplateValue{TemplateValue: tpl}}
		tpl = &pb.Payload_Template{IsDefinition: proto.Bool(true), Metrics: []*pb.Payload_Metric{member}}
	}
	def := &pb.Payload_Metric{Name: proto.String(name), Datatype: &dt, Value: &pb.Payload_Metric_TemplateValue{TemplateValue: tpl}}
	return sessMsg(0, 1, payload.BdSeqMetric(0), def)
}

// warnLog records the entries logged at warning level.
type warnLog struct {
	testLogger
	mu      sync.Mutex
	entries []string
}

func (l *warnLog) Warnf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, fmt.Sprintf(format, args...))
}

// An NBIRTH with templates nested deeper than the checks allow is reported
// and applied without its definitions, and handling it allocates in
// proportion to its size.
func TestHostNBIRTHWithDeepTemplates(t *testing.T) {
	log := &warnLog{}
	cfg := testConfig()
	cfg.Logger = log
	tr := newMockTransport()
	a, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	if err := a.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	data := deepBirth(t, 1000)
	nbirth := sessTopic(topic.NBIRTH, "")

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	tr.deliverToAll(nbirth, data)
	runtime.ReadMemStats(&after)
	if n, budget := after.TotalAlloc-before.TotalAlloc, uint64(64*len(data)); n > budget {
		t.Errorf("NBIRTH of %d bytes: %d bytes allocated, want at most %d", len(data), n, budget)
	}
	if ns := a.GetNode("g1", "n1"); ns == nil || !ns.Online {
		t.Errorf("node %+v, want n1 online", ns)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	var warned bool
	for _, e := range log.entries {
		warned = warned || strings.Contains(e, "NBIRTH from g1/n1") && strings.Contains(e, "nests templates deeper than 32")
	}
	if !warned {
		t.Errorf("warnings %q, want one for the nesting", log.entries)
	}
}
