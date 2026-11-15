// SPDX-License-Identifier: MIT

package host

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

// nodeTopic is the topic of message type mt of edge node node in group g1.
func nodeTopic(mt topic.MessageType, node string) string {
	return topic.Topic{GroupID: "g1", Type: mt, EdgeNodeID: node}.String()
}

// bytesData is an NDATA payload of seq and timestamp ts with a Bytes metric
// of n bytes.
func bytesData(t *testing.T, seq, ts uint64, n int) []byte {
	t.Helper()
	m, err := payload.NewMetric("b", pb.DataType_Bytes, make([]byte, n))
	if err != nil {
		t.Fatal(err)
	}
	return sessMsg(seq, ts, m)
}

// heldOf returns the number of messages held for the edge node node.
func heldOf(a *App, node string) int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.held[nodeKey("g1", node)])
}

// largestHeld returns the size of the largest held message.
func largestHeld(a *App) int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	n := 0
	for _, msgs := range a.held {
		for i := range msgs {
			n = max(n, len(msgs[i].data))
		}
	}
	return n
}

// heldTest runs f with a connected host in a synctest bubble, and closes the
// host once the rebirth requests it made are sent.
func heldTest(t *testing.T, f func(t *testing.T, a *App, tr *mockTransport)) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		tr := newMockTransport()
		a, err := New(tr, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Connect(context.Background()); err != nil {
			t.Fatal(err)
		}
		f(t, a, tr)
		synctest.Wait()
		_ = a.Close(context.Background())
	})
}

// The messages an NBIRTH takes back from the session it ends are held within
// the bounds of every held message: per edge node, by count and by bytes
// over all edge nodes, and nothing above maxHeldBytes.
func TestHostTakenBackMessagesWithinHeldBounds(t *testing.T) {
	t.Run("per edge node", func(t *testing.T) {
		heldTest(t, func(t *testing.T, a *App, tr *mockTransport) {
			tr.deliverToAll(nodeTopic(topic.NBIRTH, "n1"), sessMsg(0, 1000, payload.BdSeqMetric(0)))
			for range maxHeld - 6 {
				tr.deliverToAll(nodeTopic(topic.NDATA, "n1"), bytesData(t, 128, 5000, 8))
			}
			for seq := uint64(1); seq <= maxApplied; seq++ {
				tr.deliverToAll(nodeTopic(topic.NDATA, "n1"), bytesData(t, seq, 5000, 8))
			}
			tr.deliverToAll(nodeTopic(topic.NBIRTH, "n1"), sessMsg(100, 2000, payload.BdSeqMetric(1)))
			if n := heldOf(a, "n1"); n != maxHeld {
				t.Errorf("held %d messages for n1, want %d", n, maxHeld)
			}
			held(t, a)
		})
	})
	t.Run("count over all edge nodes", func(t *testing.T) {
		heldTest(t, func(t *testing.T, a *App, tr *mockTransport) {
			nodes := maxHeldTotal/maxApplied + 12
			for n := range nodes {
				node := fmt.Sprintf("n%d", n)
				tr.deliverToAll(nodeTopic(topic.NBIRTH, node), sessMsg(0, 1000, payload.BdSeqMetric(0)))
				for seq := uint64(1); seq <= maxApplied; seq++ {
					tr.deliverToAll(nodeTopic(topic.NDATA, node), bytesData(t, seq, 5000, 8))
				}
				tr.deliverToAll(nodeTopic(topic.NBIRTH, node), sessMsg(200, 2000, payload.BdSeqMetric(1)))
			}
			msgs, _, _ := held(t, a)
			if msgs != maxHeldTotal {
				t.Errorf("held %d messages, want %d", msgs, maxHeldTotal)
			}
			a.mu.RLock()
			dropped := a.heldDropped
			a.mu.RUnlock()
			if want := uint64(nodes*maxApplied - maxHeldTotal); dropped != want {
				t.Errorf("dropped %d held messages, want %d", dropped, want)
			}
			if n := heldOf(a, fmt.Sprintf("n%d", nodes-1)); n != maxApplied {
				t.Errorf("held %d messages for the last edge node, want %d", n, maxApplied)
			}
		})
	})
	t.Run("bytes over all edge nodes", func(t *testing.T) {
		heldTest(t, func(t *testing.T, a *App, tr *mockTransport) {
			const mib = 1 << 20
			for i := range 6 { // edge nodes without an NBIRTH: held
				tr.deliverToAll(nodeTopic(topic.NDATA, fmt.Sprintf("o%d", i)), bytesData(t, 5, 5000, mib))
			}
			time.Sleep(time.Second) // the clock of the bubble: they are the oldest
			tr.deliverToAll(nodeTopic(topic.NBIRTH, "n1"), sessMsg(0, 1000, payload.BdSeqMetric(0)))
			for seq := uint64(1); seq <= 4; seq++ {
				tr.deliverToAll(nodeTopic(topic.NDATA, "n1"), bytesData(t, seq, 5000, mib))
			}
			tr.deliverToAll(nodeTopic(topic.NBIRTH, "n1"), sessMsg(100, 2000, payload.BdSeqMetric(1)))
			_, bytes, _ := held(t, a)
			if bytes > maxHeldBytes {
				t.Errorf("held %d bytes, want at most %d", bytes, maxHeldBytes)
			}
			if n := heldOf(a, "n1"); n != 4 {
				t.Errorf("held %d messages for n1, want 4", n)
			}
			a.mu.RLock()
			dropped := a.heldDropped
			a.mu.RUnlock()
			if dropped != 3 {
				t.Errorf("dropped %d held messages, want 3", dropped)
			}
		})
	})
	t.Run("bytes of the messages taken back", func(t *testing.T) {
		heldTest(t, func(t *testing.T, a *App, tr *mockTransport) {
			const mib = 1 << 20
			tr.deliverToAll(nodeTopic(topic.NBIRTH, "n1"), sessMsg(0, 1000, payload.BdSeqMetric(0)))
			for seq := uint64(1); seq <= 12; seq++ {
				tr.deliverToAll(nodeTopic(topic.NDATA, "n1"), bytesData(t, seq, 5000, mib))
			}
			tr.deliverToAll(nodeTopic(topic.NBIRTH, "n1"), sessMsg(100, 2000, payload.BdSeqMetric(1)))
			_, bytes, _ := held(t, a)
			if bytes > maxHeldBytes {
				t.Errorf("held %d bytes, want at most %d", bytes, maxHeldBytes)
			}
			if n := heldOf(a, "n1"); n != 7 { // 7 messages of a little over 1 MiB fit
				t.Errorf("held %d messages for n1, want 7", n)
			}
		})
	})
	t.Run("message above maxHeldBytes", func(t *testing.T) {
		heldTest(t, func(t *testing.T, a *App, tr *mockTransport) {
			tr.deliverToAll(nodeTopic(topic.NBIRTH, "n1"), sessMsg(0, 1000, payload.BdSeqMetric(0)))
			tr.deliverToAll(nodeTopic(topic.NDATA, "n1"), bytesData(t, 1, 2001, maxHeldBytes))
			tr.deliverToAll(nodeTopic(topic.NDATA, "n1"), bytesData(t, 2, 2002, 8))
			a.mu.RLock()
			remembered := len(a.sessions[nodeKey("g1", "n1")].applied)
			a.mu.RUnlock()
			if remembered != 1 {
				t.Errorf("session remembers %d applied messages, want 1: not the one above maxHeldBytes", remembered)
			}
			tr.deliverToAll(nodeTopic(topic.NBIRTH, "n1"), sessMsg(100, 1500, payload.BdSeqMetric(1)))
			if n := largestHeld(a); n > maxHeldBytes {
				t.Errorf("held a message of %d bytes, want none above %d", n, maxHeldBytes)
			}
			if n := heldOf(a, "n1"); n != 1 {
				t.Errorf("held %d messages for n1, want 1", n)
			}
		})
	})
}

// An NDEATH ends the session: the next NBIRTH takes nothing back from it.
func TestHostNDEATHForgetsAppliedMessages(t *testing.T) {
	heldTest(t, func(t *testing.T, a *App, tr *mockTransport) {
		tr.deliverToAll(nodeTopic(topic.NBIRTH, "n1"), sessMsg(0, 1000, payload.BdSeqMetric(5)))
		tr.deliverToAll(nodeTopic(topic.NDATA, "n1"), bytesData(t, 1, 5000, 8))
		tr.deliverToAll(nodeTopic(topic.NDEATH, "n1"), sessDeath(5))
		a.mu.RLock()
		remembered := len(a.sessions[nodeKey("g1", "n1")].applied)
		a.mu.RUnlock()
		if remembered != 0 {
			t.Errorf("session remembers %d applied messages after its NDEATH, want 0", remembered)
		}
		time.Sleep(time.Second) // past lateBirthWait
		tr.deliverToAll(nodeTopic(topic.NBIRTH, "n1"), sessMsg(0, 2000, payload.BdSeqMetric(6)))
		if n := heldOf(a, "n1"); n != 0 {
			t.Errorf("held %d messages for n1 after the NBIRTH, want 0", n)
		}
	})
}

// A held message waits heldMaxAge and at most sweepInterval more, also when
// no message follows to sweep it.
func TestHostDropsHeldMessagesWithoutTraffic(t *testing.T) {
	heldTest(t, func(t *testing.T, a *App, tr *mockTransport) {
		tr.deliverToAll(nodeTopic(topic.NDATA, "a"), bytesData(t, 5, 5000, 8))
		time.Sleep(10 * time.Second)
		tr.deliverToAll(nodeTopic(topic.NDATA, "b"), bytesData(t, 5, 5000, 8))

		time.Sleep(heldMaxAge - 10*time.Second)
		synctest.Wait()
		if heldOf(a, "a") != 1 || heldOf(a, "b") != 1 {
			t.Fatalf("held %d and %d messages at heldMaxAge, want 1 and 1", heldOf(a, "a"), heldOf(a, "b"))
		}
		time.Sleep(sweepInterval)
		synctest.Wait()
		if heldOf(a, "a") != 0 || heldOf(a, "b") != 1 {
			t.Errorf("held %d and %d messages after heldMaxAge of the first, want 0 and 1", heldOf(a, "a"), heldOf(a, "b"))
		}
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if msgs, _, _ := held(t, a); msgs != 0 {
			t.Errorf("held %d messages after heldMaxAge of the last, want 0", msgs)
		}
		a.mu.RLock()
		pending := a.sweepTimer != nil
		a.mu.RUnlock()
		if pending {
			t.Error("sweep still scheduled with nothing held")
		}
	})
}

// Room for one large message drops many small ones at once, oldest first.
func TestHostLargeMessageDropsManyHeld(t *testing.T) {
	heldTest(t, func(t *testing.T, a *App, tr *mockTransport) {
		small := sizedData(t, 32)
		for i := range maxHeldTotal {
			tr.deliverToAll(ndataOf(fmt.Sprintf("s%d", i)), small)
		}
		time.Sleep(time.Millisecond)
		tr.deliverToAll(ndataOf("large"), sizedData(t, maxHeldBytes))
		msgs, bytes, keys := held(t, a)
		if msgs != 1 || bytes != maxHeldBytes || keys[0] != nodeKey("g1", "large") {
			t.Errorf("held %d messages of %d bytes for %v, want the large one", msgs, bytes, keys)
		}
		a.mu.RLock()
		dropped := a.heldDropped
		a.mu.RUnlock()
		if dropped != maxHeldTotal {
			t.Errorf("dropped %d held messages, want %d", dropped, maxHeldTotal)
		}
	})
}

// A DBIRTH applied after the NDEATH of its edge node (MQTT orders messages
// per topic only, so a will can overtake it) leaves the device stale, as the
// NDEATH left the others.
func TestHostDBIRTHAfterNDEATHIsStale(t *testing.T) {
	a, tr := sessApp(t)
	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 1000, payload.BdSeqMetric(5)))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d0"), sessMsg(1, 1001, sessInt(t, "x", 1)))
	tr.deliverToAll(sessTopic(topic.NDEATH, ""), sessDeath(5))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(2, 1002, sessInt(t, "y", 1)))

	ns := a.GetNode("g1", "n1")
	if ns.Online || !ns.Stale {
		t.Errorf("node Online %v Stale %v, want offline and stale", ns.Online, ns.Stale)
	}
	for _, id := range []string{"d0", "d1"} {
		ds := ns.Devices[id]
		if ds == nil {
			t.Fatalf("device %s missing", id)
		}
		if ds.Online || !ds.Stale {
			t.Errorf("device %s Online %v Stale %v, want offline and stale", id, ds.Online, ds.Stale)
		}
	}
}

// recMetrics records the Metrics calls of messages and deaths.
type recMetrics struct {
	NopMetrics
	mu       sync.Mutex
	messages map[string]int
	deaths   int
}

func (m *recMetrics) OnMessage(mt string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.messages == nil {
		m.messages = make(map[string]int)
	}
	m.messages[mt]++
}

func (m *recMetrics) OnNodeDeath(string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deaths++
}

// Metrics.OnMessage counts every message received on a Sparkplug topic, and
// an NDEATH of an edge node never born reports no death.
func TestHostMetricsMessagesAndDeaths(t *testing.T) {
	m := &recMetrics{}
	var deaths int
	cfg := testConfig()
	cfg.Metrics = m
	cfg.OnNodeDeath = func(string, string) { deaths++ }
	tr := newMockTransport()
	a, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	if err := a.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	tr.deliverToAll(nodeTopic(topic.NDEATH, "never"), sessDeath(1))
	if deaths != 0 || m.deaths != 0 {
		t.Errorf("NDEATH of an edge node never born: %d OnNodeDeath and %d Metrics.OnNodeDeath calls, want none", deaths, m.deaths)
	}
	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 1000, payload.BdSeqMetric(1)))
	tr.deliverToAll(sessTopic(topic.NDATA, ""), sessMsg(1, 1001, sessInt(t, "x", 1)))
	tr.deliverToAll(sessTopic(topic.NDATA, ""), sessMsg(2, 1002, sessInt(t, "x", 2)))
	tr.deliverToAll(sessTopic(topic.NDEATH, ""), sessDeath(1))
	if deaths != 1 || m.deaths != 1 {
		t.Errorf("NDEATH of a born edge node: %d OnNodeDeath and %d Metrics.OnNodeDeath calls, want 1 and 1", deaths, m.deaths)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for mt, want := range map[string]int{"NBIRTH": 1, "NDATA": 2, "NDEATH": 2} {
		if m.messages[mt] != want {
			t.Errorf("OnMessage(%s) called %d times, want %d", mt, m.messages[mt], want)
		}
	}
}

// The IDs of a received topic are logged quoted: control characters in them
// do not start a log line of their own.
func TestHostLogsTopicIDsQuoted(t *testing.T) {
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
	tr.deliverToAll(nodeTopic(topic.NBIRTH, "n\nWARN x"), []byte{0xff})
	tr.deliverToAll(topic.Topic{GroupID: "g1", Type: topic.DBIRTH, EdgeNodeID: "n1", DeviceID: "d\r1"}.String(), []byte{0xff})
	log.mu.Lock()
	defer log.mu.Unlock()
	if len(log.entries) != 2 {
		t.Fatalf("warnings %q, want 2", log.entries)
	}
	for i, want := range []string{`decode NBIRTH from "g1/n\nWARN x": `, `decode DBIRTH from "g1/n1/d\r1": `} {
		if e := log.entries[i]; !strings.HasPrefix(e, want) || strings.ContainsAny(e, "\r\n") {
			t.Errorf("warning %q, want it to start with %q and hold no line break", e, want)
		}
	}
}
