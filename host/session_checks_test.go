// SPDX-License-Identifier: MIT

package host

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	sparkplugb "github.com/otfabric/go-sparkplugb"
	"github.com/otfabric/go-sparkplugb/payload"
	"github.com/otfabric/go-sparkplugb/topic"
)

// The opt-in SessionChecks (go-sparkplugb#24): each drops the message of the
// ending session that the collision tests show applied by default, and each
// drops a valid message of an edge node that breaks its assumption, at the
// cost of a rebirth. Births are never dropped.

// checkedApp is sessApp with the configuration changed by set.
func checkedApp(t *testing.T, set func(*Config)) (*App, *mockTransport) {
	t.Helper()
	tr := newMockTransport()
	cfg := testConfig()
	set(&cfg)
	a, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	a.sessionWait = 20 * time.Millisecond
	if err := a.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	return a, tr
}

func withChecks(c sparkplugb.SessionChecks) func(*Config) {
	return func(cfg *Config) { cfg.SessionChecks = c }
}

// noRebirth fails when a rebirth is requested within a few sessionWaits. It
// is called in a synctest bubble, whose clock it moves on.
func noRebirth(t *testing.T, a *App, tr *mockTransport, before int) {
	t.Helper()
	time.Sleep(5 * a.sessionWait)
	synctest.Wait()
	if n := rebirths(tr); n != before {
		t.Errorf("%d rebirths requested, want none", n-before)
	}
}

type dropMetrics struct {
	NopMetrics
	mu    sync.Mutex
	drops []string
}

func (m *dropMetrics) OnSessionDrop(groupID, edgeNodeID, messageType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.drops = append(m.drops, groupID+"/"+edgeNodeID+"/"+messageType)
}

func (m *dropMetrics) get() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.drops...)
}

// The report, with CheckBirthMillisecond: the DDATA of the ending session in
// the NBIRTH's millisecond, held before the NBIRTH, is dropped, and the new
// session goes on in sequence without a rebirth.
func TestHostCheckBirthMillisecondDropsHeld(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dm := &dropMetrics{}
		a, tr := checkedApp(t, func(c *Config) {
			c.SessionChecks = sparkplugb.CheckBirthMillisecond
			c.Metrics = dm
		})
		var got deviceValues
		a.cfg.OnDeviceData = got.record

		tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(49, 500, sessInt(t, "v", 304))) // ending session
		waitFor(t, func() bool { return rebirths(tr) == 1 })                                  // a DDATA before its NBIRTH
		tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 500, payload.BdSeqMetric(0)))
		tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 500, sessInt(t, "v", 0)))
		before := rebirths(tr)
		for s := uint64(2); s <= 49; s++ {
			tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(s, 500+s, sessInt(t, "v", int32(1000+s))))
		}

		if got.has(304) || !got.has(1049) {
			t.Fatalf("values %v: want 1049 delivered and not 304", got.v)
		}
		if ns := a.GetNode("g1", "n1"); ns.LastSeq != 49 {
			t.Errorf("LastSeq = %d, want 49", ns.LastSeq)
		}
		noRebirth(t, a, tr, before)
		if d := dm.get(); len(d) != 1 || d[0] != "g1/n1/DDATA" {
			t.Errorf("OnSessionDrop calls %v, want one for g1/n1/DDATA", d)
		}
	})
}

// With CheckBirthMillisecond, a DDATA of the ending session in the next
// NBIRTH's millisecond, applied to the ending session in sequence before that
// NBIRTH, is not taken back for the new session.
func TestHostCheckBirthMillisecondKeepsAppliedInEndedSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, tr := checkedApp(t, withChecks(sparkplugb.CheckBirthMillisecond))
		var got deviceValues
		a.cfg.OnDeviceData = got.record

		tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(0)))
		tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 100, sessInt(t, "v", 0)))
		tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(2, 500, sessInt(t, "v", 7))) // ending session, in sequence

		tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 500, payload.BdSeqMetric(0)))
		tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 500, sessInt(t, "v", 7)))
		before := rebirths(tr)
		tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(2, 501, sessInt(t, "v", 8)))

		if !got.has(8) {
			t.Fatalf("values %v: the new session's 8 not delivered", got.v)
		}
		if v := sessValue(t, a.GetNode("g1", "n1").Devices["d1"].Metrics, "v"); v != int32(8) {
			t.Errorf("v = %v, want 8", v)
		}
		noRebirth(t, a, tr, before)
	})
}

// Without the check, the same sequence takes the applied DDATA back for the
// new session, where it stands in for the new session's seq 2 (the default,
// go-sparkplugb#24).
func TestHostDefaultTakesBackAppliedInBirthMillisecond(t *testing.T) {
	a, tr := sessApp(t)
	var got deviceValues
	a.cfg.OnDeviceData = got.record

	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(0)))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 100, sessInt(t, "v", 0)))
	tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(2, 500, sessInt(t, "v", 7)))
	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 500, payload.BdSeqMetric(0)))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 500, sessInt(t, "v", 7)))
	tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(2, 501, sessInt(t, "v", 8)))

	if got.has(8) {
		t.Fatalf("values %v: the default changed, update SessionChecks and LIMITATIONS.md", got.v)
	}
}

// Case 2 with CheckOlderInSequence: the DDATA of the ending session that
// arrives in sequence after the NBIRTH is dropped, and the new session's
// message with that seq is applied, without a rebirth.
func TestHostCheckOlderInSequenceDropsEndedSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dm := &dropMetrics{}
		a, tr := checkedApp(t, func(c *Config) {
			c.SessionChecks = sparkplugb.CheckOlderInSequence
			c.Metrics = dm
		})
		var got deviceValues
		a.cfg.OnDeviceData = got.record

		tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(0)))
		tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 100, sessInt(t, "v", 0)))
		tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 1000, payload.BdSeqMetric(0)))
		tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 1000, sessInt(t, "v", 0)))
		before := rebirths(tr)
		tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(2, 999, sessInt(t, "v", 7)))  // ending session
		tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(2, 1000, sessInt(t, "v", 8))) // this session

		if got.has(7) || !got.has(8) {
			t.Fatalf("values %v: want 8 delivered and not 7", got.v)
		}
		noRebirth(t, a, tr, before)
		if d := dm.get(); len(d) != 1 || d[0] != "g1/n1/DDATA" {
			t.Errorf("OnSessionDrop calls %v, want one for g1/n1/DDATA", d)
		}
	})
}

// The checks drop data only: a DBIRTH stamped before its NBIRTH, and a DBIRTH
// in the NBIRTH's millisecond delivered before it, are applied with both.
func TestHostSessionChecksKeepBirths(t *testing.T) {
	t.Run("stamped early", func(t *testing.T) {
		a, tr := checkedApp(t, withChecks(sparkplugb.SessionChecksAll))
		tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 200, payload.BdSeqMetric(0)))
		tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 150, sessInt(t, "y", 1)))
		if a.GetNode("g1", "n1").Devices["d1"] == nil {
			t.Error("DBIRTH in sequence, stamped before the NBIRTH, not applied")
		}
	})
	t.Run("before the NBIRTH", func(t *testing.T) {
		a, tr := checkedApp(t, withChecks(sparkplugb.SessionChecksAll))
		tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(0)))
		tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(5, 500, sessInt(t, "y", 1))) // next session's, behind
		tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(4, 500, payload.BdSeqMetric(1)))
		if a.GetNode("g1", "n1").Devices["d1"] == nil {
			t.Error("DBIRTH in the NBIRTH's millisecond, delivered before it, not applied")
		}
	})
}

// The cost of CheckBirthMillisecond: a DDATA of the new session stamped in
// its NBIRTH's millisecond and delivered before it is dropped, and the gap it
// leaves costs a rebirth.
func TestHostCheckBirthMillisecondCostsRebirth(t *testing.T) {
	a, tr := checkedApp(t, withChecks(sparkplugb.CheckBirthMillisecond))
	var got deviceValues
	a.cfg.OnDeviceData = got.record

	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(0)))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 100, sessInt(t, "v", 0)))
	for s := uint64(2); s <= 10; s++ {
		tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(s, 100+s, sessInt(t, "v", 1)))
	}
	tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(2, 500, sessInt(t, "v", 9))) // next session's, behind
	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 500, payload.BdSeqMetric(1)))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 500, sessInt(t, "v", 0)))
	before := rebirths(tr)
	tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(3, 501, sessInt(t, "v", 10)))

	if got.has(9) {
		t.Errorf("values %v: the DDATA in the NBIRTH's millisecond was applied", got.v)
	}
	waitFor(t, func() bool { return rebirths(tr) > before })
}

// The cost of CheckOlderInSequence: data of an edge node that stamps a
// payload before its NBIRTH (a sample time, a clock that went back) is
// dropped, and the gap it leaves costs a rebirth.
func TestHostCheckOlderInSequenceCostsRebirth(t *testing.T) {
	a, tr := checkedApp(t, withChecks(sparkplugb.CheckOlderInSequence))
	var got deviceValues
	a.cfg.OnDeviceData = got.record

	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 1000, payload.BdSeqMetric(0)))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 1000, sessInt(t, "v", 0)))
	before := rebirths(tr)
	tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(2, 990, sessInt(t, "v", 7)))
	tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(3, 1001, sessInt(t, "v", 8)))

	if got.has(7) {
		t.Errorf("values %v: the DDATA stamped before the NBIRTH was applied", got.v)
	}
	waitFor(t, func() bool { return rebirths(tr) > before })
}

// A message without a timestamp is not judged by the checks.
func TestHostSessionChecksIgnoreZeroTimestamp(t *testing.T) {
	a, tr := checkedApp(t, withChecks(sparkplugb.SessionChecksAll))
	var got deviceValues
	a.cfg.OnDeviceData = got.record
	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 1000, payload.BdSeqMetric(0)))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 1000, sessInt(t, "v", 0)))
	tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(2, 0, sessInt(t, "v", 7)))
	if !got.has(7) {
		t.Errorf("values %v: DDATA without a timestamp not applied", got.v)
	}
}

// SessionChecksFor takes the place of SessionChecks for an edge node; when it
// does not return normally, SessionChecks applies.
func TestHostSessionChecksFor(t *testing.T) {
	for _, tc := range []struct {
		name     string
		checks   sparkplugb.SessionChecks
		forNode  func(g, n string) sparkplugb.SessionChecks
		wantDrop bool
	}{
		{"on for the node", 0, func(g, n string) sparkplugb.SessionChecks {
			if g == "g1" && n == "n1" {
				return sparkplugb.CheckOlderInSequence
			}
			return 0
		}, true},
		{"off for the node", sparkplugb.SessionChecksAll, func(string, string) sparkplugb.SessionChecks { return 0 }, false},
		{"panics", sparkplugb.CheckOlderInSequence, func(string, string) sparkplugb.SessionChecks { panic("boom") }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, tr := checkedApp(t, func(c *Config) {
				c.SessionChecks = tc.checks
				c.SessionChecksFor = tc.forNode
			})
			var got deviceValues
			a.cfg.OnDeviceData = got.record
			tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 1000, payload.BdSeqMetric(0)))
			tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 1000, sessInt(t, "v", 0)))
			tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(2, 999, sessInt(t, "v", 7)))
			if dropped := !got.has(7); dropped != tc.wantDrop {
				t.Errorf("dropped = %v, want %v", dropped, tc.wantDrop)
			}
		})
	}
}

// The drops that are always made are reported to SessionDropMetrics too.
func TestHostSessionDropMetricsDefault(t *testing.T) {
	dm := &dropMetrics{}
	_, tr := checkedApp(t, func(c *Config) { c.Metrics = dm })
	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 1000, payload.BdSeqMetric(0)))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 1000, sessInt(t, "v", 0)))
	tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(9, 999, sessInt(t, "v", 7))) // out of sequence, before the NBIRTH
	if d := dm.get(); len(d) != 1 || d[0] != "g1/n1/DDATA" {
		t.Errorf("OnSessionDrop calls %v, want one for g1/n1/DDATA", d)
	}
}

func TestHostNewRejectsUnknownSessionChecks(t *testing.T) {
	cfg := testConfig()
	cfg.SessionChecks = 0x80
	if _, err := New(newMockTransport(), cfg); err == nil || !strings.Contains(err.Error(), "SessionChecks") {
		t.Errorf("New with unknown SessionChecks: err = %v", err)
	}
	cfg.SessionChecks = sparkplugb.SessionChecksAll
	if _, err := New(newMockTransport(), cfg); err != nil {
		t.Errorf("New with SessionChecksAll: %v", err)
	}
}
