// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/payload"
	"github.com/otfabric/go-sparkplugb/topic"
)

func sessDeath(bd uint64) []byte {
	return sessMsg(0, 0, payload.BdSeqMetric(bd))
}

// A will overtook the rebirths that were in flight when srad was killed on
// HiveMQ: the NBIRTHs that followed it, with its bdSeq, brought the dead node
// back online for good.
func TestApplicationLateBirthsAfterNDEATHKeepNodeOffline(t *testing.T) {
	a, tr := sessApp(t)
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(5)))

	tr.deliver(sessTopic(topic.NDEATH, ""), sessDeath(5))
	for ts := uint64(101); ts < 104; ts++ {
		tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, ts, payload.BdSeqMetric(5)))
		tr.deliver(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, ts, sessInt(t, "y", 1)))
	}

	if a.GetNode("g1", "n1").Online {
		t.Error("node online after its NDEATH")
	}
	if d := a.GetNode("g1", "n1").Devices["d1"]; d != nil && d.Online {
		t.Error("device online on a dead node")
	}
}

// The will of a session whose NBIRTH has not arrived yet does not match the
// session the application knows, and that NBIRTH must not start it.
func TestApplicationWillBeforeItsNBIRTH(t *testing.T) {
	a, tr := sessApp(t)
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(5)))

	tr.deliver(sessTopic(topic.NDEATH, ""), sessDeath(6))
	if !a.GetNode("g1", "n1").Online {
		t.Fatal("the NDEATH of another session took the node offline")
	}
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 200, payload.BdSeqMetric(6)))

	if a.GetNode("g1", "n1").Online {
		t.Error("node online after the will of this NBIRTH")
	}
}

// An edge node that restarts with the bdSeq of its NDEATH looks like a late
// NBIRTH: it comes online on the rebirth that follows.
func TestApplicationRestartWithSameBdSeqComesOnline(t *testing.T) {
	a, tr := sessApp(t)
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(0)))
	tr.deliver(sessTopic(topic.NDEATH, ""), sessDeath(0))
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 200, payload.BdSeqMetric(0)))

	waitFor(t, func() bool { return rebirths(tr) == 1 })
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 300, payload.BdSeqMetric(0)))
	if !a.GetNode("g1", "n1").Online {
		t.Error("node offline after answering the rebirth")
	}
}

// After the wait a NBIRTH counts again, and one with another bdSeq always does.
func TestApplicationNDEATHMemoryLimits(t *testing.T) {
	a, tr := sessApp(t)
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(1)))
	tr.deliver(sessTopic(topic.NDEATH, ""), sessDeath(1))
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 200, payload.BdSeqMetric(2)))
	if !a.GetNode("g1", "n1").Online {
		t.Fatal("next session not online")
	}

	tr.deliver(sessTopic(topic.NDEATH, ""), sessDeath(2))
	time.Sleep(2 * a.sessionWait)
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 300, payload.BdSeqMetric(2)))
	if !a.GetNode("g1", "n1").Online {
		t.Error("NBIRTH ignored after the wait")
	}
}

// The death the host ignored for its bdSeq is reported when that NBIRTH
// arrives.
func TestApplicationReportsDeathOnLateBirth(t *testing.T) {
	tr := newMockTransport()
	deaths := 0
	cfg := testConfig()
	cfg.OnNodeDeath = func(_, _ string) { deaths++ }
	a, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(5)))

	tr.deliver(sessTopic(topic.NDEATH, ""), sessDeath(6))
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 200, payload.BdSeqMetric(6)))
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 201, payload.BdSeqMetric(6)))

	if deaths != 1 {
		t.Errorf("%d deaths reported, want 1", deaths)
	}
}

// An NDEATH or NBIRTH without bdSeq says nothing about a session.
func TestApplicationNoBdSeqIsNotLate(t *testing.T) {
	a, tr := sessApp(t)
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(5)))
	tr.deliver(sessTopic(topic.NDEATH, ""), sessMsg(0, 0))
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 200, payload.BdSeqMetric(5)))
	if !a.GetNode("g1", "n1").Online {
		t.Error("NBIRTH dropped after an NDEATH without bdSeq")
	}

	tr.deliver(sessTopic(topic.NDEATH, ""), sessDeath(5))
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 300))
	if !a.GetNode("g1", "n1").Online {
		t.Error("NBIRTH without bdSeq dropped")
	}
}
