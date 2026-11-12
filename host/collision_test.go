// SPDX-License-Identifier: MIT

package host

import (
	"sync"
	"testing"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

// A message of the ending session that takes the place of the new session's
// message with its seq cannot be told from it (LIMITATIONS.md,
// go-sparkplugb#24). These tests pin down what happens then: the old value is
// delivered, the new one is not, and the new one, behind the sequence now,
// gets a rebirth requested once sessionWait passes without an NBIRTH. The
// rebirth brings the current state back; it cannot take back the delivered
// value.

type deviceValues struct {
	mu sync.Mutex
	v  []int32
}

func (d *deviceValues) record(_, _, _ string, ms []*pb.Payload_Metric) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, m := range ms {
		d.v = append(d.v, int32(m.GetIntValue()))
	}
}

func (d *deviceValues) has(x int32) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, v := range d.v {
		if v == x {
			return true
		}
	}
	return false
}

// The report: a host that starts while the edge node publishes gets a DDATA
// of the ending session, stamped in the millisecond of the NBIRTH that
// follows, before that NBIRTH. The edge node of this library stamps an NBIRTH
// after the message before it; another edge node may not.
func TestHostSessionCollisionInBirthMillisecond(t *testing.T) {
	a, tr := sessApp(t)
	var got deviceValues
	a.cfg.OnDeviceData = got.record

	tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(49, 500, sessInt(t, "v", 304))) // ending session
	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 500, payload.BdSeqMetric(0)))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 500, sessInt(t, "v", 0)))
	for s := uint64(2); s < 49; s++ {
		tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(s, 500+s, sessInt(t, "v", int32(300+s))))
	}
	before := rebirths(tr)
	tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(49, 549, sessInt(t, "v", 352)))

	if !got.has(304) || got.has(352) {
		t.Fatalf("values %v: the limitation changed, update LIMITATIONS.md", got.v)
	}
	waitFor(t, func() bool { return rebirths(tr) > before })

	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 1000, payload.BdSeqMetric(0)))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 1000, sessInt(t, "v", 352)))
	ns := a.GetNode("g1", "n1")
	if v := sessValue(t, ns.Devices["d1"].Metrics, "v"); v != int32(352) {
		t.Errorf("v = %v after the rebirth, want 352", v)
	}
	if ns.LastSeq != 1 {
		t.Errorf("LastSeq = %d after the rebirth, want 1", ns.LastSeq)
	}
}

// An edge node that publishes in order, this library's too: a DDATA of the
// ending session, older than the next NBIRTH, delivered after that NBIRTH and
// its DBIRTH with the seq the new session expects. A message in sequence is
// applied whatever its timestamp (see AppliesOldTimestampInSequence).
func TestHostSessionCollisionInSequence(t *testing.T) {
	a, tr := sessApp(t)
	var got deviceValues
	a.cfg.OnDeviceData = got.record

	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(0)))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 100, sessInt(t, "v", 0)))

	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 1000, payload.BdSeqMetric(0)))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 1000, sessInt(t, "v", 0)))
	before := rebirths(tr)
	tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(2, 999, sessInt(t, "v", 7)))  // ending session
	tr.deliverToAll(sessTopic(topic.DDATA, "d1"), sessMsg(2, 1000, sessInt(t, "v", 8))) // this session

	if !got.has(7) || got.has(8) {
		t.Fatalf("values %v: the limitation changed, update LIMITATIONS.md", got.v)
	}
	waitFor(t, func() bool { return rebirths(tr) > before })

	tr.deliverToAll(sessTopic(topic.NBIRTH, ""), sessMsg(0, 2000, payload.BdSeqMetric(0)))
	tr.deliverToAll(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 2000, sessInt(t, "v", 8)))
	if v := sessValue(t, a.GetNode("g1", "n1").Devices["d1"].Metrics, "v"); v != int32(8) {
		t.Errorf("v = %v after the rebirth, want 8", v)
	}
}
