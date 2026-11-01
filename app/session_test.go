// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/mqtt"
	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

// Messages of an edge node are stamped by its clock: these tests give every
// message its timestamp, as the sessions of an edge node have them.

func testConfig() Config { return Config{BrokerURL: "tcp://localhost:1883"} }

func sessMsg(seq, ts uint64, ms ...*pb.Payload_Metric) []byte {
	p := payload.NewPayload(seq, ms...)
	p.Timestamp = &ts
	b, _ := payload.Encode(p)
	return b
}

func sessTopic(mt topic.MessageType, dev string) string {
	return topic.Topic{GroupID: "g1", Type: mt, EdgeNodeID: "n1", DeviceID: dev}.String()
}

func sessInt(t *testing.T, name string, v int32) *pb.Payload_Metric {
	t.Helper()
	m, err := payload.NewMetric(name, pb.DataType_Int32, v)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func sessApp(t *testing.T) (*Application, *mockTransport) {
	t.Helper()
	tr := newMockTransport()
	a, err := New(tr, testConfig())
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

func rebirths(tr *mockTransport) int {
	n := 0
	for _, p := range tr.getPublished() {
		if p.Topic == topic.ForNCMD("g1", "n1") {
			n++
		}
	}
	return n
}

func sessValue(t *testing.T, ms map[string]*pb.Payload_Metric, name string) any {
	t.Helper()
	m := ms[name]
	if m == nil {
		t.Fatalf("no metric %s", name)
	}
	v, err := payload.GetValue(m)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// A DBIRTH delivered twice while rebirths overlap stays held behind the
// sequence. The next session must not take it for its own: the edge node's
// restart would come up with the old values, as the interop will step did.
func TestApplicationDropsHeldMessageOfEndedSession(t *testing.T) {
	a, tr := sessApp(t)
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(0)))
	tr.deliver(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 100, sessInt(t, "y", 1)))
	tr.deliver(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 100, sessInt(t, "y", 1))) // late, held

	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 200, payload.BdSeqMetric(1)))
	tr.deliver(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 200, sessInt(t, "y", 2)))

	ns := a.GetNode("g1", "n1")
	if d := ns.Devices["d1"]; d == nil {
		t.Fatal("device not online")
	} else if v := sessValue(t, d.Metrics, "y"); v != int32(2) {
		t.Errorf("y = %v, want 2: the DBIRTH of the session, not the earlier one", v)
	}
}

// Data of the ended session that was waiting for its gap carried the sequence
// number past the edge node's: all that followed was then behind, and held
// for an NBIRTH that never came. The host stayed frozen.
func TestApplicationOldDataDoesNotPassSeqAcrossNBIRTH(t *testing.T) {
	a, tr := sessApp(t)
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(0), sessInt(t, "x", 0)))
	tr.deliver(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 100, sessInt(t, "y", 0)))
	tr.deliver(sessTopic(topic.DDATA, "d1"), sessMsg(3, 101, sessInt(t, "y", 1))) // ahead: held

	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 200, payload.BdSeqMetric(0), sessInt(t, "x", 1)))
	tr.deliver(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 200, sessInt(t, "y", 2)))
	tr.deliver(sessTopic(topic.NDATA, ""), sessMsg(2, 201, sessInt(t, "x", 2)))
	tr.deliver(sessTopic(topic.NDATA, ""), sessMsg(3, 202, sessInt(t, "x", 3)))

	ns := a.GetNode("g1", "n1")
	if ns.LastSeq != 3 {
		t.Errorf("LastSeq = %d, want 3, the edge node's", ns.LastSeq)
	}
	if v := sessValue(t, ns.Metrics, "x"); v != int32(3) {
		t.Errorf("x = %v, want 3", v)
	}
}

// The DBIRTHs of a rebirth delivered before its NBIRTH, with the seq the
// session that is ending expects next, were applied to that session and went
// with it: the node had no devices until a gap led to another rebirth. This is
// the births failure of the interop run.
func TestApplicationTakesBackMessagesOfTheNextSession(t *testing.T) {
	a, tr := sessApp(t)
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(0)))
	tr.deliver(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 200, sessInt(t, "y", 7)))
	tr.deliver(sessTopic(topic.DBIRTH, "d2"), sessMsg(2, 200, sessInt(t, "y", 8)))
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 200, payload.BdSeqMetric(0)))

	ns := a.GetNode("g1", "n1")
	for _, d := range []string{"d1", "d2"} {
		if ds := ns.Devices[d]; ds == nil || !ds.Online {
			t.Errorf("device %s not online after its NBIRTH", d)
		}
	}
	if ns.LastSeq != 2 {
		t.Errorf("LastSeq = %d, want 2", ns.LastSeq)
	}
}

// A session keeps only so many messages, and only those not older than the
// next NBIRTH go over to it.
func TestApplicationKeepsLastMessagesOfSession(t *testing.T) {
	a, tr := sessApp(t)
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(0), sessInt(t, "x", 0)))
	for i := uint64(1); i <= maxApplied+8; i++ {
		tr.deliver(sessTopic(topic.NDATA, ""), sessMsg(i, 100+i, sessInt(t, "x", int32(i))))
	}
	a.mu.RLock()
	got := a.sessions["g1/n1"].applied
	n, first := len(got), got[0].seq
	a.mu.RUnlock()
	if n != maxApplied || first != 9 {
		t.Errorf("kept %d from seq %d, want %d from 9", n, first, maxApplied)
	}
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 500, payload.BdSeqMetric(0), sessInt(t, "x", 100)))
	if ns := a.GetNode("g1", "n1"); ns.LastSeq != 0 {
		t.Errorf("LastSeq = %d: older messages went over to the new session", ns.LastSeq)
	}
}

// When the host is ahead of the edge node every message is behind. No NBIRTH
// follows: a rebirth has to be requested, or it stays frozen.
func TestApplicationRequestsRebirthWhenNoNBIRTHFollows(t *testing.T) {
	a, tr := sessApp(t)
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(0), sessInt(t, "x", 0)))
	for i := uint64(1); i <= 5; i++ {
		tr.deliver(sessTopic(topic.NDATA, ""), sessMsg(i, 100+i, sessInt(t, "x", 1)))
	}
	tr.deliver(sessTopic(topic.NDATA, ""), sessMsg(3, 200, sessInt(t, "x", 2)))
	if n := rebirths(tr); n != 0 {
		t.Fatalf("%d rebirth requests at once, want none before the wait", n)
	}
	waitFor(t, func() bool { return rebirths(tr) == 1 })
	if a.GetNode("g1", "n1") == nil {
		t.Fatal("node lost")
	}
}

// Nothing is requested when the NBIRTH does come, or the host goes away.
func TestApplicationSessionWaitEnds(t *testing.T) {
	for name, end := range map[string]func(a *Application, tr *mockTransport){
		"NBIRTH": func(_ *Application, tr *mockTransport) {
			tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 300, payload.BdSeqMetric(1)))
		},
		"Disconnect": func(a *Application, _ *mockTransport) { _ = a.Disconnect(context.Background()) },
		"connection lost": func(a *Application, _ *mockTransport) {
			a.onConnectionLost(fmt.Errorf("gone"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			a, tr := sessApp(t)
			tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 100, payload.BdSeqMetric(0)))
			for i := uint64(1); i <= 5; i++ {
				tr.deliver(sessTopic(topic.NDATA, ""), sessMsg(i, 100+i))
			}
			tr.deliver(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 300))
			end(a, tr)
			time.Sleep(3 * a.sessionWait)
			if n := rebirths(tr); n != 0 {
				t.Errorf("%d rebirth requests, want none", n)
			}
		})
	}
}

// The clock of an edge node can go back. Old timestamps are believed only
// shortly after an NBIRTH, and a message without one is not judged.
func TestApplicationSupersededLimits(t *testing.T) {
	a, _ := sessApp(t)
	a.sessions = map[string]*session{"k": {birthTS: 100, bornAt: time.Now()}}
	for _, c := range []struct {
		ts   uint64
		want bool
	}{{99, true}, {100, false}, {101, false}, {0, false}} {
		if got := a.superseded("k", c.ts); got != c.want {
			t.Errorf("ts %d: superseded = %v, want %v", c.ts, got, c.want)
		}
	}
	if a.superseded("unknown", 1) {
		t.Error("no session: nothing is superseded")
	}
	a.sessions["k"].bornAt = time.Now().Add(-heldMaxAge - time.Second)
	if a.superseded("k", 99) {
		t.Error("superseded long after the NBIRTH")
	}
}

// A message older than the NBIRTH that is in sequence is applied: only the
// sequence says it is wrong, and an edge node may stamp a birth early.
func TestApplicationAppliesOldTimestampInSequence(t *testing.T) {
	a, tr := sessApp(t)
	tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 200, payload.BdSeqMetric(0)))
	tr.deliver(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 150, sessInt(t, "y", 1)))
	if ns := a.GetNode("g1", "n1"); ns.Devices["d1"] == nil {
		t.Error("DBIRTH in sequence not applied")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// sessionNet is a broker that keeps the order per topic only, as MQTT says,
// and an edge node with two devices that answers rebirth requests: it births
// its devices in either order, as srad does, and rebirths overlap.
type sessionNet struct {
	mu      sync.Mutex
	handler mqtt.MessageHandler
	rng     *rand.Rand
	queues  map[string][][]byte
	topics  []string
	seq     uint64
	clock   uint64
	value   int32
	asked   int
	asks    int
}

func (s *sessionNet) Connect(context.Context, mqtt.ConnectOpts) error { return nil }
func (s *sessionNet) Disconnect(context.Context) error                { return nil }

func (s *sessionNet) Subscribe(_ context.Context, t string, _ byte, h mqtt.MessageHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.HasSuffix(t, "/#") {
		s.handler = h
	}
	return nil
}

func (s *sessionNet) Publish(_ context.Context, t string, _ byte, _ bool, _ []byte) error {
	if t == topic.ForNCMD("g1", "n1") {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.asked++
		s.asks++
	}
	return nil
}

func (s *sessionNet) publish(mt topic.MessageType, dev string, seq uint64, ms ...*pb.Payload_Metric) {
	s.clock += uint64(s.rng.Intn(2)) // often the same millisecond
	t := sessTopic(mt, dev)
	if _, ok := s.queues[t]; !ok {
		s.topics = append(s.topics, t)
	}
	s.queues[t] = append(s.queues[t], sessMsg(seq, s.clock, ms...))
}

func (s *sessionNet) next() uint64 {
	s.seq = (s.seq + 1) % 256
	return s.seq
}

func (s *sessionNet) metric(name string) *pb.Payload_Metric {
	m, _ := payload.NewMetric(name, pb.DataType_Int32, s.value)
	return m
}

func (s *sessionNet) birth() {
	s.seq = 0
	s.clock += 2 // a request takes a round trip
	s.publish(topic.NBIRTH, "", 0, payload.BdSeqMetric(0), s.metric("x"))
	devices := []string{"d1", "d2"}
	if s.rng.Intn(2) == 0 {
		devices = []string{"d2", "d1"}
	}
	for _, d := range devices {
		s.publish(topic.DBIRTH, d, s.next(), s.metric("y"))
	}
}

func (s *sessionNet) update() {
	s.value++
	s.publish(topic.NDATA, "", s.next(), s.metric("x"))
	s.publish(topic.DDATA, "d1", s.next(), s.metric("y"))
	s.publish(topic.DDATA, "d2", s.next(), s.metric("y"))
}

// step delivers a message of a random topic, or lets the edge node answer a
// request; it returns false when there is nothing to do.
func (s *sessionNet) step() bool {
	s.mu.Lock()
	var heads []string
	for _, t := range s.topics {
		if len(s.queues[t]) > 0 {
			heads = append(heads, t)
		}
	}
	if s.asked > 0 && (len(heads) == 0 || s.rng.Intn(3) == 0) {
		s.asked--
		s.birth()
		s.mu.Unlock()
		return true
	}
	if len(heads) == 0 {
		s.mu.Unlock()
		return false
	}
	t := heads[s.rng.Intn(len(heads))]
	data := s.queues[t][0]
	s.queues[t] = s.queues[t][1:]
	h := s.handler
	s.mu.Unlock()
	h(t, data)
	return true
}

func (s *sessionNet) settle() {
	for idle := 0; idle < 3; {
		if s.step() {
			idle = 0
			continue
		}
		idle++
		time.Sleep(200 * time.Microsecond)
	}
}

// current says what is wrong with the view of the host, "" for nothing.
func (s *sessionNet) current(a *Application) string {
	ns := a.GetNode("g1", "n1")
	if ns == nil {
		return "node unknown"
	}
	if v, err := payload.GetValue(ns.Metrics["x"]); err != nil || v != s.value {
		return "node stale"
	}
	for _, d := range []string{"d1", "d2"} {
		ds := ns.Devices[d]
		if ds == nil || !ds.Online {
			return "device missing"
		}
		if v, err := payload.GetValue(ds.Metrics["y"]); err != nil || v != s.value {
			return "device stale"
		}
	}
	return ""
}

// quiet leaves the host alone for as long as a throttled request takes: with
// no traffic it has to recover by itself.
func (s *sessionNet) quiet(a *Application) string {
	if s.current(a) == "" {
		return ""
	}
	for end := time.Now().Add(rebirthInterval + 500*time.Millisecond); time.Now().Before(end); {
		s.settle()
		if s.current(a) == "" {
			return ""
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.settle()
	return s.current(a)
}

// Whatever the broker makes of the order across topics and however the
// rebirths overlap, the host ends up with the edge node's state. Each seed is
// a start-up, three updates, and the rebirths they lead to.
func TestApplicationConvergesOnReorderedSessions(t *testing.T) {
	const seeds = 400
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		failed = map[string]int{}
		sem    = make(chan struct{}, 64)
	)
	for seed := int64(0); seed < seeds; seed++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s := &sessionNet{clock: 1000, rng: rand.New(rand.NewSource(seed)), queues: map[string][][]byte{}}
			a, err := New(s, testConfig())
			if err != nil {
				t.Error(err)
				return
			}
			a.sessionWait = 20 * time.Millisecond
			defer func() { _ = a.Close(context.Background()) }()
			if err := a.Connect(context.Background()); err != nil {
				t.Error(err)
				return
			}
			s.mu.Lock()
			s.birth()
			s.mu.Unlock()
			s.settle()
			why := s.quiet(a)
			for i := 0; i < 3 && why == ""; i++ {
				s.mu.Lock()
				s.update()
				s.mu.Unlock()
				s.settle()
				why = s.quiet(a)
			}
			if why != "" {
				mu.Lock()
				failed[why]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(failed) > 0 {
		t.Errorf("of %d seeds, did not converge: %v", seeds, failed)
	}
}
