// SPDX-License-Identifier: MIT

package edge

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

// Data published while the node rebirths took its seq in the ending session
// and went out after the NBIRTH, or went out between the NBIRTH and the
// DBIRTH of its device. A host applied the old value in the place of the new
// session's message with that seq (go-sparkplugb#24). On the wire every
// session must count 0, 1, 2, ... and a device's data follow its DBIRTH. An
// NBIRTH is stamped after the message before it, so that a host can tell
// that message from the new session's.
func TestRebirthKeepsSessionsInSequence(t *testing.T) {
	ctx := context.Background()
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.PublishBirth(ctx, nil); err != nil {
		t.Fatal(err)
	}
	devs := []string{"d1", "d2"}
	for _, d := range devs {
		m, _ := payload.NewMetric("v", pb.DataType_Int32, int32(0))
		if err := n.PublishDeviceBirth(ctx, d, []*pb.Payload_Metric{m}); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for v := int32(0); ; v++ {
				select {
				case <-stop:
					return
				default:
				}
				m, _ := payload.NewMetric("v", pb.DataType_Int32, v)
				if i == 0 {
					_ = n.PublishNodeData(ctx, []*pb.Payload_Metric{m})
				} else {
					_ = n.PublishDeviceData(ctx, devs[i%2], []*pb.Payload_Metric{m})
				}
			}
		}()
	}
	for range 100 {
		if err := n.Rebirth(ctx); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()

	var (
		next     uint64
		born     map[string]bool
		sessions int
		lastTS   uint64
	)
	for i, p := range tr.getPublished() {
		tp, err := topic.Parse(p.Topic)
		if err != nil {
			t.Fatal(err)
		}
		pl, err := payload.Decode(p.Payload)
		if err != nil {
			t.Fatal(err)
		}
		switch tp.Type {
		case topic.NBIRTH:
			if i > 0 && pl.GetTimestamp() <= lastTS {
				t.Fatalf("message %d: NBIRTH stamped %d, not after the message before it (%d)", i, pl.GetTimestamp(), lastTS)
			}
			next, born = 0, map[string]bool{}
			sessions++
		case topic.DBIRTH:
			born[tp.DeviceID] = true
		case topic.DDATA:
			if !born[tp.DeviceID] {
				t.Fatalf("message %d: DDATA of %s before its DBIRTH", i, tp.DeviceID)
			}
		}
		if got := pl.GetSeq(); got != next {
			t.Fatalf("message %d (%s): seq %d, want %d", i, strings.TrimPrefix(p.Topic, "spBv1.0/testGroup/"), got, next)
		}
		next = (next + 1) % 256
		lastTS = pl.GetTimestamp()
	}
	if sessions != 101 {
		t.Errorf("sessions = %d, want 101", sessions)
	}
}

// birthTime stamps an NBIRTH after the message before it: in the same
// millisecond it waits for the next one. A clock that went back is not waited
// for, so then the NBIRTH is stamped earlier (LIMITATIONS.md).
func TestBirthTime(t *testing.T) {
	for _, c := range []struct {
		name  string
		clock []int64 // successive readings, in ms
		want  uint64
	}{
		{"later", []int64{1001}, 1001},
		{"same millisecond", []int64{1000, 1000, 1001}, 1001},
		{"clock went back", []int64{950}, 950},
	} {
		t.Run(c.name, func(t *testing.T) {
			n := &Node{lastTS: 1000}
			reads := 0
			n.now = func() time.Time {
				ms := c.clock[min(reads, len(c.clock)-1)]
				reads++
				return time.UnixMilli(ms)
			}
			if got := n.birthTime(); got != c.want {
				t.Errorf("birthTime = %d, want %d", got, c.want)
			}
			if reads != len(c.clock) {
				t.Errorf("read the clock %d times, want %d", reads, len(c.clock))
			}
		})
	}
}
