// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"testing"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

// fuzzTopics are the topics a fuzzed message is delivered on: every
// message type of edge node g1/n1 and its device d1, and the STATE of a
// host.
var fuzzTopics = []string{
	sessTopic(topic.NBIRTH, ""), sessTopic(topic.NDATA, ""), sessTopic(topic.NDEATH, ""),
	sessTopic(topic.DBIRTH, "d1"), sessTopic(topic.DDATA, "d1"), sessTopic(topic.DDEATH, "d1"),
	topic.ForState("other"),
}

// fuzzInt is the Int32 metric x of value v.
func fuzzInt(v int32) *pb.Payload_Metric {
	m, _ := payload.NewMetric("x", pb.DataType_Int32, v)
	return m
}

// FuzzApplicationMessages delivers a message on a topic of a known edge
// node: the application stays connected, the edge node stays known, and the
// messages it holds stay within its bounds.
func FuzzApplicationMessages(f *testing.F) {
	for kind := range uint8(len(fuzzTopics)) {
		f.Add(kind, sessMsg(2, 3, fuzzInt(2)))
		f.Add(kind, sessMsg(0, 9, payload.BdSeqMetric(1), fuzzInt(5)))
		f.Add(kind, sessDeath(0))
		f.Add(kind, sessMsg(9, 4, fuzzInt(7)))
		f.Add(kind, []byte{})
		f.Add(kind, []byte{0x0a, 0xff})
	}

	f.Fuzz(func(t *testing.T, kind uint8, data []byte) {
		tr := newMockTransport()
		a, err := New(tr, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close(context.Background())
		if err := a.Connect(context.Background()); err != nil {
			t.Fatal(err)
		}
		tr.deliver(sessTopic(topic.NBIRTH, ""), sessMsg(0, 1, payload.BdSeqMetric(0), fuzzInt(1)))
		tr.deliver(sessTopic(topic.DBIRTH, "d1"), sessMsg(1, 2, fuzzInt(1)))

		tr.deliver(fuzzTopics[int(kind)%len(fuzzTopics)], data)

		if s := a.State(); s != Connected {
			t.Fatalf("state %s, want connected", s)
		}
		if a.GetNode("g1", "n1") == nil {
			t.Fatal("edge node forgotten")
		}
		msgs, bytes, _ := held(t, a)
		if msgs > maxHeldTotal || bytes > maxHeldBytes {
			t.Fatalf("held %d messages of %d bytes", msgs, bytes)
		}
	})
}
