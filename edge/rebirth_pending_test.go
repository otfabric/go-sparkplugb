// SPDX-License-Identifier: MIT

package edge

import (
	"context"
	"strings"
	"testing"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

// gateTransport is the mock transport whose NBIRTH publishes wait for gate,
// once gated is set.
type gateTransport struct {
	*mockTransport
	gated   chan struct{} // closed: NBIRTH publishes wait for gate
	gate    chan struct{}
	waiting chan struct{} // an NBIRTH publish is waiting
}

func (g *gateTransport) Publish(ctx context.Context, t string, qos byte, retained bool, p []byte) error {
	select {
	case <-g.gated:
		if strings.Contains(t, "/NBIRTH/") {
			g.waiting <- struct{}{}
			<-g.gate
		}
	default:
	}
	return g.mockTransport.Publish(ctx, t, qos, retained, p)
}

// A rebirth request that arrives while a rebirth is pending is served by
// that rebirth: the node publishes one NBIRTH for both.
func TestRebirthRequestWhileOnePending(t *testing.T) {
	g := &gateTransport{mockTransport: newMockTransport(), gated: make(chan struct{}),
		gate: make(chan struct{}), waiting: make(chan struct{}, 1)}
	n, err := New(g, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close(context.Background())
	ctx := context.Background()
	if err := n.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{intMetric(t, "x", 1)}); err != nil {
		t.Fatal(err)
	}
	births := func() int {
		c := 0
		for _, m := range g.getPublished() {
			if strings.Contains(m.Topic, "/NBIRTH/") {
				c++
			}
		}
		return c
	}
	before := births()

	close(g.gated)
	ncmdTopic := topic.ForNCMD("testGroup", "testNode")
	g.deliver(ncmdTopic, ncmd(t, payload.RebirthMetric(true)))
	<-g.waiting // the first rebirth publishes its NBIRTH
	g.deliver(ncmdTopic, ncmd(t, payload.RebirthMetric(true)))
	if rebirthDone(n) {
		t.Fatal("rebirth not pending while its NBIRTH is published")
	}
	close(g.gate)
	eventually(t, "the rebirth", func() bool { return rebirthDone(n) })

	if got := births() - before; got != 1 {
		t.Errorf("published %d NBIRTHs for two requests during one rebirth, want 1", got)
	}
}
