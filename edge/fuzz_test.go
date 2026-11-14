// SPDX-License-Identifier: MIT

package edge

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

// FuzzEdgeMessages delivers a message on each subscription of a node that
// waits for its primary host: the STATE of the primary host, an NCMD or a
// DCMD. A STATE brings the node online exactly when it is JSON that says
// online; a command reaches its handler exactly when it decodes, with its
// metrics.
func FuzzEdgeMessages(f *testing.F) {
	cmd, _ := payload.NewMetric("setpoint", pb.DataType_Double, 1.5)
	b, _ := payload.Encode(&pb.Payload{Metrics: []*pb.Payload_Metric{cmd}})
	rebirth, _ := payload.Encode(&pb.Payload{Metrics: []*pb.Payload_Metric{payload.RebirthMetric(true)}})
	for kind := range uint8(3) {
		f.Add(kind, b)
		f.Add(kind, rebirth)
		f.Add(kind, []byte(`{"online":true,"timestamp":1}`))
		f.Add(kind, []byte(`{"online":false,"timestamp":2}`))
		f.Add(kind, []byte{})
		f.Add(kind, []byte{0xff, 0x01})
	}

	f.Fuzz(func(t *testing.T, kind uint8, data []byte) {
		tr := newMockTransport()
		calls, metrics := 0, 0
		cfg := testConfig()
		cfg.PrimaryHostID = "host"
		count := func(_ string, ms []*pb.Payload_Metric) { calls++; metrics += len(ms) }
		cfg.OnNodeCmd, cfg.OnDeviceCmd = count, count
		n, err := New(tr, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer n.Close(context.Background())
		if err := n.Connect(context.Background()); err != nil {
			t.Fatal(err)
		}

		switch kind % 3 {
		case 0:
			tr.deliver(topic.ForState("host"), data)
			var sp statePayload
			online := json.Unmarshal(data, &sp) == nil && sp.Online
			if want := map[bool]State{true: Online, false: WaitingForPrimaryHost}[online]; n.State() != want {
				t.Fatalf("state %s after STATE %q, want %s", n.State(), data, want)
			}
			return
		case 1:
			tr.deliver(topic.ForNCMD("testGroup", "testNode"), data)
		case 2:
			tr.mu.Lock()
			h := tr.handlers["spBv1.0/testGroup/DCMD/testNode/+"]
			tr.mu.Unlock()
			h("spBv1.0/testGroup/DCMD/testNode/d1", data)
		}
		if s := n.State(); s != WaitingForPrimaryHost {
			t.Fatalf("state %s after a command, want waiting_for_primary_host", s)
		}
		p, err := payload.Decode(data)
		wantCalls, wantMetrics := 0, 0
		if err == nil {
			wantCalls, wantMetrics = 1, len(p.Metrics)
			if kind%3 == 1 {
				for _, m := range p.Metrics {
					if m.GetName() == rebirthMetricName && m.GetBooleanValue() {
						wantCalls, wantMetrics = 0, 0 // a rebirth request
						break
					}
				}
			}
		}
		if calls != wantCalls || metrics != wantMetrics {
			t.Fatalf("handler called %d times with %d metrics, want %d with %d", calls, metrics, wantCalls, wantMetrics)
		}
	})
}
