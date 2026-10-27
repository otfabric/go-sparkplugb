// SPDX-License-Identifier: MIT

package interop

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/app"
	"github.com/otfabric/go-sparkplugb/mqtt"
	pb "github.com/otfabric/go-sparkplugb/proto"
)

// TestApplication runs the go-sparkplugb application that is not the primary
// host (package app: it publishes no STATE) against the edge node of every
// reference, on every fixture and broker: it must track births, alias-only
// data, device death and birth and the node's death like a host does.
func TestApplication(t *testing.T) {
	for _, b := range brokers(t) {
		for _, a := range adapters(t) {
			if !a.isEdge(t) {
				continue
			}
			for _, fx := range a.edgeFixtures(t) {
				t.Run(fmt.Sprintf("%s/%s/%s", b.name, a.name, fx), func(t *testing.T) {
					t.Parallel()
					logReference(t, a)
					runApplication(t, newWorld(t, b), a, fx)
				})
			}
		}
	}
}

func runApplication(t *testing.T, w *world, a adapter, fxName string) {
	f := a.fixture(t, fxName)
	edgeNode := "a-" + a.name
	rec := &recorder{}
	mine := func(n string) bool { return n == edgeNode }
	ap, err := app.New(mqtt.NewPahoTransport(), app.Config{
		BrokerURL: w.url,
		ClientID:  uniqueName("app"),
		OnNodeBirth: func(_, n string, _ []*pb.Payload_Metric) {
			if mine(n) {
				rec.add("NBIRTH")
			}
		},
		OnNodeDeath: func(_, n string) {
			if mine(n) {
				rec.add("NDEATH")
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ap.Connect(ctx); err != nil {
		t.Fatalf("go-sparkplugb application connect: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ap.Close(ctx)
	})

	// The edge runs without --primary-host: an application publishes no STATE.
	e := w.start(a, "edge", "--fixture", fxName, "--edge-node", edgeNode)
	offline := map[string]bool{}
	state := func(what string) {
		t.Helper()
		deadline := time.Now().Add(eventTimeout)
		for {
			diff := appStateDiff(t, ap.GetNode(f.Group, edgeNode), f, offline)
			if diff == "" {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: go-sparkplugb application state differs from the fixture: %s", what, diff)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	t.Run("births", func(t *testing.T) { state("births") })

	if a.edgeCalls(t, "publish-data") {
		t.Run("updates", func(t *testing.T) {
			for i := range f.Updates {
				e.mustCall("publish-data", map[string]int{"update": i})
				f.apply(i)
				state(fmt.Sprintf("update %d", i))
			}
		})
	}

	if devs := deviceIDs(f); len(devs) > 0 && a.edgeCalls(t, "device-death", "device-birth") {
		t.Run("device-death-birth", func(t *testing.T) {
			e.mustCall("device-death", map[string]string{"device": devs[0]})
			offline[devs[0]] = true
			state("device death")
			e.mustCall("device-birth", map[string]string{"device": devs[0]})
			delete(offline, devs[0])
			state("device birth")
		})
	}

	t.Run("death", func(t *testing.T) {
		deaths := rec.count("NDEATH")
		e.kill()
		eventually(t, "the will (NDEATH) after a kill", func() bool { return rec.count("NDEATH") > deaths })
		eventually(t, "node offline", func() bool {
			ns := ap.GetNode(f.Group, edgeNode)
			return ns != nil && !ns.Online
		})
	})
}

// appStateDiff is stateDiff for package app, whose state types mirror those
// of package host.
func appStateDiff(t *testing.T, ns *app.EdgeNodeState, f *fixture, offline map[string]bool) string {
	if ns == nil {
		return "node unknown"
	}
	hs := &hostState{Online: ns.Online, Metrics: ns.Metrics, Devices: map[string]hostDeviceState{}}
	for id, d := range ns.Devices {
		hs.Devices[id] = hostDeviceState{Online: d.Online, Metrics: d.Metrics}
	}
	return hs.diff(t, f, offline)
}
