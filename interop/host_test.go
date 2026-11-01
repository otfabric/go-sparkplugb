//go:build interop

// SPDX-License-Identifier: MIT

package interop

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/host"
	"github.com/otfabric/go-sparkplugb/mqtt"
	pb "github.com/otfabric/go-sparkplugb/proto"
)

// TestHost runs the go-sparkplugb host application (package host) against
// the edge node of every reference, on every fixture the edge serves and
// every broker: births, data with aliases, writes, rebirth, device death and
// birth, NDEATH and the will, and the edge's view of the primary host.
func TestHost(t *testing.T) {
	for _, b := range brokers(t) {
		for _, a := range adapters(t) {
			if !a.isEdge(t) {
				continue
			}
			for _, fx := range a.edgeFixtures(t) {
				t.Run(fmt.Sprintf("%s/%s/%s", b.name, a.name, fx), func(t *testing.T) {
					t.Parallel()
					logReference(t, a)
					runHost(t, newWorld(t, b), a, fx)
				})
			}
		}
	}
}

// recorder keeps what the go-sparkplugb callbacks reported.
type recorder struct {
	mu   sync.Mutex
	seen []string // "NBIRTH", "DBIRTH/pump1", "NDATA", "NDEATH", ...
}

func (r *recorder) add(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, s)
}

func (r *recorder) count(s string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, x := range r.seen {
		if x == s {
			n++
		}
	}
	return n
}

func (r *recorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.seen, " ")
}

const libHostID = "go-sparkplugb-host"

// newLibHost connects a go-sparkplugb host application that records which
// messages its callbacks saw for the given edge node.
func newLibHost(t *testing.T, w *world, hostID, edgeNode string) (*host.App, *recorder, *mqtt.PahoTransport) {
	t.Helper()
	rec := &recorder{}
	mine := func(n string) bool { return n == edgeNode }
	tr := mqtt.NewPahoTransport()
	h, err := host.New(tr, host.Config{
		HostID:    hostID,
		BrokerURL: w.url,
		ClientID:  uniqueName("host"),
		OnNodeBirth: func(_, n string, _ []*pb.Payload_Metric) {
			if mine(n) {
				rec.add("NBIRTH")
			}
		},
		OnDeviceBirth: func(_, n, d string, _ []*pb.Payload_Metric) {
			if mine(n) {
				rec.add("DBIRTH/" + d)
			}
		},
		OnNodeData: func(_, n string, _ []*pb.Payload_Metric) {
			if mine(n) {
				rec.add("NDATA")
			}
		},
		OnDeviceData: func(_, n, d string, _ []*pb.Payload_Metric) {
			if mine(n) {
				rec.add("DDATA/" + d)
			}
		},
		OnNodeDeath: func(_, n string) {
			if mine(n) {
				rec.add("NDEATH")
			}
		},
		OnDeviceDeath: func(_, n, d string) {
			if mine(n) {
				rec.add("DDEATH/" + d)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := h.Connect(ctx); err != nil {
		t.Fatalf("go-sparkplugb host connect: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = h.Close(ctx)
	})
	return h, rec, tr
}

// hostState is what a go-sparkplugb host or application knows about an edge
// node: package host and package app have state types of the same shape.
type hostState struct {
	Online  bool
	Metrics map[string]*pb.Payload_Metric
	Devices map[string]hostDeviceState
}

type hostDeviceState struct {
	Online  bool
	Metrics map[string]*pb.Payload_Metric
}

func fromHost(ns *host.EdgeNodeState) *hostState {
	if ns == nil {
		return nil
	}
	hs := &hostState{Online: ns.Online, Metrics: ns.Metrics, Devices: map[string]hostDeviceState{}}
	for id, d := range ns.Devices {
		hs.Devices[id] = hostDeviceState{Online: d.Online, Metrics: d.Metrics}
	}
	return hs
}

// snapshot lists the metrics of the fixture as the state has them, as the
// container contract writes metrics.
func snapshot(t testing.TB, metrics map[string]*pb.Payload_Metric, f *fixture, device string) []metricValue {
	t.Helper()
	var out []metricValue
	for _, fm := range f.metrics(device) {
		m, ok := metrics[fm.Name]
		if !ok {
			continue
		}
		dt := pb.DataType(m.GetDatatype()).String()
		f.memberDatatypes(t, m)
		mv := metricValue{Name: fm.Name, Datatype: dt, Value: jsonValue(t, m)}
		if !f.ignoreProperties {
			mv.Properties = jsonProperties(t, m)
		}
		out = append(out, mv)
	}
	return out
}

func sameMetrics(a, b []metricValue) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Slice(a, func(i, j int) bool { return a[i].Name < a[j].Name })
	sort.Slice(b, func(i, j int) bool { return b[i].Name < b[j].Name })
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Datatype != b[i].Datatype || !sameValue(a[i].Value, b[i].Value) ||
			!sameProperties(a[i].Properties, b[i].Properties) {
			return false
		}
	}
	return true
}

func sameProperties(a, b map[string]fxProperty) bool {
	ja, errA := json.Marshal(a)
	jb, errB := json.Marshal(b)
	return errA == nil && errB == nil && string(ja) == string(jb)
}

// expectState waits until the go-sparkplugb host knows the node and every
// online device with the values of the fixture state.
func expectState(t *testing.T, h *host.App, f *fixture, edgeNode string, offline map[string]bool, what string) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		var diff string
		if hs := fromHost(h.GetNode(f.Group, edgeNode)); hs == nil {
			diff = "node unknown"
		} else {
			diff = hs.diff(t, f, offline)
		}
		if diff == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: go-sparkplugb host state differs from the fixture: %s", what, diff)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *hostState) diff(t *testing.T, f *fixture, offline map[string]bool) string {
	if !s.Online {
		return "node offline"
	}
	if got, want := snapshot(t, s.Metrics, f, ""), f.expected(""); !sameMetrics(got, want) {
		return fmt.Sprintf("node metrics %v, want %v", got, want)
	}
	for _, d := range f.Devices {
		ds, known := s.Devices[d.ID]
		if offline[d.ID] {
			if known && ds.Online {
				return fmt.Sprintf("device %s online, want offline", d.ID)
			}
			continue
		}
		if !known || !ds.Online {
			return fmt.Sprintf("device %s not online", d.ID)
		}
		if got, want := snapshot(t, ds.Metrics, f, d.ID), f.expected(d.ID); !sameMetrics(got, want) {
			return fmt.Sprintf("device %s metrics %v, want %v", d.ID, got, want)
		}
	}
	return ""
}

// newValue returns a different valid value of the datatype.
func newValue(datatype string, v any) any {
	switch datatype {
	case "Boolean":
		b, _ := v.(bool)
		return !b
	case "String", "Text":
		s, _ := v.(string)
		return s + "!"
	}
	f, _ := v.(float64)
	switch datatype {
	case "Float", "Double":
		return f + 0.5
	case "DateTime":
		return f + 1000
	case "Int8":
		if f >= 127 {
			return f - 1
		}
	case "UInt8":
		if f >= 255 {
			return f - 1
		}
	}
	return f + 1
}

// widthSizedEdges are the references that encode a negative Int8 or Int16 in
// its width; the others sign-extend it to 32 bits. Sparkplug leaves it open,
// and the library decodes both.
var widthSizedEdges = map[string]bool{"pysparkplug": true, "srad": true}

// checkIntForm checks that the Int8 and Int16 of a birth came in the form the
// reference is known for, so that the suite does exercise both.
func checkIntForm(t *testing.T, a adapter, f *fixture, got map[string]*pb.Payload_Metric) {
	t.Helper()
	for _, m := range f.Node.Metrics {
		bits := map[string]uint{"Int8": 8, "Int16": 16}[m.Datatype]
		v, ok := m.Value.(float64)
		if bits == 0 || !ok || got[m.Name] == nil {
			continue
		}
		want := uint32(int32(v))
		if widthSizedEdges[a.name] {
			want &= 1<<bits - 1
		}
		if raw := got[m.Name].GetIntValue(); raw != want {
			t.Errorf("%s of %s on the wire is %#x, want %#x", m.Name, a.name, raw, want)
		}
	}
	// Int8 and Int16 inside composite values: the fixture gives the value, the
	// wire value has the form of the reference.
	wire := func(where, datatype string, v any, raw uint32) {
		bits := map[string]uint{"Int8": 8, "Int16": 16}[datatype]
		n, ok := v.(float64)
		if bits == 0 || !ok {
			return
		}
		want := uint32(int32(n))
		if widthSizedEdges[a.name] {
			want &= 1<<bits - 1
		}
		if raw != want {
			t.Errorf("%s of %s on the wire is %#x, want %#x", where, a.name, raw, want)
		}
	}
	for _, m := range f.Node.Metrics {
		pm := got[m.Name]
		if pm == nil {
			continue
		}
		for i, k := range pm.GetProperties().GetKeys() {
			wire(m.Name+" property "+k, m.Properties[k].Type, m.Properties[k].Value, pm.GetProperties().GetValues()[i].GetIntValue())
		}
		switch v := m.Value.(type) {
		case map[string]any:
			if ds := pm.GetDatasetValue(); ds != nil {
				types := v["types"].([]any)
				for i, row := range v["rows"].([]any) {
					for j, cell := range row.([]any) {
						wire(fmt.Sprintf("%s cell %d,%d", m.Name, i, j), types[j].(string), cell, ds.GetRows()[i].GetElements()[j].GetIntValue())
					}
				}
			}
			if tmpl := pm.GetTemplateValue(); tmpl != nil {
				for i, x := range v["parameters"].([]any) {
					p := x.(map[string]any)
					wire(m.Name+" parameter "+p["name"].(string), p["type"].(string), p["value"], tmpl.GetParameters()[i].GetIntValue())
				}
				for i, x := range v["metrics"].([]any) {
					member := x.(map[string]any)
					wire(m.Name+" member "+member["name"].(string), member["datatype"].(string), member["value"], tmpl.GetMetrics()[i].GetIntValue())
				}
			}
		}
	}
}

func runHost(t *testing.T, w *world, a adapter, fxName string) {
	f := a.fixture(t, fxName)
	edgeNode := "e-" + a.name
	h, rec, _ := newLibHost(t, w, libHostID, edgeNode)

	args := []string{"--fixture", fxName, "--edge-node", edgeNode}
	primary := a.edgeHas(t, "primaryHost")
	if primary {
		args = append(args, "--primary-host", libHostID)
	}
	e := w.start(a, "edge", args...)
	ready := e.waitFor("ready", 1, func(ev event) bool { return ev.Event == "ready" })[0]
	offline := map[string]bool{}
	run := func(step string, fn func(t *testing.T)) {
		t.Run(step, func(t *testing.T) {
			if bug, known := a.knownBug(t, "edge", w.broker.name, fxName, step); known {
				t.Skipf("known bug of %s: %s", a.name, bug)
			}
			fn(t)
		})
	}

	run("births", func(t *testing.T) {
		expectState(t, h, f, edgeNode, offline, "births")
		ns := h.GetNode(f.Group, edgeNode)
		checkIntForm(t, a, f, ns.Metrics)
		if ready.BdSeq != nil && ns.BdSeq != uint64(*ready.BdSeq) {
			t.Errorf("bdSeq %d, the edge says %d", ns.BdSeq, *ready.BdSeq)
		}
		if a.edgeHas(t, "aliases") {
			// The alias each metric had in the NBIRTH must resolve to it.
			// An edge may assign its own aliases (Tahu does), not the fixture's.
			for _, m := range f.Node.Metrics {
				pm := ns.Metrics[m.Name]
				if pm == nil || pm.Alias == nil {
					t.Errorf("%s has no alias in the NBIRTH", m.Name)
					continue
				}
				if name, ok := h.ResolveAlias(f.Group, edgeNode, pm.GetAlias()); !ok || name != m.Name {
					t.Errorf("alias %d of %s resolves to %q", pm.GetAlias(), m.Name, name)
				}
			}
		}
	})

	if a.edgeCalls(t, "publish-data") {
		run("updates", func(t *testing.T) {
			for i := range f.Updates {
				res := e.mustCall("publish-data", map[string]int{"update": i})
				f.apply(i)
				expectState(t, h, f, edgeNode, offline, fmt.Sprintf("update %d (%s)", i, res.Result))
			}
		})
	}

	if a.edgeHas(t, "writes") {
		run("writes", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			applied := 0
			for _, dev := range append([]string{""}, deviceIDs(f)...) {
				m, ok := f.writable(dev)
				if !ok {
					continue
				}
				nv := newValue(m.Datatype, m.Value)
				m.Value = nv
				cmd := []*pb.Payload_Metric{pbMetric(t, m, false)}
				var err error
				if dev == "" {
					err = h.SendNodeCommand(ctx, f.Group, edgeNode, cmd)
				} else {
					err = h.SendDeviceCommand(ctx, f.Group, edgeNode, dev, cmd)
				}
				if err != nil {
					t.Fatalf("command %s: %v", m.Name, err)
				}
				f.set(dev, m.Name, nv)
				applied++
				expectState(t, h, f, edgeNode, offline, "write "+m.Name)
			}
			e.waitFor("applied command", applied, func(ev event) bool {
				return ev.Event == "command" && ev.Applied != nil && *ev.Applied
			})
		})
	}

	if a.edgeHas(t, "rebirthCommand") {
		run("rebirth-request", func(t *testing.T) {
			before := rec.count("NBIRTH")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := h.RequestRebirth(ctx, f.Group, edgeNode); err != nil {
				t.Fatal(err)
			}
			eventually(t, "NBIRTH after the rebirth request", func() bool { return rec.count("NBIRTH") > before })
			expectState(t, h, f, edgeNode, offline, "rebirth")
		})
	}

	if devs := deviceIDs(f); len(devs) > 0 && a.edgeCalls(t, "device-death", "device-birth") {
		run("device-death-birth", func(t *testing.T) {
			dev := devs[0]
			e.mustCall("device-death", map[string]string{"device": dev})
			offline[dev] = true
			expectState(t, h, f, edgeNode, offline, "device death")
			e.mustCall("device-birth", map[string]string{"device": dev})
			delete(offline, dev)
			expectState(t, h, f, edgeNode, offline, "device birth")
		})
	}

	if a.edgeCalls(t, "rebirth") {
		run("rebirth-call", func(t *testing.T) {
			before := rec.count("NBIRTH")
			e.mustCall("rebirth", map[string]any{})
			eventually(t, "NBIRTH of the rebirth", func() bool { return rec.count("NBIRTH") > before })
			expectState(t, h, f, edgeNode, offline, "rebirth")
		})
	}

	t.Run("death", func(t *testing.T) {
		step, how := "will", "kill"
		if a.edgeHas(t, "gracefulDeath") {
			step, how = "graceful-death", "stop"
		}
		before := rec.count("NDEATH")
		if how == "stop" {
			e.stop()
		} else {
			e.kill()
		}
		if bug, known := a.knownBug(t, "edge", w.broker.name, fxName, step); known {
			t.Logf("known bug of %s: %s", a.name, bug)
			return
		}
		eventually(t, "NDEATH on "+how, func() bool { return rec.count("NDEATH") > before })
		eventually(t, "node offline", func() bool {
			ns := h.GetNode(f.Group, edgeNode)
			return ns != nil && !ns.Online
		})
	})

	run("will", func(t *testing.T) {
		f = a.fixture(t, fxName)
		births := rec.count("NBIRTH")
		e.restart()
		eventually(t, "NBIRTH after the restart", func() bool { return rec.count("NBIRTH") > births })
		expectState(t, h, f, edgeNode, map[string]bool{}, "restart")
		bd := h.GetNode(f.Group, edgeNode).BdSeq
		deaths := rec.count("NDEATH")
		e.kill()
		eventually(t, "the will (NDEATH) after a kill", func() bool { return rec.count("NDEATH") > deaths })
		// A broker may still deliver the will of an earlier session (NanoMQ
		// and HiveMQ CE do after an MQTT 5 DISCONNECT): the host ignores its
		// older bdSeq, so wait for the node to go offline.
		eventually(t, fmt.Sprintf("node offline after its will, bdSeq %d", bd), func() bool {
			return !h.GetNode(f.Group, edgeNode).Online
		})
	})

	if primary {
		run("primary-host-offline", func(t *testing.T) {
			e.restart()
			e.waitFor("primary host online", 1, func(ev event) bool {
				return ev.Event == "primary-host" && ev.Online != nil && *ev.Online
			})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := h.Close(ctx); err != nil {
				t.Fatalf("close: %v", err)
			}
			e.waitFor("primary host offline", 1, func(ev event) bool {
				return ev.Event == "primary-host" && ev.Online != nil && !*ev.Online
			})
		})
	}

}

func deviceIDs(f *fixture) []string {
	var out []string
	for _, d := range f.Devices {
		out = append(out, d.ID)
	}
	return out
}
