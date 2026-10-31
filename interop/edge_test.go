//go:build interop

// SPDX-License-Identifier: MIT

package interop

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/edge"
	"github.com/otfabric/go-sparkplugb/mqtt"
	pb "github.com/otfabric/go-sparkplugb/proto"
)

// TestEdge runs the go-sparkplugb edge node (package edge) against the host
// application of every reference, on every fixture whose datatypes the host
// declares and every broker. The edge publishes the fixture as the reference
// edges do; the reference host must report exactly the fixture: births with
// aliases, alias-only data, commands, rebirth on request, device death and
// birth, NDEATH, the will, and the primary host's STATE.
func TestEdge(t *testing.T) {
	for _, b := range brokers(t) {
		for _, a := range adapters(t) {
			if !a.isHost(t) {
				continue
			}
			for _, fx := range []string{"baseline", "scalars"} {
				t.Run(fmt.Sprintf("%s/%s/%s", b.name, a.name, fx), func(t *testing.T) {
					f := a.fixture(t, fx)
					if missing := a.hostAccepts(t, f); len(missing) > 0 {
						t.Skipf("%s host does not declare %v", a.name, missing)
					}
					if bug, known := a.knownBug(t, "host", b.name, fx, "births"); known {
						t.Skipf("known bug of %s: %s", a.name, bug)
					}
					t.Parallel()
					logReference(t, a)
					runEdge(t, newWorld(t, b), a, f)
				})
			}
		}
	}
}

// libEdge is a go-sparkplugb edge node that publishes a fixture and applies
// writes the way the reference edges do.
type libEdge struct {
	t        *testing.T
	node     *edge.Node
	f        *fixture
	mu       sync.Mutex
	commands []string // "<device or ->/<metric>=<value>"
}

func newLibEdge(t *testing.T, w *world, f *fixture, edgeNode, clientID, primaryHost string) (*libEdge, *mqtt.PahoTransport) {
	t.Helper()
	le := &libEdge{t: t, f: f}
	tr := mqtt.NewPahoTransport()
	n, err := edge.New(tr, edge.Config{
		GroupID:       f.Group,
		EdgeNodeID:    edgeNode,
		BrokerURL:     w.url,
		ClientID:      clientID,
		PrimaryHostID: primaryHost,
		OnNodeCmd:     func(_ string, ms []*pb.Payload_Metric) { le.command("", ms) },
		OnDeviceCmd:   func(d string, ms []*pb.Payload_Metric) { le.command(d, ms) },
	})
	if err != nil {
		t.Fatal(err)
	}
	le.node = n
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = n.Close(ctx)
	})
	return le, tr
}

// command applies a write to a writable metric and publishes the new value,
// as NDATA or DDATA, as a Sparkplug edge node reports what it changed.
func (le *libEdge) command(device string, ms []*pb.Payload_Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, m := range ms {
		name := m.GetName()
		if name == "" && m.Alias != nil {
			name, _ = le.node.ResolveAlias(m.GetAlias())
		}
		le.mu.Lock()
		fm, ok := le.f.metric(device, name)
		le.mu.Unlock()
		if !ok || !fm.Writable {
			continue
		}
		fm.Value = jsonValue(le.t, withDatatype(m, fm.Datatype, le.t))
		le.mu.Lock()
		le.f.set(device, name, fm.Value)
		key := device
		if key == "" {
			key = "-"
		}
		le.commands = append(le.commands, fmt.Sprintf("%s/%s=%v", key, name, fm.Value))
		le.mu.Unlock()
		out := []*pb.Payload_Metric{pbMetric(le.t, fm, false)}
		var err error
		if device == "" {
			err = le.node.PublishNodeData(ctx, out)
		} else {
			err = le.node.PublishDeviceData(ctx, device, out)
		}
		if err != nil {
			le.t.Errorf("report write of %s: %v", name, err)
		}
	}
}

// withDatatype gives a command metric the datatype of the fixture when the
// host sent none.
func withDatatype(m *pb.Payload_Metric, dt string, t testing.TB) *pb.Payload_Metric {
	if m.Datatype == nil {
		d := uint32(dataType(t, dt))
		m.Datatype = &d
	}
	return m
}

func (le *libEdge) commandCount() int {
	le.mu.Lock()
	defer le.mu.Unlock()
	return len(le.commands)
}

// births publishes NBIRTH and a DBIRTH per device, with names and aliases.
func (le *libEdge) births(ctx context.Context) error {
	var node []*pb.Payload_Metric
	for _, m := range le.f.Node.Metrics {
		node = append(node, pbMetric(le.t, m, true))
	}
	if err := le.node.PublishBirth(ctx, node); err != nil {
		return fmt.Errorf("NBIRTH: %w", err)
	}
	for _, d := range le.f.Devices {
		var ms []*pb.Payload_Metric
		for _, m := range d.Metrics {
			ms = append(ms, pbMetric(le.t, m, true))
		}
		if err := le.node.PublishDeviceBirth(ctx, d.ID, ms); err != nil {
			return fmt.Errorf("DBIRTH %s: %w", d.ID, err)
		}
	}
	return nil
}

// publishUpdate publishes updates[i]: NDATA and a DDATA per device, by alias
// only when byAlias is set.
func (le *libEdge) publishUpdate(ctx context.Context, i int, byAlias bool) error {
	le.f.apply(i)
	u := le.f.Updates[i]
	build := func(device string, vs []fxValue) []*pb.Payload_Metric {
		var out []*pb.Payload_Metric
		for _, v := range vs {
			fm, _ := le.f.metric(device, v.Name)
			if byAlias {
				out = append(out, pbAliasMetric(le.t, fm))
			} else {
				out = append(out, pbMetric(le.t, fm, false))
			}
		}
		return out
	}
	if len(u.Node) > 0 {
		if err := le.node.PublishNodeData(ctx, build("", u.Node)); err != nil {
			return err
		}
	}
	for _, d := range le.f.Devices {
		if vs, ok := u.Devices[d.ID]; ok {
			if err := le.node.PublishDeviceData(ctx, d.ID, build(d.ID, vs)); err != nil {
				return err
			}
		}
	}
	return nil
}

// fromEdge selects the "message" events of the host about one edge node.
func fromEdge(edgeNode string, types ...string) func(event) bool {
	return func(ev event) bool {
		return ev.Event == "message" && ev.EdgeNode == edgeNode && (len(types) == 0 || slices.Contains(types, ev.Type))
	}
}

// onlyFixture keeps the metrics the fixture names, sorted by name: what two
// stacks must agree on.
func onlyFixture(f *fixture, device string, ms []metricValue) []metricValue {
	var out []metricValue
	for _, m := range ms {
		if _, ok := f.metric(device, m.Name); ok {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// checkMessage compares the metrics of a host message with the fixture state.
func checkMessage(t *testing.T, f *fixture, ev event, want []metricValue) {
	t.Helper()
	got := onlyFixture(f, ev.device(), ev.Metrics)
	if !sameMetrics(got, append([]metricValue(nil), want...)) {
		t.Errorf("%s: host reported %v, want %v", ev, got, want)
	}
}

func runEdge(t *testing.T, w *world, a adapter, f *fixture) {
	const refHostID = "reference-host"
	edgeNode := "g-" + a.name
	h := w.start(a, "host", "--host-id", refHostID)

	le, _ := newLibEdge(t, w, f, edgeNode, uniqueName("edge"), refHostID)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := le.node.Connect(ctx); err != nil {
		t.Fatalf("go-sparkplugb edge connect: %v", err)
	}
	eventually(t, "the edge to see the primary host online", func() bool { return le.node.State() == edge.Online })
	if err := le.births(ctx); err != nil {
		t.Fatalf("births: %v", err)
	}
	byAlias := a.hostHas(t, "aliasResolution")
	nodes := func() []hostNode {
		var out struct {
			Nodes []hostNode `json:"nodes"`
		}
		decodeResult(t, h.mustCall("nodes", map[string]any{}), &out)
		var mine []hostNode
		for _, n := range out.Nodes {
			if n.EdgeNode == edgeNode {
				mine = append(mine, n)
			}
		}
		return mine
	}
	hasNodes := a.hostCalls(t, "nodes")
	run := func(step string, fn func(t *testing.T)) {
		t.Run(step, func(t *testing.T) {
			if bug, known := a.knownBug(t, "host", w.broker.name, f.Name, step); known {
				t.Skipf("known bug of %s: %s", a.name, bug)
			}
			fn(t)
		})
	}

	run("births", func(t *testing.T) {
		n := 1 + len(f.Devices)
		evs := h.waitFor("birth", n, fromEdge(edgeNode, "NBIRTH", "DBIRTH"))
		// MQTT orders messages per topic only: seq is the order.
		sort.SliceStable(evs, func(i, j int) bool { return seqOf(evs[i]) < seqOf(evs[j]) })
		nb := evs[0]
		if nb.Type != "NBIRTH" || nb.Seq == nil || *nb.Seq != 0 {
			t.Errorf("first message %s, want NBIRTH seq=0", nb)
		}
		if nb.BdSeq == nil || uint64(*nb.BdSeq) != le.node.BdSeq() {
			t.Errorf("%s: want bdSeq %d", nb, le.node.BdSeq())
		}
		if !slices.ContainsFunc(nb.Metrics, func(m metricValue) bool { return m.Name == "Node Control/Rebirth" }) {
			t.Errorf("NBIRTH as the host reports it has no Node Control/Rebirth: %v", nb.Metrics)
		}
		checkMessage(t, f, nb, f.expected(""))
		for i, d := range f.Devices {
			ev := evs[1+i]
			if ev.Type != "DBIRTH" || ev.device() != d.ID || ev.Seq == nil || *ev.Seq != 1+i {
				t.Errorf("message %d is %s, want DBIRTH/%s seq=%d", 1+i, ev, d.ID, 1+i)
				continue
			}
			checkMessage(t, f, ev, f.expected(d.ID))
		}
	})

	run("updates", func(t *testing.T) {
		for i, u := range f.Updates {
			before := len(h.matching(fromEdge(edgeNode, "NDATA", "DDATA")))
			if err := le.publishUpdate(ctx, i, byAlias); err != nil {
				t.Fatalf("update %d: %v", i, err)
			}
			n := len(u.Devices)
			if len(u.Node) > 0 {
				n++
			}
			evs := h.waitFor("data", before+n, fromEdge(edgeNode, "NDATA", "DDATA"))[before:]
			for _, ev := range evs {
				vs := u.Node
				if ev.Type == "DDATA" {
					vs = u.Devices[ev.device()]
				}
				var want []metricValue
				for _, v := range vs {
					fm, _ := f.metric(ev.device(), v.Name)
					want = append(want, metricValue{Name: v.Name, Datatype: fm.Datatype, Value: v.Value})
				}
				sort.Slice(want, func(i, j int) bool { return want[i].Name < want[j].Name })
				checkMessage(t, f, ev, want)
			}
		}
	})

	if hasNodes {
		run("nodes", func(t *testing.T) {
			ns := nodes()
			if len(ns) != 1 || !ns[0].Online || ns[0].BdSeq == nil || uint64(*ns[0].BdSeq) != le.node.BdSeq() {
				t.Fatalf("host tracks %+v, want %s online with bdSeq %d", ns, edgeNode, le.node.BdSeq())
			}
			if len(ns[0].Devices) != len(f.Devices) {
				t.Errorf("host tracks %d devices, want %d", len(ns[0].Devices), len(f.Devices))
			}
		})
	}

	if a.hostCalls(t, "send-command") {
		run("commands", func(t *testing.T) {
			sent := 0
			for _, dev := range append([]string{""}, deviceIDs(f)...) {
				m, ok := f.writable(dev)
				if !ok {
					continue
				}
				nv := newValue(m.Datatype, m.Value)
				payload := map[string]any{
					"group": f.Group, "edgeNode": edgeNode,
					"metrics": []metricValue{{Name: m.Name, Datatype: m.Datatype, Value: nv}},
				}
				if dev != "" {
					payload["device"] = dev
				}
				typ := "NDATA"
				if dev != "" {
					typ = "DDATA"
				}
				before := len(h.matching(fromEdge(edgeNode, typ)))
				h.mustCall("send-command", payload)
				sent++
				eventually(t, "the edge to receive the command for "+m.Name, func() bool { return le.commandCount() >= sent })
				ev := h.waitFor("report of the write", before+1, fromEdge(edgeNode, typ))[before]
				checkMessage(t, f, ev, []metricValue{{Name: m.Name, Datatype: m.Datatype, Value: nv}})
			}
			if sent == 0 {
				t.Skip("the fixture has no writable metric")
			}
		})
	}

	if a.hostCalls(t, "request-rebirth") {
		run("rebirth-request", func(t *testing.T) {
			before := len(h.matching(fromEdge(edgeNode, "NBIRTH")))
			h.mustCall("request-rebirth", map[string]string{"group": f.Group, "edgeNode": edgeNode})
			nb := h.waitFor("NBIRTH of the rebirth", before+1, fromEdge(edgeNode, "NBIRTH"))[before]
			if nb.Seq == nil || *nb.Seq != 0 || nb.BdSeq == nil || uint64(*nb.BdSeq) != le.node.BdSeq() {
				t.Errorf("%s, want seq=0 bdSeq=%d", nb, le.node.BdSeq())
			}
			checkMessage(t, f, nb, f.expected(""))
		})
	}

	if devs := deviceIDs(f); len(devs) > 0 {
		run("device-death-birth", func(t *testing.T) {
			dev := devs[0]
			before := len(h.matching(fromEdge(edgeNode, "DDEATH")))
			if err := le.node.PublishDeviceDeath(ctx, dev); err != nil {
				t.Fatal(err)
			}
			if ev := h.waitFor("DDEATH", before+1, fromEdge(edgeNode, "DDEATH"))[before]; ev.device() != dev {
				t.Errorf("%s, want DDEATH/%s", ev, dev)
			}
			if hasNodes {
				eventually(t, "the host to track the device offline", func() bool {
					ns := nodes()
					// Offline, or gone: a host that asked for a rebirth meanwhile
					// knows only the devices the rebirth announced.
					return len(ns) == 1 && !slices.ContainsFunc(ns[0].Devices, func(d hostDevice) bool {
						return d.Device == dev && d.Online
					})
				})
			}
			births := len(h.matching(fromEdge(edgeNode, "DBIRTH")))
			var ms []*pb.Payload_Metric
			for _, m := range f.metrics(dev) {
				ms = append(ms, pbMetric(t, m, true))
			}
			if err := le.node.PublishDeviceBirth(ctx, dev, ms); err != nil {
				t.Fatal(err)
			}
			ev := h.waitFor("DBIRTH", births+1, fromEdge(edgeNode, "DBIRTH"))[births]
			checkMessage(t, f, ev, f.expected(dev))
		})
	}

	run("graceful-death", func(t *testing.T) {
		bd := le.node.BdSeq()
		if err := le.node.Disconnect(ctx); err != nil {
			t.Fatal(err)
		}
		ev := h.waitFor("NDEATH", 1, fromEdge(edgeNode, "NDEATH"))[0]
		if ev.BdSeq == nil || uint64(*ev.BdSeq) != bd {
			t.Errorf("%s, want bdSeq %d of the NBIRTH", ev, bd)
		}
		if hasNodes {
			eventually(t, "the host to track the node offline", func() bool {
				ns := nodes()
				return len(ns) == 1 && !ns[0].Online
			})
		}
	})

	run("will", func(t *testing.T) {
		// A second session of the same edge node, then a takeover of its
		// client ID: the broker drops it without DISCONNECT and publishes
		// the will, an NDEATH with the bdSeq of this session's NBIRTH.
		clientID := uniqueName("edge")
		le2, _ := newLibEdge(t, w, a.fixture(t, f.Name), edgeNode, clientID, refHostID)
		if err := le2.node.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		eventually(t, "the primary host online", func() bool { return le2.node.State() == edge.Online })
		births := len(h.matching(fromEdge(edgeNode, "NBIRTH")))
		if err := le2.births(ctx); err != nil {
			t.Fatal(err)
		}
		nb := h.waitFor("NBIRTH", births+1, fromEdge(edgeNode, "NBIRTH"))[births]
		deaths := len(h.matching(fromEdge(edgeNode, "NDEATH")))
		thief := mqtt.NewPahoTransport()
		if err := thief.Connect(ctx, mqtt.ConnectOpts{BrokerURL: w.url, ClientID: clientID, CleanSession: true}); err != nil {
			t.Fatalf("takeover: %v", err)
		}
		defer func() { _ = thief.Disconnect(context.Background()) }()
		ev := h.waitFor("the will", deaths+1, fromEdge(edgeNode, "NDEATH"))[deaths]
		if ev.BdSeq == nil || nb.BdSeq == nil || *ev.BdSeq != *nb.BdSeq {
			t.Errorf("will %s, NBIRTH %s: bdSeq must match", ev, nb)
		}
		eventually(t, "the edge to notice the lost connection", func() bool { return le2.node.State() == edge.Disconnected })
		if hasNodes {
			eventually(t, "the host to track the node offline", func() bool {
				ns := nodes()
				return len(ns) == 1 && !ns[0].Online
			})
		}
	})

	if a.hostHas(t, "gracefulOffline") {
		run("primary-host-offline", func(t *testing.T) {
			le3, _ := newLibEdge(t, w, a.fixture(t, f.Name), edgeNode, uniqueName("edge"), refHostID)
			if err := le3.node.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			eventually(t, "the primary host online", func() bool { return le3.node.State() == edge.Online })
			if err := le3.births(ctx); err != nil {
				t.Fatal(err)
			}
			h.stop()
			// Sparkplug 3.0: the node disconnects (NDEATH first), connects
			// again and waits for the primary host.
			eventually(t, "the edge to reconnect and wait for the primary host", func() bool {
				return le3.node.State() == edge.WaitingForPrimaryHost
			})
		})
	}
}

// hostNode is one entry of the result of the "nodes" call of a host.
type hostNode struct {
	Group    string       `json:"group"`
	EdgeNode string       `json:"edgeNode"`
	Online   bool         `json:"online"`
	BdSeq    *int         `json:"bdSeq"`
	Devices  []hostDevice `json:"devices"`
}

type hostDevice struct {
	Device string `json:"device"`
	Online bool   `json:"online"`
}

func seqOf(ev event) int {
	if ev.Seq == nil {
		return 256
	}
	return *ev.Seq
}
