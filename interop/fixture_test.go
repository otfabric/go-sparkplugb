// SPDX-License-Identifier: MIT

package interop

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
)

// fixture is a sparkplugb-interop fixture: the edge node, its devices and the
// updates it publishes. The tests read it from the image they run against
// ("print-fixture"), so expectations follow the image rather than a copy here.
type fixture struct {
	Name     string     `json:"name"`
	Group    string     `json:"group"`
	EdgeNode string     `json:"edgeNode"`
	Node     fxNode     `json:"node"`
	Devices  []fxDevice `json:"devices"`
	Updates  []fxUpdate `json:"updates"`
}

type fxNode struct {
	Metrics []fxMetric `json:"metrics"`
}

type fxDevice struct {
	ID      string     `json:"id"`
	Metrics []fxMetric `json:"metrics"`
}

type fxMetric struct {
	Name     string `json:"name"`
	Alias    uint64 `json:"alias"`
	Datatype string `json:"datatype"`
	Value    any    `json:"value"`
	Writable bool   `json:"writable"`
}

type fxValue struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}

type fxUpdate struct {
	Node    []fxValue            `json:"node"`
	Devices map[string][]fxValue `json:"devices"`
}

// clone returns a deep copy, so a test can apply updates to its own state.
func (f *fixture) clone() *fixture {
	b, _ := json.Marshal(f)
	out := &fixture{}
	_ = json.Unmarshal(b, out)
	return out
}

// metrics returns the metrics of the node ("") or of a device.
func (f *fixture) metrics(device string) []fxMetric {
	if device == "" {
		return f.Node.Metrics
	}
	for _, d := range f.Devices {
		if d.ID == device {
			return d.Metrics
		}
	}
	return nil
}

func (f *fixture) metric(device, name string) (fxMetric, bool) {
	for _, m := range f.metrics(device) {
		if m.Name == name {
			return m, true
		}
	}
	return fxMetric{}, false
}

// set changes the value of a metric of the node ("") or of a device.
func (f *fixture) set(device, name string, v any) {
	ms := f.Node.Metrics
	if device != "" {
		for i := range f.Devices {
			if f.Devices[i].ID == device {
				ms = f.Devices[i].Metrics
			}
		}
	}
	for i := range ms {
		if ms[i].Name == name {
			ms[i].Value = v
		}
	}
}

// apply applies updates[n] to the fixture.
func (f *fixture) apply(n int) {
	u := f.Updates[n]
	for _, v := range u.Node {
		f.set("", v.Name, v.Value)
	}
	for dev, vs := range u.Devices {
		for _, v := range vs {
			f.set(dev, v.Name, v.Value)
		}
	}
}

// writable returns the first writable metric of the node ("") or a device.
func (f *fixture) writable(device string) (fxMetric, bool) {
	for _, m := range f.metrics(device) {
		if m.Writable {
			return m, true
		}
	}
	return fxMetric{}, false
}

// datatypes lists every datatype the fixture uses.
func (f *fixture) datatypes() []string {
	seen := map[string]bool{}
	var out []string
	add := func(ms []fxMetric) {
		for _, m := range ms {
			if !seen[m.Datatype] {
				seen[m.Datatype] = true
				out = append(out, m.Datatype)
			}
		}
	}
	add(f.Node.Metrics)
	for _, d := range f.Devices {
		add(d.Metrics)
	}
	return out
}

func dataType(t testing.TB, name string) pb.DataType {
	t.Helper()
	v, ok := pb.DataType_value[name]
	if !ok {
		t.Fatalf("fixture datatype %q is not a Sparkplug datatype", name)
	}
	return pb.DataType(v)
}

// goValue converts a fixture value (decoded JSON) to the Go type the payload
// package takes for the datatype.
func goValue(t testing.TB, datatype string, v any) any {
	t.Helper()
	if v == nil {
		return nil
	}
	switch datatype {
	case "Boolean":
		return v.(bool)
	case "String", "Text":
		return v.(string)
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("fixture value %v of %s is not a number", v, datatype)
	}
	switch datatype {
	case "Int8":
		return int8(f)
	case "Int16":
		return int16(f)
	case "Int32":
		return int32(f)
	case "Int64":
		return int64(f)
	case "UInt8":
		return uint8(f)
	case "UInt16":
		return uint16(f)
	case "UInt32":
		return uint32(f)
	case "UInt64":
		return uint64(f)
	case "Float":
		return float32(f)
	case "Double":
		return f
	case "DateTime":
		return time.UnixMilli(int64(f)).UTC()
	}
	t.Fatalf("fixture datatype %s is not supported here", datatype)
	return nil
}

// pbMetric builds a metric of the fixture with its name, and its alias when
// withAlias is set.
func pbMetric(t testing.TB, m fxMetric, withAlias bool) *pb.Payload_Metric {
	t.Helper()
	dt := dataType(t, m.Datatype)
	var (
		out *pb.Payload_Metric
		err error
	)
	if withAlias {
		out, err = payload.NewMetricWithAlias(m.Name, m.Alias, dt, goValue(t, m.Datatype, m.Value))
	} else {
		out, err = payload.NewMetric(m.Name, dt, goValue(t, m.Datatype, m.Value))
	}
	if err != nil {
		t.Fatalf("metric %s: %v", m.Name, err)
	}
	return out
}

// pbAliasMetric builds a data metric that carries only the alias, as a
// Sparkplug edge node may after its birth.
func pbAliasMetric(t testing.TB, m fxMetric) *pb.Payload_Metric {
	t.Helper()
	out, err := payload.NewAliasMetric(m.Alias, dataType(t, m.Datatype), goValue(t, m.Datatype, m.Value))
	if err != nil {
		t.Fatalf("metric %s: %v", m.Name, err)
	}
	return out
}

// jsonValue converts a received metric to its value as the container
// contract writes it: numbers for the numeric types and DateTime
// (milliseconds), booleans, strings.
func jsonValue(t testing.TB, m *pb.Payload_Metric) any {
	t.Helper()
	v, err := payload.GetValue(m)
	if err != nil {
		t.Fatalf("value of %s: %v", m.GetName(), err)
	}
	switch x := v.(type) {
	case nil, bool, string:
		return x
	case int8:
		return float64(x)
	case int16:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case uint8:
		return float64(x)
	case uint16:
		return float64(x)
	case uint32:
		return float64(x)
	case uint64:
		return float64(x)
	case float32:
		return float64(x)
	case float64:
		return x
	case time.Time:
		return float64(x.UnixMilli())
	}
	t.Fatalf("value of %s has unexpected type %T", m.GetName(), v)
	return nil
}

// sameValue compares two values as the container contract writes them.
// Fixture values of Float are exact in float32, so no tolerance is needed;
// JSON numbers above 2^53 are not allowed in fixtures.
func sameValue(a, b any) bool {
	fa, okA := a.(float64)
	fb, okB := b.(float64)
	if okA && okB {
		return fa == fb || (math.IsNaN(fa) && math.IsNaN(fb))
	}
	return fmt.Sprint(a) == fmt.Sprint(b) && fmt.Sprintf("%T", a) == fmt.Sprintf("%T", b)
}

// metricValue is a metric as the container contract writes it.
type metricValue struct {
	Name     string `json:"name"`
	Datatype string `json:"datatype"`
	Value    any    `json:"value"`
}

func (m metricValue) String() string { return fmt.Sprintf("%s(%s)=%v", m.Name, m.Datatype, m.Value) }

// expected lists the metrics of the node ("") or a device as a host must
// report them, in fixture order.
func (f *fixture) expected(device string) []metricValue {
	var out []metricValue
	for _, m := range f.metrics(device) {
		out = append(out, metricValue{Name: m.Name, Datatype: m.Datatype, Value: m.Value})
	}
	return out
}
