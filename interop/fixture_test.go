//go:build interop

// SPDX-License-Identifier: MIT

package interop

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
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

	// ignoreProperties leaves properties out of comparisons, for a reference
	// host that has a known bug with them.
	ignoreProperties bool
	// ignoreUInt32Cells leaves the UInt32 columns of DataSets out of
	// comparisons, for a reference host that has a known bug with them.
	ignoreUInt32Cells bool
}

type fxNode struct {
	Metrics []fxMetric `json:"metrics"`
}

type fxDevice struct {
	ID      string     `json:"id"`
	Metrics []fxMetric `json:"metrics"`
}

type fxMetric struct {
	Name       string                `json:"name"`
	Alias      uint64                `json:"alias"`
	Datatype   string                `json:"datatype"`
	Value      any                   `json:"value"`
	Writable   bool                  `json:"writable"`
	Properties map[string]fxProperty `json:"properties"`
}

// fxProperty is a property of a metric, as the container contract writes it.
type fxProperty struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
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
	out := &fixture{ignoreProperties: f.ignoreProperties, ignoreUInt32Cells: f.ignoreUInt32Cells}
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
	case "DataSet":
		return goDataSet(t, v.(map[string]any))
	case "Template":
		return goTemplate(t, v.(map[string]any))
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

func goDataSet(t testing.TB, v map[string]any) *pb.Payload_DataSet {
	t.Helper()
	var (
		columns []string
		names   []string
		types   []pb.DataType
		rows    [][]any
	)
	for _, c := range v["columns"].([]any) {
		columns = append(columns, c.(string))
	}
	for _, n := range v["types"].([]any) {
		names = append(names, n.(string))
		types = append(types, dataType(t, n.(string)))
	}
	for _, r := range v["rows"].([]any) {
		var row []any
		for i, cell := range r.([]any) {
			row = append(row, goValue(t, names[i], cell))
		}
		rows = append(rows, row)
	}
	m, err := payload.NewDataSet("", columns, types, rows)
	if err != nil {
		t.Fatalf("DataSet: %v", err)
	}
	return m.GetDatasetValue()
}

func goTemplate(t testing.TB, v map[string]any) *pb.Payload_Template {
	t.Helper()
	var params []*pb.Payload_Template_Parameter
	for _, x := range v["parameters"].([]any) {
		p := x.(map[string]any)
		dt := p["type"].(string)
		tp, err := payload.NewTemplateParameter(p["name"].(string), dataType(t, dt), goValue(t, dt, p["value"]))
		if err != nil {
			t.Fatalf("template parameter: %v", err)
		}
		params = append(params, tp)
	}
	var members []*pb.Payload_Metric
	for _, x := range v["metrics"].([]any) {
		m := x.(map[string]any)
		dt := m["datatype"].(string)
		tm, err := payload.NewMetric(m["name"].(string), dataType(t, dt), goValue(t, dt, m["value"]))
		if err != nil {
			t.Fatalf("template member: %v", err)
		}
		members = append(members, tm)
	}
	var (
		m   *pb.Payload_Metric
		err error
	)
	if v["isDefinition"].(bool) {
		m, err = payload.NewTemplateDefinition("", v["version"].(string), members, params)
	} else {
		m, err = payload.NewTemplateInstance("", v["templateRef"].(string), members, params)
	}
	if err != nil {
		t.Fatalf("template: %v", err)
	}
	return m.GetTemplateValue()
}

// goProperties sets the properties of a fixture metric on a birth metric.
func goProperties(t testing.TB, m fxMetric, out *pb.Payload_Metric) {
	t.Helper()
	if len(m.Properties) == 0 {
		return
	}
	out.Properties = &pb.Payload_PropertySet{}
	for _, k := range slices.Sorted(maps.Keys(m.Properties)) {
		p := m.Properties[k]
		v, err := payload.NewPropertyValue(dataType(t, p.Type), goValue(t, p.Type, p.Value))
		if err != nil {
			t.Fatalf("property %s of %s: %v", k, m.Name, err)
		}
		out.Properties.Keys = append(out.Properties.Keys, k)
		out.Properties.Values = append(out.Properties.Values, v)
	}
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

// jsonNumber is a number as the container contract writes it.
func jsonNumber(t testing.TB, v any) any {
	t.Helper()
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
	t.Fatalf("value has unexpected type %T", v)
	return nil
}

// jsonValue converts a received metric to its value as the container
// contract writes it: numbers for the numeric types and DateTime
// (milliseconds), booleans, strings, objects for DataSet and Template.
func jsonValue(t testing.TB, m *pb.Payload_Metric) any {
	t.Helper()
	v, err := payload.GetValue(m)
	if err != nil {
		t.Fatalf("value of %s: %v", m.GetName(), err)
	}
	switch x := v.(type) {
	case *pb.Payload_DataSet:
		return jsonDataSet(t, m)
	case *pb.Payload_Template:
		return jsonTemplate(t, x)
	}
	return jsonNumber(t, v)
}

func jsonDataSet(t testing.TB, m *pb.Payload_Metric) any {
	t.Helper()
	columns, types, rows, err := payload.GetDataSetValues(m)
	if err != nil {
		t.Fatalf("DataSet %s: %v", m.GetName(), err)
	}
	names := []any{}
	for _, dt := range types {
		names = append(names, dt.String())
	}
	out := []any{}
	for _, row := range rows {
		cells := []any{}
		for _, c := range row {
			cells = append(cells, jsonNumber(t, c))
		}
		out = append(out, cells)
	}
	return map[string]any{"columns": columns, "types": names, "rows": out}
}

func jsonTemplate(t testing.TB, tmpl *pb.Payload_Template) any {
	t.Helper()
	params := []any{}
	for _, p := range tmpl.GetParameters() {
		v, err := payload.GetTemplateParameterValue(p)
		if err != nil {
			t.Fatalf("parameter %s: %v", p.GetName(), err)
		}
		params = append(params, map[string]any{"name": p.GetName(), "type": pb.DataType(p.GetType()).String(), "value": jsonNumber(t, v)})
	}
	members := []any{}
	for _, m := range tmpl.GetMetrics() {
		members = append(members, map[string]any{"name": m.GetName(), "datatype": pb.DataType(m.GetDatatype()).String(), "value": jsonValue(t, m)})
	}
	out := map[string]any{"isDefinition": tmpl.GetIsDefinition(), "parameters": params, "metrics": members}
	if tmpl.GetVersion() != "" {
		out["version"] = tmpl.GetVersion()
	}
	if tmpl.GetTemplateRef() != "" {
		out["templateRef"] = tmpl.GetTemplateRef()
	}
	return out
}

// jsonProperties converts the properties of a received metric.
func jsonProperties(t testing.TB, m *pb.Payload_Metric) map[string]fxProperty {
	t.Helper()
	ps := m.GetProperties()
	if len(ps.GetKeys()) == 0 {
		return nil
	}
	out := map[string]fxProperty{}
	for i, k := range ps.GetKeys() {
		v, err := payload.GetPropertyValue(ps.GetValues()[i])
		if err != nil {
			t.Fatalf("property %s of %s: %v", k, m.GetName(), err)
		}
		out[k] = fxProperty{Type: pb.DataType(ps.GetValues()[i].GetType()).String(), Value: jsonNumber(t, v)}
	}
	return out
}

// memberDatatypes gives the members of a template instance the datatypes of
// the definition when they have none: data messages may omit them, as they do
// for other metrics, and the Tahu edge does.
func (f *fixture) memberDatatypes(t testing.TB, m *pb.Payload_Metric) {
	t.Helper()
	tmpl := m.GetTemplateValue()
	if tmpl == nil || tmpl.GetIsDefinition() {
		return
	}
	def, ok := f.metric("", tmpl.GetTemplateRef())
	if !ok {
		t.Fatalf("no template definition %q", tmpl.GetTemplateRef())
	}
	for _, member := range tmpl.GetMetrics() {
		if member.Datatype != nil {
			continue
		}
		for _, x := range def.Value.(map[string]any)["metrics"].([]any) {
			if d := x.(map[string]any); d["name"] == member.GetName() {
				withDatatype(member, d["datatype"].(string), t)
			}
		}
	}
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
	if _, ok := a.(map[string]any); ok {
		ja, errA := json.Marshal(a)
		jb, errB := json.Marshal(b)
		return errA == nil && errB == nil && string(ja) == string(jb)
	}
	return fmt.Sprint(a) == fmt.Sprint(b) && fmt.Sprintf("%T", a) == fmt.Sprintf("%T", b)
}

// metricValue is a metric as the container contract writes it.
type metricValue struct {
	Name       string                `json:"name"`
	Datatype   string                `json:"datatype"`
	Value      any                   `json:"value"`
	Properties map[string]fxProperty `json:"properties"`
}

// withoutUInt32Cells returns the metric with the UInt32 cells of a DataSet zero.
func (m metricValue) withoutUInt32Cells() metricValue {
	ds, ok := m.Value.(map[string]any)
	if !ok || m.Datatype != "DataSet" {
		return m
	}
	b, _ := json.Marshal(ds)
	out := map[string]any{}
	_ = json.Unmarshal(b, &out)
	types := out["types"].([]any)
	for _, r := range out["rows"].([]any) {
		for i, t := range types {
			if t == "UInt32" {
				r.([]any)[i] = float64(0)
			}
		}
	}
	m.Value = out
	return m
}

func (m metricValue) String() string { return fmt.Sprintf("%s(%s)=%v", m.Name, m.Datatype, m.Value) }

// expected lists the metrics of the node ("") or a device as a host must
// report them, in fixture order.
func (f *fixture) expected(device string) []metricValue {
	var out []metricValue
	for _, m := range f.metrics(device) {
		mv := metricValue{Name: m.Name, Datatype: m.Datatype, Value: m.Value}
		if !f.ignoreProperties {
			mv.Properties = m.Properties
		}
		out = append(out, mv)
	}
	return out
}
