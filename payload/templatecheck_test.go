// SPDX-License-Identifier: MIT

package payload

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	pb "github.com/otfabric/go-sparkplugb/proto"
	"google.golang.org/protobuf/proto"
)

// templateFixture returns a definition "Motor" with members "rpm" and "amps"
// and parameter "rated", and an instance "M1" of it with all members.
func templateFixture(t *testing.T) (def, inst *pb.Payload_Metric) {
	t.Helper()
	member := func(name string, v float64) *pb.Payload_Metric {
		m, err := NewMetric(name, pb.DataType_Double, v)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	param := func(v int32) *pb.Payload_Template_Parameter {
		p, err := NewTemplateParameter("rated", pb.DataType_Int32, v)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	def, err := NewTemplateDefinition("Motor", "1.0",
		[]*pb.Payload_Metric{member("rpm", 0), member("amps", 0)},
		[]*pb.Payload_Template_Parameter{param(0)})
	if err != nil {
		t.Fatal(err)
	}
	inst, err = NewTemplateInstance("M1", "Motor",
		[]*pb.Payload_Metric{member("rpm", 1500), member("amps", 3.2)},
		[]*pb.Payload_Template_Parameter{param(400)})
	if err != nil {
		t.Fatal(err)
	}
	return def, inst
}

func TestNewTemplateDefsValid(t *testing.T) {
	def, inst := templateFixture(t)
	scalar, _ := NewMetric("temp", pb.DataType_Double, 1.0)
	// The instance may come before its definition in the NBIRTH.
	defs, err := NewTemplateDefs([]*pb.Payload_Metric{inst, scalar, def})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := defs["Motor"]; !ok || len(defs) != 1 {
		t.Fatalf("definitions %v", defs)
	}
	if err := defs.Check([]*pb.Payload_Metric{inst}, true); err != nil {
		t.Fatalf("DBIRTH: %v", err)
	}
	// A data message may have a subset of the members, and none of the
	// parameters.
	sub := proto.Clone(inst).(*pb.Payload_Metric)
	sub.GetTemplateValue().Metrics = sub.GetTemplateValue().Metrics[:1]
	sub.GetTemplateValue().Parameters = nil
	if err := defs.Check([]*pb.Payload_Metric{sub}, false); err != nil {
		t.Fatalf("NDATA subset: %v", err)
	}
	// No templates, no definitions: nothing to check.
	var none TemplateDefs
	if err := none.Check([]*pb.Payload_Metric{scalar}, false); err != nil {
		t.Fatal(err)
	}
}

func TestNewTemplateDefsMalformed(t *testing.T) {
	tests := []struct {
		name string
		edit func(def, inst *pb.Payload_Metric) []*pb.Payload_Metric
		want error
	}{
		{"instance without definition", func(_, inst *pb.Payload_Metric) []*pb.Payload_Metric {
			return []*pb.Payload_Metric{inst}
		}, ErrUnknownTemplate},
		{"instance refers to another name", func(def, inst *pb.Payload_Metric) []*pb.Payload_Metric {
			inst.GetTemplateValue().TemplateRef = proto.String("Pump")
			return []*pb.Payload_Metric{def, inst}
		}, ErrUnknownTemplate},
		{"definition with template_ref", func(def, inst *pb.Payload_Metric) []*pb.Payload_Metric {
			def.GetTemplateValue().TemplateRef = proto.String("Motor")
			return []*pb.Payload_Metric{def, inst}
		}, ErrInvalidTemplate},
		{"definition without name", func(def, _ *pb.Payload_Metric) []*pb.Payload_Metric {
			def.Name = nil
			return []*pb.Payload_Metric{def}
		}, ErrInvalidTemplate},
		{"definition twice", func(def, inst *pb.Payload_Metric) []*pb.Payload_Metric {
			return []*pb.Payload_Metric{def, proto.Clone(def).(*pb.Payload_Metric), inst}
		}, ErrInvalidTemplate},
		{"definition version not UTF-8", func(def, _ *pb.Payload_Metric) []*pb.Payload_Metric {
			def.GetTemplateValue().Version = proto.String("\xff")
			return []*pb.Payload_Metric{def}
		}, ErrInvalidTemplate},
		{"definition parameter without name", func(def, _ *pb.Payload_Metric) []*pb.Payload_Metric {
			def.GetTemplateValue().Parameters[0].Name = nil
			return []*pb.Payload_Metric{def}
		}, ErrInvalidTemplate},
		{"definition parameter without type", func(def, _ *pb.Payload_Metric) []*pb.Payload_Metric {
			def.GetTemplateValue().Parameters[0].Type = nil
			return []*pb.Payload_Metric{def}
		}, ErrInvalidTemplate},
		{"definition parameter of a complex type", func(def, _ *pb.Payload_Metric) []*pb.Payload_Metric {
			dt := uint32(pb.DataType_DataSet)
			def.GetTemplateValue().Parameters[0].Type = &dt
			return []*pb.Payload_Metric{def}
		}, ErrInvalidTemplate},
		{"instance without is_definition", func(def, inst *pb.Payload_Metric) []*pb.Payload_Metric {
			inst.GetTemplateValue().IsDefinition = nil
			return []*pb.Payload_Metric{def, inst}
		}, ErrInvalidTemplate},
		{"instance without template_ref", func(def, inst *pb.Payload_Metric) []*pb.Payload_Metric {
			inst.GetTemplateValue().TemplateRef = nil
			return []*pb.Payload_Metric{def, inst}
		}, ErrInvalidTemplate},
		{"instance with empty template_ref", func(def, inst *pb.Payload_Metric) []*pb.Payload_Metric {
			inst.GetTemplateValue().TemplateRef = proto.String("")
			return []*pb.Payload_Metric{def, inst}
		}, ErrInvalidTemplate},
		{"instance template_ref not UTF-8", func(def, inst *pb.Payload_Metric) []*pb.Payload_Metric {
			inst.GetTemplateValue().TemplateRef = proto.String("\xfe")
			return []*pb.Payload_Metric{def, inst}
		}, ErrInvalidTemplate},
		{"instance member not in definition", func(def, inst *pb.Payload_Metric) []*pb.Payload_Metric {
			extra, _ := NewMetric("volts", pb.DataType_Double, 1.0)
			inst.GetTemplateValue().Metrics = append(inst.GetTemplateValue().Metrics, extra)
			return []*pb.Payload_Metric{def, inst}
		}, ErrInvalidTemplate},
		{"instance in birth lacks a member", func(def, inst *pb.Payload_Metric) []*pb.Payload_Metric {
			inst.GetTemplateValue().Metrics = inst.GetTemplateValue().Metrics[1:]
			return []*pb.Payload_Metric{def, inst}
		}, ErrInvalidTemplate},
		{"instance parameter not in definition", func(def, inst *pb.Payload_Metric) []*pb.Payload_Metric {
			p, _ := NewTemplateParameter("other", pb.DataType_Int32, int32(1))
			inst.GetTemplateValue().Parameters = append(inst.GetTemplateValue().Parameters, p)
			return []*pb.Payload_Metric{def, inst}
		}, ErrInvalidTemplate},
		{"nested instance in definition refers to nothing", func(def, _ *pb.Payload_Metric) []*pb.Payload_Metric {
			nested, _ := NewTemplateInstance("sub", "Missing", nil, nil)
			def.GetTemplateValue().Metrics = append(def.GetTemplateValue().Metrics, nested)
			return []*pb.Payload_Metric{def}
		}, ErrUnknownTemplate},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def, inst := templateFixture(t)
			_, err := NewTemplateDefs(tt.edit(def, inst))
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
}

// An invalid definition is reported, and kept: its instances still refer to
// it. The SparkplugNet edge node sends an empty template_ref with its
// definitions.
func TestNewTemplateDefsKeepsInvalid(t *testing.T) {
	def, inst := templateFixture(t)
	def.GetTemplateValue().TemplateRef = proto.String("")
	defs, err := NewTemplateDefs([]*pb.Payload_Metric{def, inst})
	if !errors.Is(err, ErrInvalidTemplate) || !strings.Contains(err.Error(), "has a template_ref") {
		t.Fatalf("got %v", err)
	}
	if _, ok := defs["Motor"]; !ok {
		t.Fatal("definition dropped")
	}
	if err := defs.Check([]*pb.Payload_Metric{inst}, false); err != nil {
		t.Fatalf("instance of the kept definition: %v", err)
	}
	// One without a name cannot be referred to, and is left out.
	def.Name = nil
	if defs, _ := NewTemplateDefs([]*pb.Payload_Metric{def}); len(defs) != 0 {
		t.Fatalf("definitions %v", defs)
	}
}

func TestTemplateDefsCheck(t *testing.T) {
	def, inst := templateFixture(t)
	defs, err := NewTemplateDefs([]*pb.Payload_Metric{def})
	if err != nil {
		t.Fatal(err)
	}

	// A definition outside the NBIRTH, in a birth or in data.
	for _, birth := range []bool{true, false} {
		if err := defs.Check([]*pb.Payload_Metric{def}, birth); !errors.Is(err, ErrInvalidTemplate) {
			t.Errorf("definition, birth=%v: %v", birth, err)
		}
	}
	// A DBIRTH instance needs all members, data does not.
	partial := proto.Clone(inst).(*pb.Payload_Metric)
	partial.GetTemplateValue().Metrics = partial.GetTemplateValue().Metrics[:1]
	if err := defs.Check([]*pb.Payload_Metric{partial}, true); !errors.Is(err, ErrInvalidTemplate) {
		t.Errorf("partial in DBIRTH: %v", err)
	}
	if err := defs.Check([]*pb.Payload_Metric{partial}, false); err != nil {
		t.Errorf("partial in data: %v", err)
	}
	// Data by alias, without the datatype: the value still makes it a
	// template, and an unknown reference is found.
	alias := uint64(7)
	byAlias := &pb.Payload_Metric{Alias: &alias, Value: &pb.Payload_Metric_TemplateValue{
		TemplateValue: &pb.Payload_Template{TemplateRef: proto.String("Pump"), IsDefinition: proto.Bool(false)},
	}}
	err = defs.Check([]*pb.Payload_Metric{byAlias}, false)
	if !errors.Is(err, ErrUnknownTemplate) {
		t.Fatalf("by alias: %v", err)
	}
	if want := `template instance "alias 7"`; !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not name %s", err, want)
	}
	// Any instance with no definitions at all.
	var none TemplateDefs
	if err := none.Check([]*pb.Payload_Metric{inst}, false); !errors.Is(err, ErrUnknownTemplate) {
		t.Errorf("no definitions: %v", err)
	}
	// The version is not compared: Sparkplug gives it no matching rule.
	other := proto.Clone(inst).(*pb.Payload_Metric)
	other.GetTemplateValue().Version = proto.String("2.0")
	if err := defs.Check([]*pb.Payload_Metric{other}, true); err != nil {
		t.Errorf("other version: %v", err)
	}
	// A member by alias only is not checked by name.
	anon := proto.Clone(partial).(*pb.Payload_Metric)
	anon.GetTemplateValue().Metrics[0].Name = nil
	anon.GetTemplateValue().Metrics[0].Alias = &alias
	if err := defs.Check([]*pb.Payload_Metric{anon}, false); err != nil {
		t.Errorf("member by alias: %v", err)
	}
}

// Nested instances: a definition may have a member that is an instance of
// another definition, and an instance of it is checked member by member.
func TestTemplateDefsNested(t *testing.T) {
	rpm, _ := NewMetric("rpm", pb.DataType_Double, 0.0)
	motor, _ := NewTemplateDefinition("Motor", "", []*pb.Payload_Metric{rpm}, nil)
	motorRef, _ := NewTemplateInstance("motor", "Motor", []*pb.Payload_Metric{proto.Clone(rpm).(*pb.Payload_Metric)}, nil)
	pump, _ := NewTemplateDefinition("Pump", "", []*pb.Payload_Metric{motorRef}, nil)

	nestedRPM, _ := NewMetric("rpm", pb.DataType_Double, 900.0)
	nested, _ := NewTemplateInstance("motor", "Motor", []*pb.Payload_Metric{nestedRPM}, nil)
	p1, _ := NewTemplateInstance("P1", "Pump", []*pb.Payload_Metric{nested}, nil)
	defs, err := NewTemplateDefs([]*pb.Payload_Metric{pump, motor, p1})
	if err != nil {
		t.Fatal(err)
	}

	bad := proto.Clone(p1).(*pb.Payload_Metric)
	extra, _ := NewMetric("torque", pb.DataType_Double, 1.0)
	inner := bad.GetTemplateValue().Metrics[0].GetTemplateValue()
	inner.Metrics = append(inner.Metrics, extra)
	if err := defs.Check([]*pb.Payload_Metric{bad}, false); !errors.Is(err, ErrInvalidTemplate) {
		t.Errorf("nested extra member: %v", err)
	}

	wrong := proto.Clone(p1).(*pb.Payload_Metric)
	wrong.GetTemplateValue().Metrics[0].GetTemplateValue().TemplateRef = proto.String("Fan")
	if err := defs.Check([]*pb.Payload_Metric{wrong}, false); !errors.Is(err, ErrUnknownTemplate) {
		t.Errorf("nested unknown ref: %v", err)
	}

	defInside := proto.Clone(p1).(*pb.Payload_Metric)
	defInside.GetTemplateValue().Metrics[0].GetTemplateValue().IsDefinition = proto.Bool(true)
	if err := defs.Check([]*pb.Payload_Metric{defInside}, false); !errors.Is(err, ErrInvalidTemplate) {
		t.Errorf("definition inside instance: %v", err)
	}
}

// FuzzTemplateDefs runs the template checks on a decoded payload, as the
// definitions of a birth and as the instances of a birth and of data: every
// error is ErrInvalidTemplate or ErrUnknownTemplate.
func FuzzTemplateDefs(f *testing.F) {
	rpm, _ := NewMetric("rpm", pb.DataType_Double, 0.0)
	def, _ := NewTemplateDefinition("Motor", "1", []*pb.Payload_Metric{rpm}, nil)
	inst, _ := NewTemplateInstance("M1", "Motor", []*pb.Payload_Metric{rpm}, nil)
	b, _ := Encode(NewPayload(0, def, inst))
	f.Add(b)
	for _, ms := range [][]*pb.Payload_Metric{
		{nestedDefinition(3, "d")},
		{recursiveDefinition(), nestedInstance(3)},
		{nestedDefinition(maxTemplateDepth+1, "d")},
		{nestedInstance(2)},
	} {
		b, _ := Encode(NewPayload(0, ms...))
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := Decode(data)
		if err != nil {
			return
		}
		check := func(what string, err error) {
			if err != nil && !errors.Is(err, ErrInvalidTemplate) && !errors.Is(err, ErrUnknownTemplate) {
				t.Fatalf("%s: %v, want ErrInvalidTemplate or ErrUnknownTemplate", what, err)
			}
		}
		defs, err := NewTemplateDefs(p.GetMetrics())
		check("NewTemplateDefs", err)
		check("Check in a birth", defs.Check(p.GetMetrics(), true))
		check("Check in data", defs.Check(p.GetMetrics(), false))
	})
}

// A definition may give a member name more than once. The completeness check
// of an instance in a birth then costs the members of the instance, not of
// the definition.
func TestTemplateBirthCheckFollowsInstance(t *testing.T) {
	dt := uint32(pb.DataType_Int32)
	member := func(name string) *pb.Payload_Metric {
		return &pb.Payload_Metric{Name: proto.String(name), Datatype: &dt}
	}
	instance := func(names ...string) *pb.Payload_Template {
		inst := &pb.Payload_Template{IsDefinition: proto.Bool(false), TemplateRef: proto.String("D")}
		for _, n := range names {
			inst.Metrics = append(inst.Metrics, member(n))
		}
		return inst
	}

	const n = 1000
	def := &pb.Payload_Template{IsDefinition: proto.Bool(true)}
	for range n {
		def.Metrics = append(def.Metrics, member("a"))
	}
	c := TemplateDefs{"D": def}.newChecker()
	if err := c.checkInstance([]string{"i"}, instance("a"), true); err != nil {
		t.Fatal(err)
	}
	if c.steps != 1 {
		t.Errorf("an instance of 1 member against a definition of %d: %d steps, want 1", n, c.steps)
	}

	// Repeated names still have to be all there.
	def = &pb.Payload_Template{IsDefinition: proto.Bool(true), Metrics: []*pb.Payload_Metric{member("a"), member("a"), member("b")}}
	defs := TemplateDefs{"D": def}
	for _, tc := range []struct {
		members []string
		want    error
	}{
		{[]string{"a", "b"}, nil},
		{[]string{"b", "a", "a"}, nil},
		{[]string{"a", "a"}, ErrInvalidTemplate},
		{[]string{"b"}, ErrInvalidTemplate},
	} {
		if err := defs.newChecker().checkInstance([]string{"i"}, instance(tc.members...), true); !errors.Is(err, tc.want) {
			t.Errorf("instance %v: %v, want %v", tc.members, err, tc.want)
		}
	}
}

// Check takes the names of a definition from NewTemplateDefs, so that a
// message costs the same however large the stored definition is.
func TestTemplateCheckAllocsIndependentOfDefinition(t *testing.T) {
	dt := uint32(pb.DataType_Int32)
	tdt := uint32(pb.DataType_Template)
	defsOf := func(members int) TemplateDefs {
		def := &pb.Payload_Template{IsDefinition: proto.Bool(true)}
		for i := range members {
			def.Metrics = append(def.Metrics, &pb.Payload_Metric{Name: proto.String("m" + strconv.Itoa(i)), Datatype: &dt})
			def.Parameters = append(def.Parameters, &pb.Payload_Template_Parameter{Name: proto.String("p" + strconv.Itoa(i)), Type: &dt})
		}
		defs, err := NewTemplateDefs([]*pb.Payload_Metric{{Name: proto.String("D"), Datatype: &tdt, Value: &pb.Payload_Metric_TemplateValue{TemplateValue: def}}})
		if err != nil {
			t.Fatal(err)
		}
		return defs
	}
	data := []*pb.Payload_Metric{{Name: proto.String("i"), Datatype: &tdt, Value: &pb.Payload_Metric_TemplateValue{TemplateValue: &pb.Payload_Template{
		IsDefinition: proto.Bool(false), TemplateRef: proto.String("D"),
		Metrics:    []*pb.Payload_Metric{{Name: proto.String("m0"), Datatype: &dt}},
		Parameters: []*pb.Payload_Template_Parameter{{Name: proto.String("p0"), Type: &dt}},
	}}}}
	allocs := func(defs TemplateDefs) float64 {
		return testing.AllocsPerRun(20, func() {
			if err := defs.Check(data, false); err != nil {
				t.Fatal(err)
			}
		})
	}
	small, large := defsOf(1), defsOf(10000)
	if s, l := allocs(small), allocs(large); l > s {
		t.Errorf("Check against a definition of 10000 members: %v allocations, of 1 member: %v", l, s)
	}
}

// A definition changed after NewTemplateDefs is not checked against the
// names it had then when it has more or fewer members or parameters.
func TestTemplateCheckDefinitionChanged(t *testing.T) {
	def, inst := templateFixture(t)
	defs, err := NewTemplateDefs([]*pb.Payload_Metric{def})
	if err != nil {
		t.Fatal(err)
	}
	extra, _ := NewMetric("volts", pb.DataType_Double, 0.0)
	tv := def.GetTemplateValue()
	tv.Metrics = append(tv.Metrics, extra)
	data, _ := NewMetric("volts", pb.DataType_Double, 230.0)
	it := inst.GetTemplateValue()
	it.Metrics = []*pb.Payload_Metric{data}
	if err := defs.Check([]*pb.Payload_Metric{inst}, false); err != nil {
		t.Fatal(err)
	}
}

// Sibling paths do not share an array: one does not change the other.
func TestTemplateDeeperCopiesPath(t *testing.T) {
	path := make([]string, 1, 4)
	path[0] = "root"
	a, err := deeper(path, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := deeper(path, "b"); err != nil {
		t.Fatal(err)
	}
	if label(a) != "root/a" {
		t.Errorf("path %q, want root/a", label(a))
	}
}
