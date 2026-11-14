// SPDX-License-Identifier: MIT

package payload

import (
	"errors"
	"runtime"
	"strings"
	"testing"

	pb "github.com/otfabric/go-sparkplugb/proto"
	"google.golang.org/protobuf/proto"
)

// nestedDefinition is a template definition metric with templates nested
// depth deep: the definition itself is at depth 1, and each level has one
// member, a definition, named name.
func nestedDefinition(depth int, name string) *pb.Payload_Metric {
	dt := uint32(pb.DataType_Template)
	t := &pb.Payload_Template{IsDefinition: proto.Bool(true)}
	for i := 1; i < depth; i++ {
		member := &pb.Payload_Metric{Name: proto.String(name), Datatype: &dt, Value: &pb.Payload_Metric_TemplateValue{TemplateValue: t}}
		t = &pb.Payload_Template{IsDefinition: proto.Bool(true), Metrics: []*pb.Payload_Metric{member}}
	}
	return &pb.Payload_Metric{Name: proto.String(name), Datatype: &dt, Value: &pb.Payload_Metric_TemplateValue{TemplateValue: t}}
}

// recursiveDefinition is definition "D", whose member "m" is an instance of
// "D" without members.
func recursiveDefinition() *pb.Payload_Metric {
	dt := uint32(pb.DataType_Template)
	inner := &pb.Payload_Template{IsDefinition: proto.Bool(false), TemplateRef: proto.String("D")}
	m := &pb.Payload_Metric{Name: proto.String("m"), Datatype: &dt, Value: &pb.Payload_Metric_TemplateValue{TemplateValue: inner}}
	return &pb.Payload_Metric{Name: proto.String("D"), Datatype: &dt, Value: &pb.Payload_Metric_TemplateValue{
		TemplateValue: &pb.Payload_Template{IsDefinition: proto.Bool(true), Metrics: []*pb.Payload_Metric{m}},
	}}
}

// nestedInstance is an instance of "D" with instances of "D" nested depth
// deep through member "m".
func nestedInstance(depth int) *pb.Payload_Metric {
	dt := uint32(pb.DataType_Template)
	t := &pb.Payload_Template{IsDefinition: proto.Bool(false), TemplateRef: proto.String("D")}
	for i := 1; i < depth; i++ {
		member := &pb.Payload_Metric{Name: proto.String("m"), Datatype: &dt, Value: &pb.Payload_Metric_TemplateValue{TemplateValue: t}}
		t = &pb.Payload_Template{IsDefinition: proto.Bool(false), TemplateRef: proto.String("D"), Metrics: []*pb.Payload_Metric{member}}
	}
	return &pb.Payload_Metric{Name: proto.String("i"), Datatype: &dt, Value: &pb.Payload_Metric_TemplateValue{TemplateValue: t}}
}

func TestTemplateNestingDepth(t *testing.T) {
	for _, tc := range []struct {
		depth int
		ok    bool
	}{
		{1, true},
		{2, true},
		{maxTemplateDepth - 1, true},
		{maxTemplateDepth, true},
		{maxTemplateDepth + 1, false},
		{maxTemplateDepth + 2, false},
		{1000, false},
	} {
		check := func(what string, err error) {
			t.Helper()
			if tc.ok && err != nil {
				t.Errorf("%s nested %d deep: %v", what, tc.depth, err)
			}
			if !tc.ok && !errors.Is(err, ErrInvalidTemplate) {
				t.Errorf("%s nested %d deep: %v, want ErrInvalidTemplate", what, tc.depth, err)
			}
		}
		_, err := NewTemplateDefs([]*pb.Payload_Metric{nestedDefinition(tc.depth, "d")})
		check("definitions", err)

		defs, err := NewTemplateDefs([]*pb.Payload_Metric{recursiveDefinition()})
		if err != nil {
			t.Fatal(err)
		}
		check("instances in data", defs.Check([]*pb.Payload_Metric{nestedInstance(tc.depth)}, false))
		_, err = NewTemplateDefs([]*pb.Payload_Metric{recursiveDefinition(), nestedInstance(tc.depth)})
		if !tc.ok {
			check("instances in the NBIRTH", err)
		}
	}
}

// allocated returns the bytes f allocates, the least of three runs.
func allocated(f func()) uint64 {
	least := ^uint64(0)
	for range 3 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		f()
		runtime.ReadMemStats(&after)
		least = min(least, after.TotalAlloc-before.TotalAlloc)
	}
	return least
}

// The template checks allocate in proportion to what they check, not to the
// square of a nesting or of a number of instances.
func TestTemplateChecksAllocationBudget(t *testing.T) {
	const budget = 1 << 20

	// Long names, nested as deep as allowed.
	long := strings.Repeat("n", 64<<10)
	deep := []*pb.Payload_Metric{nestedDefinition(maxTemplateDepth, long)}
	if n := allocated(func() {
		if _, err := NewTemplateDefs(deep); err != nil {
			t.Error(err)
		}
	}); n > budget {
		t.Errorf("definitions nested %d deep with names of %d bytes: %d bytes allocated, want at most %d", maxTemplateDepth, len(long), n, budget)
	}

	// Short names, nested far deeper than allowed.
	deeper := []*pb.Payload_Metric{nestedDefinition(2000, strings.Repeat("n", 100))}
	if n := allocated(func() {
		if _, err := NewTemplateDefs(deeper); !errors.Is(err, ErrInvalidTemplate) {
			t.Error(err)
		}
	}); n > budget {
		t.Errorf("definitions nested 2000 deep: %d bytes allocated, want at most %d", n, budget)
	}

	// Many instances of a definition with many members.
	const members, instances = 2000, 2000
	dt := uint32(pb.DataType_Int32)
	def := &pb.Payload_Template{IsDefinition: proto.Bool(true)}
	for i := range members {
		def.Metrics = append(def.Metrics, &pb.Payload_Metric{Name: proto.String(strings.Repeat("x", 8) + string(rune('a'+i%26)) + strings.Repeat("y", i/26)), Datatype: &dt})
	}
	tdt := uint32(pb.DataType_Template)
	defs := TemplateDefs{"D": def}
	data := make([]*pb.Payload_Metric, instances)
	for i := range data {
		data[i] = &pb.Payload_Metric{Name: proto.String("i"), Datatype: &tdt, Value: &pb.Payload_Metric_TemplateValue{TemplateValue: &pb.Payload_Template{
			IsDefinition: proto.Bool(false), TemplateRef: proto.String("D"),
			Metrics: []*pb.Payload_Metric{{Name: def.Metrics[0].Name, Datatype: &dt}},
		}}}
	}
	if n := allocated(func() {
		if err := defs.Check(data, false); err != nil {
			t.Error(err)
		}
	}); n > budget {
		t.Errorf("%d instances of a definition of %d members: %d bytes allocated, want at most %d", instances, members, n, budget)
	}
}
