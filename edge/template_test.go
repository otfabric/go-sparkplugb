// SPDX-License-Identifier: MIT

package edge

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"google.golang.org/protobuf/proto"
)

func motorTemplate(t *testing.T) (def *pb.Payload_Metric, inst func(name string, rpm, amps float64) *pb.Payload_Metric) {
	t.Helper()
	member := func(name string, v float64) *pb.Payload_Metric {
		m, err := payload.NewMetric(name, pb.DataType_Double, v)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	def, err := payload.NewTemplateDefinition("Motor", "1.0",
		[]*pb.Payload_Metric{member("rpm", 0), member("amps", 0)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	inst = func(name string, rpm, amps float64) *pb.Payload_Metric {
		m, err := payload.NewTemplateInstance(name, "Motor",
			[]*pb.Payload_Metric{member("rpm", rpm), member("amps", amps)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	return def, inst
}

func connectedNode(t *testing.T) (*Node, *mockTransport) {
	t.Helper()
	tr := newMockTransport()
	n, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	return n, tr
}

// Sparkplug 3.0.0 tck-id-topics-nbirth-templates: the definitions of every
// instance an edge node and its devices publish are in the NBIRTH.
func TestTemplateDefinitionsInNBIRTH(t *testing.T) {
	ctx := context.Background()
	def, inst := motorTemplate(t)

	n, tr := connectedNode(t)
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{inst("M1", 1, 2)}); !errors.Is(err, payload.ErrUnknownTemplate) {
		t.Fatalf("instance without definition in NBIRTH: %v", err)
	}
	if n.born || len(tr.getPublished()) != 0 {
		t.Fatal("a refused NBIRTH was published or left the node born")
	}
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{def, inst("M1", 1, 2)}); err != nil {
		t.Fatal(err)
	}

	// Instances of devices and data refer to the NBIRTH definitions.
	if err := n.PublishDeviceBirth(ctx, "d1", []*pb.Payload_Metric{inst("M2", 3, 4)}); err != nil {
		t.Fatal(err)
	}
	if err := n.PublishDeviceData(ctx, "d1", []*pb.Payload_Metric{inst("M2", 5, 6)}); err != nil {
		t.Fatal(err)
	}
	if err := n.PublishNodeData(ctx, []*pb.Payload_Metric{inst("M1", 7, 8)}); err != nil {
		t.Fatal(err)
	}

	// tck-id-payloads-template-definition-nbirth-only.
	if err := n.PublishDeviceBirth(ctx, "d2", []*pb.Payload_Metric{def}); !errors.Is(err, payload.ErrInvalidTemplate) {
		t.Fatalf("definition in DBIRTH: %v", err)
	}
	if err := n.PublishNodeData(ctx, []*pb.Payload_Metric{def}); !errors.Is(err, payload.ErrInvalidTemplate) {
		t.Fatalf("definition in NDATA: %v", err)
	}
	if err := n.PublishDeviceData(ctx, "d1", []*pb.Payload_Metric{def}); !errors.Is(err, payload.ErrInvalidTemplate) {
		t.Fatalf("definition in DDATA: %v", err)
	}

	// An instance of a definition the NBIRTH does not have.
	pump, err := payload.NewTemplateInstance("P1", "Pump", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.PublishDeviceBirth(ctx, "d2", []*pb.Payload_Metric{pump}); !errors.Is(err, payload.ErrUnknownTemplate) {
		t.Fatalf("unknown definition in DBIRTH: %v", err)
	}
	if err := n.PublishNodeData(ctx, []*pb.Payload_Metric{pump}); !errors.Is(err, payload.ErrUnknownTemplate) {
		t.Fatalf("unknown definition in NDATA: %v", err)
	}
	if err := n.PublishDeviceData(ctx, "d1", []*pb.Payload_Metric{pump}); !errors.Is(err, payload.ErrUnknownTemplate) {
		t.Fatalf("unknown definition in DDATA: %v", err)
	}

	// A DBIRTH instance must have all members of the definition.
	partial := inst("M3", 1, 1)
	partial.GetTemplateValue().Metrics = partial.GetTemplateValue().Metrics[:1]
	if err := n.PublishDeviceBirth(ctx, "d2", []*pb.Payload_Metric{partial}); !errors.Is(err, payload.ErrInvalidTemplate) {
		t.Fatalf("partial instance in DBIRTH: %v", err)
	}

	// Nothing refused was published, and refused messages took no seq.
	pubs := tr.getPublished()
	if len(pubs) != 4 { // NBIRTH, DBIRTH, DDATA, NDATA
		t.Fatalf("%d messages published", len(pubs))
	}
	if n.Seq() != 3 {
		t.Fatalf("seq %d after three messages after the NBIRTH", n.Seq())
	}
	if _, ok := n.devices["d2"]; ok {
		t.Fatal("a refused DBIRTH registered its device")
	}
}

// A new NBIRTH replaces the definitions: an instance of one it dropped is
// refused afterwards.
func TestTemplateDefinitionsFollowNBIRTH(t *testing.T) {
	ctx := context.Background()
	def, inst := motorTemplate(t)
	n, _ := connectedNode(t)
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{def}); err != nil {
		t.Fatal(err)
	}
	scalar, _ := payload.NewMetric("temp", pb.DataType_Double, 1.0)
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{scalar}); err != nil {
		t.Fatal(err)
	}
	err := n.PublishNodeData(ctx, []*pb.Payload_Metric{inst("M1", 1, 2)})
	if !errors.Is(err, payload.ErrUnknownTemplate) {
		t.Fatalf("instance after its definition was dropped: %v", err)
	}
	if !strings.Contains(err.Error(), `"M1"`) || !strings.Contains(err.Error(), `"Motor"`) {
		t.Errorf("error %q names neither instance nor definition", err)
	}
}

// Regression: an NDATA with a subset of an instance's members replaced the
// instance in the stored birth, so the next rebirth published an NBIRTH
// instance without all members of its definition.
func TestRebirthKeepsTemplateMembers(t *testing.T) {
	ctx := context.Background()
	def, inst := motorTemplate(t)
	n, tr := connectedNode(t)
	if err := n.PublishBirth(ctx, []*pb.Payload_Metric{def, inst("M1", 1, 2)}); err != nil {
		t.Fatal(err)
	}
	if err := n.PublishDeviceBirth(ctx, "d1", []*pb.Payload_Metric{inst("M2", 3, 4)}); err != nil {
		t.Fatal(err)
	}
	upd := inst("M1", 1500, 0)
	upd.GetTemplateValue().Metrics = upd.GetTemplateValue().Metrics[:1] // rpm only
	upd.GetTemplateValue().GetMetrics()[0].Datatype = nil
	if err := n.PublishNodeData(ctx, []*pb.Payload_Metric{upd}); err != nil {
		t.Fatal(err)
	}
	dupd := proto.Clone(upd).(*pb.Payload_Metric)
	dupd.Name = proto.String("M2")
	if err := n.PublishDeviceData(ctx, "d1", []*pb.Payload_Metric{dupd}); err != nil {
		t.Fatal(err)
	}
	if err := n.Rebirth(ctx); err != nil {
		t.Fatalf("rebirth: %v", err)
	}

	pubs := tr.getPublished()
	for _, want := range []struct {
		topic, metric string
	}{{"/NBIRTH/", "M1"}, {"/DBIRTH/", "M2"}} {
		var last publishedMsg
		for _, p := range pubs {
			if strings.Contains(p.Topic, want.topic) {
				last = p
			}
		}
		p, err := payload.Decode(last.Payload)
		if err != nil {
			t.Fatal(err)
		}
		var got *pb.Payload_Template
		for _, m := range p.GetMetrics() {
			if m.GetName() == want.metric {
				got = m.GetTemplateValue()
			}
		}
		if got == nil || len(got.GetMetrics()) != 2 {
			t.Fatalf("rebirth %s instance %s: %v", want.topic, want.metric, got)
		}
		for _, m := range got.GetMetrics() {
			if m.GetDatatype() != uint32(pb.DataType_Double) {
				t.Errorf("member %s lost its datatype", m.GetName())
			}
			v, _ := payload.GetValue(m)
			switch m.GetName() {
			case "rpm":
				if v != 1500.0 {
					t.Errorf("%s rpm %v, want the NDATA value", want.metric, v)
				}
			case "amps":
				if v == 0.0 {
					t.Errorf("%s amps took the value of a member the NDATA did not have", want.metric)
				}
			}
		}
		if got.GetTemplateRef() != "Motor" || got.IsDefinition == nil || got.GetIsDefinition() {
			t.Errorf("rebirth instance %v", got)
		}
	}
}
