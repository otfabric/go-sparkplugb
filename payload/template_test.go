// SPDX-License-Identifier: MIT

package payload

import (
	"testing"

	pb "github.com/otfabric/go-sparkplugb/proto"
)

func TestTemplateDefinitionRoundTrip(t *testing.T) {
	tempMetric, err := NewMetric("temperature", pb.DataType_Double, float64(0))
	if err != nil {
		t.Fatal(err)
	}

	param, err := NewTemplateParameter("unit", pb.DataType_String, "celsius")
	if err != nil {
		t.Fatal(err)
	}

	def, err := NewTemplateDefinition("SensorTemplate", "1.0",
		[]*pb.Payload_Metric{tempMetric},
		[]*pb.Payload_Template_Parameter{param})
	if err != nil {
		t.Fatal(err)
	}

	tmpl := def.GetTemplateValue()
	if tmpl == nil {
		t.Fatal("expected template value")
	}
	if tmpl.GetVersion() != "1.0" {
		t.Errorf("version = %q, want 1.0", tmpl.GetVersion())
	}
	if !tmpl.GetIsDefinition() {
		t.Error("expected is_definition = true")
	}
	if tmpl.GetTemplateRef() != "" {
		t.Error("definition should not have template_ref")
	}
	if len(tmpl.GetMetrics()) != 1 {
		t.Errorf("metrics count = %d, want 1", len(tmpl.GetMetrics()))
	}
	if len(tmpl.GetParameters()) != 1 {
		t.Errorf("parameters count = %d, want 1", len(tmpl.GetParameters()))
	}

	// Verify round-trip through encode/decode
	p := NewPayload(0, def)
	b, err := Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	tmpl2 := p2.Metrics[0].GetTemplateValue()
	if tmpl2.GetVersion() != "1.0" {
		t.Errorf("decoded version = %q", tmpl2.GetVersion())
	}
	if !tmpl2.GetIsDefinition() {
		t.Error("decoded is_definition should be true")
	}
}

func TestTemplateInstanceRoundTrip(t *testing.T) {
	tempMetric, err := NewMetric("temperature", pb.DataType_Double, float64(22.5))
	if err != nil {
		t.Fatal(err)
	}

	param, err := NewTemplateParameter("unit", pb.DataType_String, "fahrenheit")
	if err != nil {
		t.Fatal(err)
	}

	inst, err := NewTemplateInstance("sensor1", "SensorTemplate",
		[]*pb.Payload_Metric{tempMetric},
		[]*pb.Payload_Template_Parameter{param})
	if err != nil {
		t.Fatal(err)
	}

	tmpl := inst.GetTemplateValue()
	if tmpl.GetIsDefinition() {
		t.Error("instance should have is_definition = false")
	}
	if tmpl.GetTemplateRef() != "SensorTemplate" {
		t.Errorf("template_ref = %q, want SensorTemplate", tmpl.GetTemplateRef())
	}

	// Encode/decode
	p := NewPayload(0, inst)
	b, err := Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	tmpl2 := p2.Metrics[0].GetTemplateValue()
	if tmpl2.GetTemplateRef() != "SensorTemplate" {
		t.Errorf("decoded template_ref = %q", tmpl2.GetTemplateRef())
	}
}

func TestTemplateParameterTypes(t *testing.T) {
	tests := []struct {
		name string
		dt   pb.DataType
		val  any
	}{
		{"uint32", pb.DataType_UInt32, uint32(42)},
		{"uint64", pb.DataType_UInt64, uint64(100)},
		{"float", pb.DataType_Float, float32(3.14)},
		{"double", pb.DataType_Double, float64(2.718)},
		{"bool", pb.DataType_Boolean, true},
		{"string", pb.DataType_String, "hello"},
		{"text", pb.DataType_Text, "world"},
		{"int8", pb.DataType_Int8, uint32(8)},
		{"int16", pb.DataType_Int16, uint32(16)},
		{"int32", pb.DataType_Int32, uint32(32)},
		{"uint8", pb.DataType_UInt8, uint32(8)},
		{"uint16", pb.DataType_UInt16, uint32(16)},
		{"int64", pb.DataType_Int64, uint64(64)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := NewTemplateParameter(tc.name, tc.dt, tc.val)
			if err != nil {
				t.Fatal(err)
			}
			got, err := GetTemplateParameterValue(p)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.val {
				t.Errorf("got %v, want %v", got, tc.val)
			}
		})
	}
}

func TestTemplateParameterTypeMismatch(t *testing.T) {
	_, err := NewTemplateParameter("x", pb.DataType_UInt32, "not a uint32")
	if err != ErrTypeMismatch {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
	_, err = NewTemplateParameter("x", pb.DataType_UInt64, "not a uint64")
	if err != ErrTypeMismatch {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
	_, err = NewTemplateParameter("x", pb.DataType_Float, "not a float")
	if err != ErrTypeMismatch {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
	_, err = NewTemplateParameter("x", pb.DataType_Double, "not a double")
	if err != ErrTypeMismatch {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
	_, err = NewTemplateParameter("x", pb.DataType_Boolean, "not a bool")
	if err != ErrTypeMismatch {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
	_, err = NewTemplateParameter("x", pb.DataType_String, 42)
	if err != ErrTypeMismatch {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
}

func TestTemplateParameterUnknownType(t *testing.T) {
	_, err := NewTemplateParameter("x", pb.DataType_Bytes, []byte{})
	if err != ErrUnknownType {
		t.Errorf("expected ErrUnknownType, got %v", err)
	}
}

func TestGetTemplateParameterValueNil(t *testing.T) {
	_, err := GetTemplateParameterValue(nil)
	if err != ErrNilMetric {
		t.Errorf("expected ErrNilMetric, got %v", err)
	}
}

func TestGetTemplateParameterValueUnknownType(t *testing.T) {
	dt := uint32(pb.DataType_Bytes)
	p := &pb.Payload_Template_Parameter{Type: &dt}
	_, err := GetTemplateParameterValue(p)
	if err != ErrUnknownType {
		t.Errorf("expected ErrUnknownType, got %v", err)
	}
}

func TestTemplateDefinitionNoParams(t *testing.T) {
	m, _ := NewMetric("x", pb.DataType_Int32, int32(0))
	def, err := NewTemplateDefinition("T", "1.0", []*pb.Payload_Metric{m}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(def.GetTemplateValue().GetParameters()) != 0 {
		t.Error("expected 0 parameters")
	}
}
