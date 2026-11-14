// SPDX-License-Identifier: MIT

package payload

import (
	pb "github.com/otfabric/go-sparkplugb/proto"
)

// NewTemplateDefinition creates a template definition metric.
// A definition has is_definition=true, a version, and member metrics.
// The metric keeps metrics and params, and the members and parameters they
// point to, as they are: they are not copied.
func NewTemplateDefinition(name, version string, metrics []*pb.Payload_Metric, params []*pb.Payload_Template_Parameter) (*pb.Payload_Metric, error) {
	isDef := true
	tmpl := &pb.Payload_Template{
		Version:      &version,
		Metrics:      metrics,
		Parameters:   params,
		IsDefinition: &isDef,
	}

	dt := uint32(pb.DataType_Template)
	m := &pb.Payload_Metric{
		Name:     &name,
		Datatype: &dt,
		Value:    &pb.Payload_Metric_TemplateValue{TemplateValue: tmpl},
	}
	return m, nil
}

// NewTemplateInstance creates a template instance metric.
// An instance has template_ref set to the definition name, optional parameter overrides.
// The metric keeps metrics and params as they are: they are not copied.
func NewTemplateInstance(name, templateRef string, metrics []*pb.Payload_Metric, params []*pb.Payload_Template_Parameter) (*pb.Payload_Metric, error) {
	isDef := false
	tmpl := &pb.Payload_Template{
		TemplateRef:  &templateRef,
		Metrics:      metrics,
		Parameters:   params,
		IsDefinition: &isDef,
	}

	dt := uint32(pb.DataType_Template)
	m := &pb.Payload_Metric{
		Name:     &name,
		Datatype: &dt,
		Value:    &pb.Payload_Metric_TemplateValue{TemplateValue: tmpl},
	}
	return m, nil
}

// NewTemplateParameter creates a template parameter with the given name, type,
// and value. The value has the Go type GetValue returns for the datatype. For
// an integer the wire type, uint32 or uint64, is also taken, as long as it is a
// value of the datatype; a value that is not returns ErrTypeMismatch.
func NewTemplateParameter(name string, dt pb.DataType, v any) (*pb.Payload_Template_Parameter, error) {
	t := uint32(dt)
	p := &pb.Payload_Template_Parameter{
		Name: &name,
		Type: &t,
	}

	if isIntegerType(dt) {
		u, err := encodeIntegerOrRaw(dt, v)
		if err != nil {
			return nil, err
		}
		if usesLongValue(dt) {
			p.Value = &pb.Payload_Template_Parameter_LongValue{LongValue: u}
		} else {
			p.Value = &pb.Payload_Template_Parameter_IntValue{IntValue: uint32(u)}
		}
		return p, nil
	}

	switch dt {
	case pb.DataType_Float:
		val, ok := v.(float32)
		if !ok {
			return nil, ErrTypeMismatch
		}
		p.Value = &pb.Payload_Template_Parameter_FloatValue{FloatValue: val}
	case pb.DataType_Double:
		val, ok := v.(float64)
		if !ok {
			return nil, ErrTypeMismatch
		}
		p.Value = &pb.Payload_Template_Parameter_DoubleValue{DoubleValue: val}
	case pb.DataType_Boolean:
		val, ok := v.(bool)
		if !ok {
			return nil, ErrTypeMismatch
		}
		p.Value = &pb.Payload_Template_Parameter_BooleanValue{BooleanValue: val}
	case pb.DataType_String, pb.DataType_Text:
		val, ok := v.(string)
		if !ok {
			return nil, ErrTypeMismatch
		}
		p.Value = &pb.Payload_Template_Parameter_StringValue{StringValue: val}
	default:
		return nil, ErrUnknownType
	}
	return p, nil
}

// GetTemplateParameterValue extracts the Go value from a template parameter. It
// has the Go type GetValue returns for the datatype: integers and DateTime are
// decoded by the rules of GetValue, and one that is not a value of the datatype,
// or a value carried in another field than the datatype's, returns
// ErrTypeMismatch. A parameter without a value reads as the zero value. The
// members of a template are metrics: GetValue reads them.
func GetTemplateParameterValue(p *pb.Payload_Template_Parameter) (any, error) {
	if p == nil {
		return nil, ErrNilMetric
	}

	dt := pb.DataType(p.GetType())
	if isIntegerType(dt) {
		switch v := p.Value.(type) {
		case nil:
			return decodeInteger(dt, 0)
		case *pb.Payload_Template_Parameter_IntValue:
			return decodeInteger(dt, uint64(v.IntValue))
		case *pb.Payload_Template_Parameter_LongValue:
			return decodeInteger(dt, v.LongValue)
		}
		return nil, ErrTypeMismatch
	}

	switch dt {
	case pb.DataType_Float:
		switch v := p.Value.(type) {
		case nil:
			return float32(0), nil
		case *pb.Payload_Template_Parameter_FloatValue:
			return v.FloatValue, nil
		}
	case pb.DataType_Double:
		switch v := p.Value.(type) {
		case nil:
			return float64(0), nil
		case *pb.Payload_Template_Parameter_DoubleValue:
			return v.DoubleValue, nil
		}
	case pb.DataType_Boolean:
		switch v := p.Value.(type) {
		case nil:
			return false, nil
		case *pb.Payload_Template_Parameter_BooleanValue:
			return v.BooleanValue, nil
		}
	case pb.DataType_String, pb.DataType_Text:
		switch v := p.Value.(type) {
		case nil:
			return "", nil
		case *pb.Payload_Template_Parameter_StringValue:
			return v.StringValue, nil
		}
	default:
		return nil, ErrUnknownType
	}
	return nil, ErrTypeMismatch
}
