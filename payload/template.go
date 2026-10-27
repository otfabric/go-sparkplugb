// SPDX-License-Identifier: MIT

package payload

import (
	pb "github.com/otfabric/go-sparkplugb/proto"
)

// NewTemplateDefinition creates a template definition metric.
// A definition has is_definition=true, a version, and member metrics.
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

// NewTemplateParameter creates a template parameter with the given name, type, and value.
func NewTemplateParameter(name string, dt pb.DataType, v any) (*pb.Payload_Template_Parameter, error) {
	t := uint32(dt)
	p := &pb.Payload_Template_Parameter{
		Name: &name,
		Type: &t,
	}

	switch dt {
	case pb.DataType_Int8, pb.DataType_Int16, pb.DataType_Int32,
		pb.DataType_UInt8, pb.DataType_UInt16, pb.DataType_UInt32:
		val, ok := v.(uint32)
		if !ok {
			return nil, ErrTypeMismatch
		}
		p.Value = &pb.Payload_Template_Parameter_IntValue{IntValue: val}
	case pb.DataType_Int64, pb.DataType_UInt64:
		val, ok := v.(uint64)
		if !ok {
			return nil, ErrTypeMismatch
		}
		p.Value = &pb.Payload_Template_Parameter_LongValue{LongValue: val}
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

// GetTemplateParameterValue extracts the Go value from a template parameter.
func GetTemplateParameterValue(p *pb.Payload_Template_Parameter) (any, error) {
	if p == nil {
		return nil, ErrNilMetric
	}

	dt := pb.DataType(p.GetType())
	switch dt {
	case pb.DataType_Int8, pb.DataType_Int16, pb.DataType_Int32,
		pb.DataType_UInt8, pb.DataType_UInt16, pb.DataType_UInt32:
		return p.GetIntValue(), nil
	case pb.DataType_Int64, pb.DataType_UInt64:
		return p.GetLongValue(), nil
	case pb.DataType_Float:
		return p.GetFloatValue(), nil
	case pb.DataType_Double:
		return p.GetDoubleValue(), nil
	case pb.DataType_Boolean:
		return p.GetBooleanValue(), nil
	case pb.DataType_String, pb.DataType_Text:
		return p.GetStringValue(), nil
	default:
		return nil, ErrUnknownType
	}
}
