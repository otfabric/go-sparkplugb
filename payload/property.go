// SPDX-License-Identifier: MIT

package payload

import (
	pb "github.com/otfabric/go-sparkplugb/proto"
)

// NewPropertyValue creates a PropertyValue of the given type. The value has the
// Go type GetValue returns for the datatype, *pb.Payload_PropertySet for a
// PropertySet and *pb.Payload_PropertySetList for a PropertySetList. For an
// integer the wire type, uint32 or uint64, is also taken, as long as it is a
// value of the datatype; a value that is not returns ErrTypeMismatch. A nil
// value sets is_null.
func NewPropertyValue(dt pb.DataType, v any) (*pb.Payload_PropertyValue, error) {
	t := uint32(dt)
	p := &pb.Payload_PropertyValue{Type: &t}
	if v == nil {
		null := true
		p.IsNull = &null
		return p, nil
	}

	if isIntegerType(dt) {
		u, err := encodeIntegerOrRaw(dt, v)
		if err != nil {
			return nil, err
		}
		if usesLongValue(dt) {
			p.Value = &pb.Payload_PropertyValue_LongValue{LongValue: u}
		} else {
			p.Value = &pb.Payload_PropertyValue_IntValue{IntValue: uint32(u)}
		}
		return p, nil
	}

	switch dt {
	case pb.DataType_Float:
		val, ok := v.(float32)
		if !ok {
			return nil, ErrTypeMismatch
		}
		p.Value = &pb.Payload_PropertyValue_FloatValue{FloatValue: val}
	case pb.DataType_Double:
		val, ok := v.(float64)
		if !ok {
			return nil, ErrTypeMismatch
		}
		p.Value = &pb.Payload_PropertyValue_DoubleValue{DoubleValue: val}
	case pb.DataType_Boolean:
		val, ok := v.(bool)
		if !ok {
			return nil, ErrTypeMismatch
		}
		p.Value = &pb.Payload_PropertyValue_BooleanValue{BooleanValue: val}
	case pb.DataType_String, pb.DataType_Text:
		val, ok := v.(string)
		if !ok {
			return nil, ErrTypeMismatch
		}
		p.Value = &pb.Payload_PropertyValue_StringValue{StringValue: val}
	case pb.DataType_PropertySet:
		val, ok := v.(*pb.Payload_PropertySet)
		if !ok {
			return nil, ErrTypeMismatch
		}
		p.Value = &pb.Payload_PropertyValue_PropertysetValue{PropertysetValue: val}
	case pb.DataType_PropertySetList:
		val, ok := v.(*pb.Payload_PropertySetList)
		if !ok {
			return nil, ErrTypeMismatch
		}
		p.Value = &pb.Payload_PropertyValue_PropertysetsValue{PropertysetsValue: val}
	default:
		return nil, ErrUnknownType
	}
	return p, nil
}

// GetPropertyValue extracts the Go value from a PropertyValue. It has the Go
// type GetValue returns for the datatype, and integers and DateTime are decoded
// by the rules of GetValue: one that is not a value of the datatype returns
// ErrTypeMismatch. A PropertyValue with is_null set is nil.
func GetPropertyValue(p *pb.Payload_PropertyValue) (any, error) {
	if p == nil {
		return nil, ErrNilMetric
	}
	if p.GetIsNull() {
		return nil, nil
	}

	dt := pb.DataType(p.GetType())
	if isIntegerType(dt) {
		return propertyInteger(dt, p)
	}

	switch dt {
	case pb.DataType_Float:
		return p.GetFloatValue(), nil
	case pb.DataType_Double:
		return p.GetDoubleValue(), nil
	case pb.DataType_Boolean:
		return p.GetBooleanValue(), nil
	case pb.DataType_String, pb.DataType_Text:
		return p.GetStringValue(), nil
	case pb.DataType_PropertySet:
		return p.GetPropertysetValue(), nil
	case pb.DataType_PropertySetList:
		return p.GetPropertysetsValue(), nil
	default:
		return nil, ErrUnknownType
	}
}

// propertyInteger decodes the integer of p as dt.
func propertyInteger(dt pb.DataType, p *pb.Payload_PropertyValue) (any, error) {
	switch v := p.GetValue().(type) {
	case nil:
		return decodeInteger(dt, 0)
	case *pb.Payload_PropertyValue_IntValue:
		return decodeInteger(dt, uint64(v.IntValue))
	case *pb.Payload_PropertyValue_LongValue:
		return decodeInteger(dt, v.LongValue)
	}
	return nil, ErrTypeMismatch
}
