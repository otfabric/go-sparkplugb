// SPDX-License-Identifier: MIT

package payload

import (
	"encoding/binary"
	"errors"
	"math"
	"time"

	pb "github.com/otfabric/go-sparkplugb/proto"
)

var (
	ErrTypeMismatch  = errors.New("sparkplugb: datatype mismatch")
	ErrInvalidArray  = errors.New("sparkplugb: invalid array encoding")
	ErrUnknownType   = errors.New("sparkplugb: unknown datatype")
	ErrNilMetric     = errors.New("sparkplugb: nil metric")
	ErrArrayTooLarge = errors.New("sparkplugb: array exceeds maximum size")
	// ErrStringTooLarge is returned by GetValue for a string above MaxStringBytes.
	ErrStringTooLarge = errors.New("sparkplugb: string exceeds maximum size")
)

const (
	// MaxArrayElements bounds the elements of a decoded BooleanArray or StringArray.
	MaxArrayElements = 1 << 20 // 1M elements
	// MaxStringBytes bounds a string returned by GetValue, scalar or array element.
	MaxStringBytes = 1 << 24 // 16 MiB
)

type DataType = pb.DataType

const (
	Unknown         = pb.DataType_Unknown
	Int8            = pb.DataType_Int8
	Int16           = pb.DataType_Int16
	Int32           = pb.DataType_Int32
	Int64           = pb.DataType_Int64
	UInt8           = pb.DataType_UInt8
	UInt16          = pb.DataType_UInt16
	UInt32          = pb.DataType_UInt32
	UInt64          = pb.DataType_UInt64
	Float           = pb.DataType_Float
	Double          = pb.DataType_Double
	Boolean         = pb.DataType_Boolean
	String          = pb.DataType_String
	DateTime        = pb.DataType_DateTime
	Text            = pb.DataType_Text
	UUID            = pb.DataType_UUID
	DataSet         = pb.DataType_DataSet
	Bytes           = pb.DataType_Bytes
	File            = pb.DataType_File
	Template        = pb.DataType_Template
	PropertySet     = pb.DataType_PropertySet
	PropertySetList = pb.DataType_PropertySetList
	Int8Array       = pb.DataType_Int8Array
	Int16Array      = pb.DataType_Int16Array
	Int32Array      = pb.DataType_Int32Array
	Int64Array      = pb.DataType_Int64Array
	UInt8Array      = pb.DataType_UInt8Array
	UInt16Array     = pb.DataType_UInt16Array
	UInt32Array     = pb.DataType_UInt32Array
	UInt64Array     = pb.DataType_UInt64Array
	FloatArray      = pb.DataType_FloatArray
	DoubleArray     = pb.DataType_DoubleArray
	BooleanArray    = pb.DataType_BooleanArray
	StringArray     = pb.DataType_StringArray
	DateTimeArray   = pb.DataType_DateTimeArray
)

func GetValue(m *pb.Payload_Metric) (any, error) {
	if m == nil {
		return nil, ErrNilMetric
	}
	if m.IsNull != nil && *m.IsNull {
		return nil, nil
	}

	dt := pb.DataType(m.GetDatatype())
	switch dt {
	case pb.DataType_Int8:
		return int8(int32(integer(m))), nil
	case pb.DataType_Int16:
		return int16(int32(integer(m))), nil
	case pb.DataType_Int32:
		return int32(integer(m)), nil
	case pb.DataType_Int64:
		return int64(integer(m)), nil
	case pb.DataType_UInt8:
		return uint8(integer(m)), nil
	case pb.DataType_UInt16:
		return uint16(integer(m)), nil
	case pb.DataType_UInt32:
		return uint32(integer(m)), nil
	case pb.DataType_UInt64:
		return integer(m), nil
	case pb.DataType_Float:
		return m.GetFloatValue(), nil
	case pb.DataType_Double:
		return m.GetDoubleValue(), nil
	case pb.DataType_Boolean:
		return m.GetBooleanValue(), nil
	case pb.DataType_String, pb.DataType_Text, pb.DataType_UUID:
		if len(m.GetStringValue()) > MaxStringBytes {
			return nil, ErrStringTooLarge
		}
		return m.GetStringValue(), nil
	case pb.DataType_DateTime:
		ms := integer(m)
		return time.UnixMilli(int64(ms)).UTC(), nil
	case pb.DataType_Bytes, pb.DataType_File:
		return m.GetBytesValue(), nil
	case pb.DataType_DataSet:
		return m.GetDatasetValue(), nil
	case pb.DataType_Template:
		return m.GetTemplateValue(), nil

	// Array types
	case pb.DataType_Int8Array:
		return decodeInt8Array(m.GetBytesValue())
	case pb.DataType_Int16Array:
		return decodeInt16Array(m.GetBytesValue())
	case pb.DataType_Int32Array:
		return decodeInt32Array(m.GetBytesValue())
	case pb.DataType_Int64Array:
		return decodeInt64Array(m.GetBytesValue())
	case pb.DataType_UInt8Array:
		b := m.GetBytesValue()
		out := make([]uint8, len(b))
		copy(out, b)
		return out, nil
	case pb.DataType_UInt16Array:
		return decodeUInt16Array(m.GetBytesValue())
	case pb.DataType_UInt32Array:
		return decodeUInt32Array(m.GetBytesValue())
	case pb.DataType_UInt64Array:
		return decodeUInt64Array(m.GetBytesValue())
	case pb.DataType_FloatArray:
		return decodeFloatArray(m.GetBytesValue())
	case pb.DataType_DoubleArray:
		return decodeDoubleArray(m.GetBytesValue())
	case pb.DataType_BooleanArray:
		return decodeBooleanArray(m.GetBytesValue())
	case pb.DataType_StringArray:
		return decodeStringArray(m.GetBytesValue())
	case pb.DataType_DateTimeArray:
		return decodeDateTimeArray(m.GetBytesValue())

	default:
		return nil, ErrUnknownType
	}
}

// integer returns an integer metric value from whichever of int_value and
// long_value carries it. Sparkplug puts 8- to 32-bit types in int_value, but
// Eclipse Tahu (Java and JavaScript) encodes UInt32 in long_value.
func integer(m *pb.Payload_Metric) uint64 {
	if v, ok := m.GetValue().(*pb.Payload_Metric_LongValue); ok {
		return v.LongValue
	}
	return uint64(m.GetIntValue())
}

func SetValue(m *pb.Payload_Metric, dt pb.DataType, v any) error {
	if m == nil {
		return ErrNilMetric
	}
	d := uint32(dt)
	m.Datatype = &d

	if v == nil {
		t := true
		m.IsNull = &t
		return nil
	}
	f := false
	m.IsNull = &f

	switch dt {
	case pb.DataType_Int8:
		val, ok := v.(int8)
		if !ok {
			return ErrTypeMismatch
		}
		u := uint32(int32(val))
		m.Value = &pb.Payload_Metric_IntValue{IntValue: u}
	case pb.DataType_Int16:
		val, ok := v.(int16)
		if !ok {
			return ErrTypeMismatch
		}
		u := uint32(int32(val))
		m.Value = &pb.Payload_Metric_IntValue{IntValue: u}
	case pb.DataType_Int32:
		val, ok := v.(int32)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_IntValue{IntValue: uint32(val)}
	case pb.DataType_Int64:
		val, ok := v.(int64)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_LongValue{LongValue: uint64(val)}
	case pb.DataType_UInt8:
		val, ok := v.(uint8)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_IntValue{IntValue: uint32(val)}
	case pb.DataType_UInt16:
		val, ok := v.(uint16)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_IntValue{IntValue: uint32(val)}
	case pb.DataType_UInt32:
		val, ok := v.(uint32)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_IntValue{IntValue: val}
	case pb.DataType_UInt64:
		val, ok := v.(uint64)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_LongValue{LongValue: val}
	case pb.DataType_Float:
		val, ok := v.(float32)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_FloatValue{FloatValue: val}
	case pb.DataType_Double:
		val, ok := v.(float64)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_DoubleValue{DoubleValue: val}
	case pb.DataType_Boolean:
		val, ok := v.(bool)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BooleanValue{BooleanValue: val}
	case pb.DataType_String, pb.DataType_Text, pb.DataType_UUID:
		val, ok := v.(string)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_StringValue{StringValue: val}
	case pb.DataType_DateTime:
		val, ok := v.(time.Time)
		if !ok {
			return ErrTypeMismatch
		}
		ms := uint64(val.UnixMilli())
		m.Value = &pb.Payload_Metric_LongValue{LongValue: ms}
	case pb.DataType_Bytes, pb.DataType_File:
		val, ok := v.([]byte)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: val}
	case pb.DataType_DataSet:
		val, ok := v.(*pb.Payload_DataSet)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_DatasetValue{DatasetValue: val}
	case pb.DataType_Template:
		val, ok := v.(*pb.Payload_Template)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_TemplateValue{TemplateValue: val}

	// Array types
	case pb.DataType_Int8Array:
		val, ok := v.([]int8)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: encodeInt8Array(val)}
	case pb.DataType_Int16Array:
		val, ok := v.([]int16)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: encodeInt16Array(val)}
	case pb.DataType_Int32Array:
		val, ok := v.([]int32)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: encodeInt32Array(val)}
	case pb.DataType_Int64Array:
		val, ok := v.([]int64)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: encodeInt64Array(val)}
	case pb.DataType_UInt8Array:
		val, ok := v.([]uint8)
		if !ok {
			return ErrTypeMismatch
		}
		out := make([]byte, len(val))
		copy(out, val)
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: out}
	case pb.DataType_UInt16Array:
		val, ok := v.([]uint16)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: encodeUInt16Array(val)}
	case pb.DataType_UInt32Array:
		val, ok := v.([]uint32)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: encodeUInt32Array(val)}
	case pb.DataType_UInt64Array:
		val, ok := v.([]uint64)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: encodeUInt64Array(val)}
	case pb.DataType_FloatArray:
		val, ok := v.([]float32)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: encodeFloatArray(val)}
	case pb.DataType_DoubleArray:
		val, ok := v.([]float64)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: encodeDoubleArray(val)}
	case pb.DataType_BooleanArray:
		val, ok := v.([]bool)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: encodeBooleanArray(val)}
	case pb.DataType_StringArray:
		val, ok := v.([]string)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: encodeStringArray(val)}
	case pb.DataType_DateTimeArray:
		val, ok := v.([]time.Time)
		if !ok {
			return ErrTypeMismatch
		}
		m.Value = &pb.Payload_Metric_BytesValue{BytesValue: encodeDateTimeArray(val)}

	default:
		return ErrUnknownType
	}
	return nil
}

// Array encoding/decoding — all numeric arrays are little-endian packed.

func decodeInt8Array(b []byte) ([]int8, error) {
	out := make([]int8, len(b))
	for i, v := range b {
		out[i] = int8(v)
	}
	return out, nil
}

func encodeInt8Array(v []int8) []byte {
	out := make([]byte, len(v))
	for i, val := range v {
		out[i] = byte(val)
	}
	return out
}

func decodeInt16Array(b []byte) ([]int16, error) {
	if len(b)%2 != 0 {
		return nil, ErrInvalidArray
	}
	out := make([]int16, len(b)/2)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
	}
	return out, nil
}

func encodeInt16Array(v []int16) []byte {
	out := make([]byte, len(v)*2)
	for i, val := range v {
		binary.LittleEndian.PutUint16(out[i*2:], uint16(val))
	}
	return out
}

func decodeInt32Array(b []byte) ([]int32, error) {
	if len(b)%4 != 0 {
		return nil, ErrInvalidArray
	}
	out := make([]int32, len(b)/4)
	for i := range out {
		out[i] = int32(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return out, nil
}

func encodeInt32Array(v []int32) []byte {
	out := make([]byte, len(v)*4)
	for i, val := range v {
		binary.LittleEndian.PutUint32(out[i*4:], uint32(val))
	}
	return out
}

func decodeInt64Array(b []byte) ([]int64, error) {
	if len(b)%8 != 0 {
		return nil, ErrInvalidArray
	}
	out := make([]int64, len(b)/8)
	for i := range out {
		out[i] = int64(binary.LittleEndian.Uint64(b[i*8:]))
	}
	return out, nil
}

func encodeInt64Array(v []int64) []byte {
	out := make([]byte, len(v)*8)
	for i, val := range v {
		binary.LittleEndian.PutUint64(out[i*8:], uint64(val))
	}
	return out
}

func decodeUInt16Array(b []byte) ([]uint16, error) {
	if len(b)%2 != 0 {
		return nil, ErrInvalidArray
	}
	out := make([]uint16, len(b)/2)
	for i := range out {
		out[i] = binary.LittleEndian.Uint16(b[i*2:])
	}
	return out, nil
}

func encodeUInt16Array(v []uint16) []byte {
	out := make([]byte, len(v)*2)
	for i, val := range v {
		binary.LittleEndian.PutUint16(out[i*2:], val)
	}
	return out
}

func decodeUInt32Array(b []byte) ([]uint32, error) {
	if len(b)%4 != 0 {
		return nil, ErrInvalidArray
	}
	out := make([]uint32, len(b)/4)
	for i := range out {
		out[i] = binary.LittleEndian.Uint32(b[i*4:])
	}
	return out, nil
}

func encodeUInt32Array(v []uint32) []byte {
	out := make([]byte, len(v)*4)
	for i, val := range v {
		binary.LittleEndian.PutUint32(out[i*4:], val)
	}
	return out
}

func decodeUInt64Array(b []byte) ([]uint64, error) {
	if len(b)%8 != 0 {
		return nil, ErrInvalidArray
	}
	out := make([]uint64, len(b)/8)
	for i := range out {
		out[i] = binary.LittleEndian.Uint64(b[i*8:])
	}
	return out, nil
}

func encodeUInt64Array(v []uint64) []byte {
	out := make([]byte, len(v)*8)
	for i, val := range v {
		binary.LittleEndian.PutUint64(out[i*8:], val)
	}
	return out
}

func decodeFloatArray(b []byte) ([]float32, error) {
	if len(b)%4 != 0 {
		return nil, ErrInvalidArray
	}
	out := make([]float32, len(b)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return out, nil
}

func encodeFloatArray(v []float32) []byte {
	out := make([]byte, len(v)*4)
	for i, val := range v {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(val))
	}
	return out
}

func decodeDoubleArray(b []byte) ([]float64, error) {
	if len(b)%8 != 0 {
		return nil, ErrInvalidArray
	}
	out := make([]float64, len(b)/8)
	for i := range out {
		out[i] = math.Float64frombits(binary.LittleEndian.Uint64(b[i*8:]))
	}
	return out, nil
}

func encodeDoubleArray(v []float64) []byte {
	out := make([]byte, len(v)*8)
	for i, val := range v {
		binary.LittleEndian.PutUint64(out[i*8:], math.Float64bits(val))
	}
	return out
}

func decodeBooleanArray(b []byte) ([]bool, error) {
	if len(b) < 4 {
		return nil, ErrInvalidArray
	}
	count := int(binary.LittleEndian.Uint32(b[:4]))
	if count > MaxArrayElements {
		return nil, ErrArrayTooLarge
	}
	bits := b[4:]
	needed := (count + 7) / 8
	if len(bits) < needed {
		return nil, ErrInvalidArray
	}
	out := make([]bool, count)
	for i := range out {
		out[i] = bits[i/8]&(1<<uint(i%8)) != 0
	}
	return out, nil
}

func encodeBooleanArray(v []bool) []byte {
	byteCount := (len(v) + 7) / 8
	out := make([]byte, 4+byteCount)
	binary.LittleEndian.PutUint32(out[:4], uint32(len(v)))
	for i, val := range v {
		if val {
			out[4+i/8] |= 1 << uint(i%8)
		}
	}
	return out
}

// StringArray: null-terminated strings packed into bytes_value.
func decodeStringArray(b []byte) ([]string, error) {
	if len(b) == 0 {
		return []string{}, nil
	}
	var out []string
	add := func(s []byte) error {
		if len(s) > MaxStringBytes {
			return ErrStringTooLarge
		}
		if len(out) == MaxArrayElements {
			return ErrArrayTooLarge
		}
		out = append(out, string(s))
		return nil
	}
	start := 0
	for i, c := range b {
		if c == 0 {
			if err := add(b[start:i]); err != nil {
				return nil, err
			}
			start = i + 1
		}
	}
	if start < len(b) {
		if err := add(b[start:]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func encodeStringArray(v []string) []byte {
	if len(v) == 0 {
		return nil
	}
	var size int
	for _, s := range v {
		size += len(s) + 1
	}
	out := make([]byte, 0, size)
	for _, s := range v {
		out = append(out, s...)
		out = append(out, 0)
	}
	return out
}

func decodeDateTimeArray(b []byte) ([]time.Time, error) {
	if len(b)%8 != 0 {
		return nil, ErrInvalidArray
	}
	out := make([]time.Time, len(b)/8)
	for i := range out {
		ms := binary.LittleEndian.Uint64(b[i*8:])
		out[i] = time.UnixMilli(int64(ms)).UTC()
	}
	return out, nil
}

func encodeDateTimeArray(v []time.Time) []byte {
	out := make([]byte, len(v)*8)
	for i, t := range v {
		binary.LittleEndian.PutUint64(out[i*8:], uint64(t.UnixMilli()))
	}
	return out
}
