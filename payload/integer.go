// SPDX-License-Identifier: MIT

package payload

import (
	"math"
	"time"

	pb "github.com/otfabric/go-sparkplugb/proto"
)

// The integer rules of Metric, DataSet.DataSetValue, Template.Parameter and
// PropertyValue. All four carry integers in the same unsigned fields, int_value
// and long_value, so they share one decoder and one encoder. What is read and
// written, and why, is in INTEROPERABILITY.md under "Integers".

// isIntegerType reports whether a value of dt is carried in int_value or
// long_value.
func isIntegerType(dt pb.DataType) bool {
	switch dt {
	case pb.DataType_Int8, pb.DataType_Int16, pb.DataType_Int32, pb.DataType_Int64,
		pb.DataType_UInt8, pb.DataType_UInt16, pb.DataType_UInt32, pb.DataType_UInt64,
		pb.DataType_DateTime:
		return true
	}
	return false
}

// decodeInteger returns the Go value of dt for a wire value, which is
// zero-extended when it came from int_value. A value that is not one of dt
// returns ErrTypeMismatch: nothing is truncated.
func decodeInteger(dt pb.DataType, v uint64) (any, error) {
	switch dt {
	case pb.DataType_Int8:
		s, err := signedInt(v, 8)
		if err != nil {
			return nil, err
		}
		return int8(s), nil
	case pb.DataType_Int16:
		s, err := signedInt(v, 16)
		if err != nil {
			return nil, err
		}
		return int16(s), nil
	case pb.DataType_Int32:
		s, err := signedInt(v, 32)
		if err != nil {
			return nil, err
		}
		return int32(s), nil
	case pb.DataType_Int64:
		return int64(v), nil
	case pb.DataType_UInt8:
		if v > math.MaxUint8 {
			return nil, ErrTypeMismatch
		}
		return uint8(v), nil
	case pb.DataType_UInt16:
		if v > math.MaxUint16 {
			return nil, ErrTypeMismatch
		}
		return uint16(v), nil
	case pb.DataType_UInt32:
		if v > math.MaxUint32 {
			return nil, ErrTypeMismatch
		}
		return uint32(v), nil
	case pb.DataType_UInt64:
		return v, nil
	case pb.DataType_DateTime:
		t, err := dateTime(v)
		if err != nil {
			return nil, err
		}
		return t, nil
	}
	return nil, ErrUnknownType
}

// signedInt decodes an Int8, Int16 or Int32 of the given width from its wire
// value. Sparkplug carries them in an unsigned field and does not say how a
// negative Int8 or Int16 goes in: Eclipse Tahu sign-extends it (int8(-8) is
// 0xFFFFFFF8), pysparkplug and srad send the width (0xF8). Both are accepted;
// the encoder sign-extends. A value that is neither is not an integer of that
// width, and returns ErrTypeMismatch rather than being truncated.
func signedInt(v uint64, bits uint) (int64, error) {
	shift := 64 - bits
	s := int64(v<<shift) >> shift
	// The width-sized form, the 32-bit sign extension of int_value, and the
	// 64-bit one, which long_value can carry.
	if v < 1<<bits || v == uint64(uint32(s)) || v == uint64(s) {
		return s, nil
	}
	return 0, ErrTypeMismatch
}

// dateTime decodes milliseconds since the epoch. Sparkplug's DateTime is a
// uint64; a time.Time holds them only up to math.MaxInt64.
func dateTime(ms uint64) (time.Time, error) {
	if ms > math.MaxInt64 {
		return time.Time{}, ErrTypeMismatch
	}
	return time.UnixMilli(int64(ms)).UTC(), nil
}

// dateTimeMillis encodes t as milliseconds since the epoch. A time before the
// epoch has no uint64 form, and one too far ahead no int64 number of
// milliseconds: both return ErrTypeMismatch.
func dateTimeMillis(t time.Time) (uint64, error) {
	if t.Before(time.UnixMilli(0)) || t.After(time.UnixMilli(math.MaxInt64)) {
		return 0, ErrTypeMismatch
	}
	return uint64(t.UnixMilli()), nil
}

// encodeInteger returns the wire value of v, which must have the Go type of dt.
// A negative value is sign-extended: to 32 bits for what goes in int_value, to
// 64 for what goes in long_value.
func encodeInteger(dt pb.DataType, v any) (uint64, error) {
	var out uint64
	ok := false
	switch dt {
	case pb.DataType_Int8:
		var x int8
		if x, ok = v.(int8); ok {
			out = uint64(uint32(int32(x)))
		}
	case pb.DataType_Int16:
		var x int16
		if x, ok = v.(int16); ok {
			out = uint64(uint32(int32(x)))
		}
	case pb.DataType_Int32:
		var x int32
		if x, ok = v.(int32); ok {
			out = uint64(uint32(x))
		}
	case pb.DataType_Int64:
		var x int64
		if x, ok = v.(int64); ok {
			out = uint64(x)
		}
	case pb.DataType_UInt8:
		var x uint8
		if x, ok = v.(uint8); ok {
			out = uint64(x)
		}
	case pb.DataType_UInt16:
		var x uint16
		if x, ok = v.(uint16); ok {
			out = uint64(x)
		}
	case pb.DataType_UInt32:
		var x uint32
		if x, ok = v.(uint32); ok {
			out = uint64(x)
		}
	case pb.DataType_UInt64:
		out, ok = v.(uint64)
	case pb.DataType_DateTime:
		x, isTime := v.(time.Time)
		if !isTime {
			return 0, ErrTypeMismatch
		}
		return dateTimeMillis(x)
	default:
		return 0, ErrUnknownType
	}
	if !ok {
		return 0, ErrTypeMismatch
	}
	return out, nil
}

// encodeIntegerOrRaw is encodeInteger for NewDataSet, NewTemplateParameter and
// NewPropertyValue, which also take the wire type of dt, uint32 or uint64. Such a
// value has to be a value of dt, and it is written in the form encodeInteger
// writes.
func encodeIntegerOrRaw(dt pb.DataType, v any) (uint64, error) {
	var raw uint64
	switch x := v.(type) {
	case uint32:
		if usesLongValue(dt) {
			return 0, ErrTypeMismatch
		}
		raw = uint64(x)
	case uint64:
		if !usesLongValue(dt) {
			return 0, ErrTypeMismatch
		}
		raw = x
	default:
		return encodeInteger(dt, v)
	}
	typed, err := decodeInteger(dt, raw)
	if err != nil {
		return 0, err
	}
	return encodeInteger(dt, typed)
}

// usesLongValue reports whether an integer of dt is written to long_value.
func usesLongValue(dt pb.DataType) bool {
	switch dt {
	case pb.DataType_Int64, pb.DataType_UInt64, pb.DataType_DateTime:
		return true
	}
	return false
}
