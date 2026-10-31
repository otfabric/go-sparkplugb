// SPDX-License-Identifier: MIT

package payload

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/otfabric/go-sparkplugb/proto"
	"google.golang.org/protobuf/proto"
)

func TestScalarRoundTrip(t *testing.T) {
	ts := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		dt   pb.DataType
		val  any
	}{
		{"Int8 positive", pb.DataType_Int8, int8(42)},
		{"Int8 negative", pb.DataType_Int8, int8(-42)},
		{"Int8 min", pb.DataType_Int8, int8(math.MinInt8)},
		{"Int8 max", pb.DataType_Int8, int8(math.MaxInt8)},
		{"Int16 positive", pb.DataType_Int16, int16(1234)},
		{"Int16 negative", pb.DataType_Int16, int16(-1234)},
		{"Int32 positive", pb.DataType_Int32, int32(100000)},
		{"Int32 negative", pb.DataType_Int32, int32(-100000)},
		{"Int64", pb.DataType_Int64, int64(-9876543210)},
		{"UInt8", pb.DataType_UInt8, uint8(255)},
		{"UInt16", pb.DataType_UInt16, uint16(65535)},
		{"UInt32", pb.DataType_UInt32, uint32(4294967295)},
		{"UInt64", pb.DataType_UInt64, uint64(18446744073709551615)},
		{"Float", pb.DataType_Float, float32(3.14)},
		{"Double", pb.DataType_Double, float64(2.718281828)},
		{"Boolean true", pb.DataType_Boolean, true},
		{"Boolean false", pb.DataType_Boolean, false},
		{"String", pb.DataType_String, "hello"},
		{"Text", pb.DataType_Text, "multiline\ntext"},
		{"UUID", pb.DataType_UUID, "550e8400-e29b-41d4-a716-446655440000"},
		{"DateTime", pb.DataType_DateTime, ts},
		{"Bytes", pb.DataType_Bytes, []byte{0x01, 0x02, 0x03}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &pb.Payload_Metric{}
			if err := SetValue(m, tt.dt, tt.val); err != nil {
				t.Fatalf("SetValue: %v", err)
			}
			got, err := GetValue(m)
			if err != nil {
				t.Fatalf("GetValue: %v", err)
			}
			if !reflect.DeepEqual(got, tt.val) {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.val, tt.val)
			}
		})
	}
}

func TestNullMetric(t *testing.T) {
	m := &pb.Payload_Metric{}
	if err := SetValue(m, pb.DataType_Int32, nil); err != nil {
		t.Fatal(err)
	}
	got, err := GetValue(m)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestTypeMismatch(t *testing.T) {
	m := &pb.Payload_Metric{}
	err := SetValue(m, pb.DataType_Int32, "not an int")
	if err != ErrTypeMismatch {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
}

func TestArrayRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		dt   pb.DataType
		val  any
	}{
		{"Int8Array", pb.DataType_Int8Array, []int8{-128, 0, 127}},
		{"Int16Array", pb.DataType_Int16Array, []int16{-32768, 0, 32767}},
		{"Int32Array", pb.DataType_Int32Array, []int32{-2147483648, 0, 2147483647}},
		{"Int64Array", pb.DataType_Int64Array, []int64{-9223372036854775808, 0, 9223372036854775807}},
		{"UInt8Array", pb.DataType_UInt8Array, []uint8{0, 128, 255}},
		{"UInt16Array", pb.DataType_UInt16Array, []uint16{0, 1000, 65535}},
		{"UInt32Array", pb.DataType_UInt32Array, []uint32{0, 1000000, 4294967295}},
		{"UInt64Array", pb.DataType_UInt64Array, []uint64{0, 1000000000000, 18446744073709551615}},
		{"FloatArray", pb.DataType_FloatArray, []float32{-1.5, 0, 3.14}},
		{"DoubleArray", pb.DataType_DoubleArray, []float64{-1.5e100, 0, 1.5e100}},
		{"BooleanArray", pb.DataType_BooleanArray, []bool{true, false, true, true, false, true, false, false, true}},
		{"StringArray", pb.DataType_StringArray, []string{"hello", "world", ""}},
		{"DateTimeArray", pb.DataType_DateTimeArray, []time.Time{
			time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &pb.Payload_Metric{}
			if err := SetValue(m, tt.dt, tt.val); err != nil {
				t.Fatalf("SetValue: %v", err)
			}
			got, err := GetValue(m)
			if err != nil {
				t.Fatalf("GetValue: %v", err)
			}
			if !reflect.DeepEqual(got, tt.val) {
				t.Errorf("got %v, want %v", got, tt.val)
			}
		})
	}
}

func TestBooleanArrayBitPacking(t *testing.T) {
	input := []bool{true, false, true, true, false, true, false, false, true}
	encoded := encodeBooleanArray(input)

	// 4-byte count = 9
	if len(encoded) != 4+2 {
		t.Fatalf("expected 6 bytes, got %d", len(encoded))
	}

	decoded, err := decodeBooleanArray(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, input) {
		t.Errorf("got %v, want %v", decoded, input)
	}
}

func TestEmptyArrays(t *testing.T) {
	m := &pb.Payload_Metric{}
	if err := SetValue(m, pb.DataType_StringArray, []string{}); err != nil {
		t.Fatal(err)
	}
	got, err := GetValue(m)
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := got.([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", got)
	}
	if len(arr) != 0 {
		t.Errorf("expected empty, got %v", arr)
	}
}

func TestInvalidArrayDecoding(t *testing.T) {
	// Int16Array with odd byte count
	_, err := decodeInt16Array([]byte{0x01, 0x02, 0x03})
	if err != ErrInvalidArray {
		t.Errorf("expected ErrInvalidArray for odd Int16Array, got %v", err)
	}

	// BooleanArray too short
	_, err = decodeBooleanArray([]byte{0x01, 0x02})
	if err != ErrInvalidArray {
		t.Errorf("expected ErrInvalidArray for short BooleanArray, got %v", err)
	}

	// BooleanArray count exceeds data
	_, err = decodeBooleanArray([]byte{0xFF, 0x00, 0x00, 0x00, 0x01})
	if err != ErrInvalidArray {
		t.Errorf("expected ErrInvalidArray for count>data BooleanArray, got %v", err)
	}
}

func TestSignedIntExtension(t *testing.T) {
	m := &pb.Payload_Metric{}
	if err := SetValue(m, pb.DataType_Int8, int8(-1)); err != nil {
		t.Fatal(err)
	}
	raw := m.GetIntValue()
	if raw != 0xFFFFFFFF {
		t.Errorf("Int8(-1) wire value = %#x, want 0xFFFFFFFF", raw)
	}
	got, err := GetValue(m)
	if err != nil {
		t.Fatal(err)
	}
	if got.(int8) != -1 {
		t.Errorf("round-trip Int8(-1) = %v", got)
	}
}

func TestSignedIntEncoding(t *testing.T) {
	for _, tc := range []struct {
		dt   pb.DataType
		v    any
		want uint32
	}{
		{pb.DataType_Int8, int8(-8), 0xFFFFFFF8},
		{pb.DataType_Int8, int8(-128), 0xFFFFFF80},
		{pb.DataType_Int8, int8(127), 0x7F},
		{pb.DataType_Int8, int8(0), 0},
		{pb.DataType_Int16, int16(-8), 0xFFFFFFF8},
		{pb.DataType_Int16, int16(-32768), 0xFFFF8000},
		{pb.DataType_Int16, int16(32767), 0x7FFF},
		{pb.DataType_Int16, int16(0), 0},
	} {
		m := &pb.Payload_Metric{}
		if err := SetValue(m, tc.dt, tc.v); err != nil {
			t.Fatalf("%v %v: %v", tc.dt, tc.v, err)
		}
		if got := m.GetIntValue(); got != tc.want {
			t.Errorf("%v %v on the wire = %#x, want %#x", tc.dt, tc.v, got, tc.want)
		}
	}
}

func TestSignedIntDecoding(t *testing.T) {
	for _, tc := range []struct {
		dt   pb.DataType
		wire uint32
		want any
	}{
		// Sign-extended.
		{pb.DataType_Int8, 0xFFFFFFF8, int8(-8)},
		{pb.DataType_Int8, 0xFFFFFFFF, int8(-1)},
		{pb.DataType_Int8, 0xFFFFFF80, int8(-128)},
		{pb.DataType_Int16, 0xFFFFFFF8, int16(-8)},
		{pb.DataType_Int16, 0xFFFFFFFF, int16(-1)},
		{pb.DataType_Int16, 0xFFFF8000, int16(-32768)},
		// Width-sized.
		{pb.DataType_Int8, 0xF8, int8(-8)},
		{pb.DataType_Int8, 0xFF, int8(-1)}, // 255 is the width form of -1
		{pb.DataType_Int8, 0x80, int8(-128)},
		{pb.DataType_Int16, 0xFFF8, int16(-8)},
		{pb.DataType_Int16, 0xFFFF, int16(-1)},
		{pb.DataType_Int16, 0x8000, int16(-32768)},
		// The forms coincide.
		{pb.DataType_Int8, 0, int8(0)},
		{pb.DataType_Int8, 127, int8(127)},
		{pb.DataType_Int16, 0, int16(0)},
		{pb.DataType_Int16, 127, int16(127)},
		{pb.DataType_Int16, 128, int16(128)}, // not the sign bit of an Int16
		{pb.DataType_Int16, 32767, int16(32767)},
	} {
		dt := uint32(tc.dt)
		m := &pb.Payload_Metric{Datatype: &dt, Value: &pb.Payload_Metric_IntValue{IntValue: tc.wire}}
		got, err := GetValue(m)
		if err != nil || got != tc.want {
			t.Errorf("%v from %#x: got %v (%T), %v; want %v", tc.dt, tc.wire, got, got, err, tc.want)
		}
	}
}

func TestSignedIntOutOfRange(t *testing.T) {
	for _, tc := range []struct {
		dt   pb.DataType
		wire uint32
	}{
		{pb.DataType_Int8, 0x00000100}, // one above the width
		{pb.DataType_Int8, 0x0000FFF8}, // the width form of an Int16
		{pb.DataType_Int8, 0xFFFFFF7F}, // sign bit of the width clear
		{pb.DataType_Int8, 0x7FFFFFFF},
		{pb.DataType_Int8, 0x80000000},
		{pb.DataType_Int16, 0x00010000},
		{pb.DataType_Int16, 0xFFFF7FFF},
		{pb.DataType_Int16, 0x7FFFFFFF},
		{pb.DataType_Int16, 0x80000000},
	} {
		dt := uint32(tc.dt)
		m := &pb.Payload_Metric{Datatype: &dt, Value: &pb.Payload_Metric_IntValue{IntValue: tc.wire}}
		got, err := GetValue(m)
		if !errors.Is(err, ErrTypeMismatch) || got != nil {
			t.Errorf("%v from %#x: got %v, %v; want ErrTypeMismatch", tc.dt, tc.wire, got, err)
		}
	}
	// long_value holds the same rule, beyond 32 bits too.
	dt := uint32(pb.DataType_Int8)
	m := &pb.Payload_Metric{Datatype: &dt, Value: &pb.Payload_Metric_LongValue{LongValue: 0x1FFFFFFF8}}
	if _, err := GetValue(m); !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("Int8 from long_value 0x1FFFFFFF8: %v, want ErrTypeMismatch", err)
	}
}

func TestNilMetricGetValue(t *testing.T) {
	_, err := GetValue(nil)
	if err != ErrNilMetric {
		t.Errorf("expected ErrNilMetric, got %v", err)
	}
}

func TestNilMetricSetValue(t *testing.T) {
	err := SetValue(nil, pb.DataType_Int32, int32(0))
	if err != ErrNilMetric {
		t.Errorf("expected ErrNilMetric, got %v", err)
	}
}

func TestUnknownDataType(t *testing.T) {
	m := &pb.Payload_Metric{}
	err := SetValue(m, pb.DataType(999), int32(0))
	if err != ErrUnknownType {
		t.Errorf("expected ErrUnknownType, got %v", err)
	}
}

func TestGetValueUnknownDataType(t *testing.T) {
	m := &pb.Payload_Metric{}
	dt := uint32(999)
	m.Datatype = &dt
	_, err := GetValue(m)
	if err != ErrUnknownType {
		t.Errorf("expected ErrUnknownType, got %v", err)
	}
}

func TestTypeMismatchAllTypes(t *testing.T) {
	types := []struct {
		dt  pb.DataType
		bad any
	}{
		{pb.DataType_Int8, "bad"},
		{pb.DataType_Int16, "bad"},
		{pb.DataType_Int64, "bad"},
		{pb.DataType_UInt8, "bad"},
		{pb.DataType_UInt16, "bad"},
		{pb.DataType_UInt32, "bad"},
		{pb.DataType_UInt64, "bad"},
		{pb.DataType_Float, "bad"},
		{pb.DataType_Double, "bad"},
		{pb.DataType_Boolean, "bad"},
		{pb.DataType_String, 42},
		{pb.DataType_DateTime, "bad"},
		{pb.DataType_Bytes, 42},
		{pb.DataType_Int8Array, "bad"},
		{pb.DataType_Int16Array, "bad"},
		{pb.DataType_Int32Array, "bad"},
		{pb.DataType_Int64Array, "bad"},
		{pb.DataType_UInt8Array, "bad"},
		{pb.DataType_UInt16Array, "bad"},
		{pb.DataType_UInt32Array, "bad"},
		{pb.DataType_UInt64Array, "bad"},
		{pb.DataType_FloatArray, "bad"},
		{pb.DataType_DoubleArray, "bad"},
		{pb.DataType_BooleanArray, "bad"},
		{pb.DataType_StringArray, 42},
		{pb.DataType_DateTimeArray, "bad"},
	}
	for _, tt := range types {
		t.Run(tt.dt.String(), func(t *testing.T) {
			m := &pb.Payload_Metric{}
			err := SetValue(m, tt.dt, tt.bad)
			if err != ErrTypeMismatch {
				t.Errorf("expected ErrTypeMismatch for %s, got %v", tt.dt, err)
			}
		})
	}
}

func TestInvalidArrayDecodingMoreCases(t *testing.T) {
	tests := []struct {
		name string
		fn   func([]byte) (any, error)
		data []byte
	}{
		{"Int32 odd", func(b []byte) (any, error) { return decodeInt32Array(b) }, []byte{1, 2, 3}},
		{"Int64 odd", func(b []byte) (any, error) { return decodeInt64Array(b) }, []byte{1, 2, 3}},
		{"UInt16 odd", func(b []byte) (any, error) { return decodeUInt16Array(b) }, []byte{1, 2, 3}},
		{"UInt32 odd", func(b []byte) (any, error) { return decodeUInt32Array(b) }, []byte{1, 2, 3}},
		{"UInt64 odd", func(b []byte) (any, error) { return decodeUInt64Array(b) }, []byte{1, 2, 3}},
		{"Float odd", func(b []byte) (any, error) { return decodeFloatArray(b) }, []byte{1, 2, 3}},
		{"Double odd", func(b []byte) (any, error) { return decodeDoubleArray(b) }, []byte{1, 2, 3}},
		{"DateTime odd", func(b []byte) (any, error) { return decodeDateTimeArray(b) }, []byte{1, 2, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.fn(tt.data)
			if err != ErrInvalidArray {
				t.Errorf("expected ErrInvalidArray, got %v", err)
			}
		})
	}
}

func TestBooleanArrayTooLarge(t *testing.T) {
	data := make([]byte, 4)
	// count = MaxArrayElements + 1
	count := uint32(MaxArrayElements + 1)
	data[0] = byte(count)
	data[1] = byte(count >> 8)
	data[2] = byte(count >> 16)
	data[3] = byte(count >> 24)
	_, err := decodeBooleanArray(data)
	if err != ErrArrayTooLarge {
		t.Errorf("expected ErrArrayTooLarge, got %v", err)
	}
}

func TestGetQualityNoProperties(t *testing.T) {
	m := &pb.Payload_Metric{}
	_, ok := GetQuality(m)
	if ok {
		t.Error("expected no quality")
	}
}

func TestGetQualityMissingKey(t *testing.T) {
	m, _ := NewMetric("x", pb.DataType_Int32, int32(0))
	qKey := "other"
	qType := uint32(pb.DataType_Int32)
	qVal := uint32(0)
	m.Properties = &pb.Payload_PropertySet{
		Keys: []string{qKey},
		Values: []*pb.Payload_PropertyValue{{
			Type:  &qType,
			Value: &pb.Payload_PropertyValue_IntValue{IntValue: qVal},
		}},
	}
	_, ok := GetQuality(m)
	if ok {
		t.Error("expected no quality for non-quality property")
	}
}

func TestFileType(t *testing.T) {
	m := &pb.Payload_Metric{}
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if err := SetValue(m, pb.DataType_File, data); err != nil {
		t.Fatal(err)
	}
	got, err := GetValue(m)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, data) {
		t.Errorf("got %v, want %v", got, data)
	}
}

func TestIntegerInLongValue(t *testing.T) {
	// Eclipse Tahu encodes UInt32 in long_value instead of int_value.
	for _, tc := range []struct {
		dt   pb.DataType
		long uint64
		want any
	}{
		{pb.DataType_UInt32, 4000000000, uint32(4000000000)},
		{pb.DataType_Int32, uint64(math.MaxUint64 - 9), int32(-10)},
		{pb.DataType_Int8, uint64(math.MaxUint64), int8(-1)},
	} {
		dt := uint32(tc.dt)
		m := &pb.Payload_Metric{Datatype: &dt, Value: &pb.Payload_Metric_LongValue{LongValue: tc.long}}
		got, err := GetValue(m)
		if err != nil || got != tc.want {
			t.Errorf("%v in long_value %d: got %v (%T), %v; want %v", tc.dt, tc.long, got, got, err, tc.want)
		}
	}
}

// Regression: MaxStringBytes was exported and enforced nowhere, and a
// StringArray of NUL bytes grew 16 bytes of string header per byte received.
func TestDecodeLimits(t *testing.T) {
	long := strings.Repeat("x", MaxStringBytes+1)
	limit := strings.Repeat("x", MaxStringBytes)
	strMetric := func(dt pb.DataType, s string) *pb.Payload_Metric {
		return &pb.Payload_Metric{Datatype: proto.Uint32(uint32(dt)), Value: &pb.Payload_Metric_StringValue{StringValue: s}}
	}
	arrMetric := func(b []byte) *pb.Payload_Metric {
		return &pb.Payload_Metric{Datatype: proto.Uint32(uint32(pb.DataType_StringArray)), Value: &pb.Payload_Metric_BytesValue{BytesValue: b}}
	}
	tests := []struct {
		name string
		m    *pb.Payload_Metric
		want error
	}{
		{"string at the limit", strMetric(pb.DataType_String, limit), nil},
		{"string", strMetric(pb.DataType_String, long), ErrStringTooLarge},
		{"text", strMetric(pb.DataType_Text, long), ErrStringTooLarge},
		{"uuid", strMetric(pb.DataType_UUID, long), ErrStringTooLarge},
		{"array element at the limit", arrMetric([]byte(limit + "\x00")), nil},
		{"array element", arrMetric([]byte(long + "\x00")), ErrStringTooLarge},
		{"last array element, unterminated", arrMetric([]byte("a\x00" + long)), ErrStringTooLarge},
		{"array at the element limit", arrMetric(make([]byte, MaxArrayElements)), nil},
		{"array elements", arrMetric(make([]byte, MaxArrayElements+1)), ErrArrayTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := GetValue(tt.m); !errors.Is(err, tt.want) {
				t.Fatalf("err %v, want %v", err, tt.want)
			}
		})
	}
}
