// SPDX-License-Identifier: MIT

package payload

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	pb "github.com/otfabric/go-sparkplugb/proto"
)

// integerReaders read a wire value through each place an integer is carried:
// metric, DataSet cell, template parameter, PropertyValue.
var integerReaders = []func(dt pb.DataType, wire uint64, long bool) (any, error){
	func(dt pb.DataType, wire uint64, long bool) (any, error) {
		d := uint32(dt)
		m := &pb.Payload_Metric{Datatype: &d}
		if long {
			m.Value = &pb.Payload_Metric_LongValue{LongValue: wire}
		} else {
			m.Value = &pb.Payload_Metric_IntValue{IntValue: uint32(wire)}
		}
		return GetValue(m)
	},
	func(dt pb.DataType, wire uint64, long bool) (any, error) {
		cell := &pb.Payload_DataSet_DataSetValue{}
		if long {
			cell.Value = &pb.Payload_DataSet_DataSetValue_LongValue{LongValue: wire}
		} else {
			cell.Value = &pb.Payload_DataSet_DataSetValue_IntValue{IntValue: uint32(wire)}
		}
		m := &pb.Payload_Metric{Value: &pb.Payload_Metric_DatasetValue{DatasetValue: &pb.Payload_DataSet{
			Columns: []string{"c"},
			Types:   []uint32{uint32(dt)},
			Rows:    []*pb.Payload_DataSet_Row{{Elements: []*pb.Payload_DataSet_DataSetValue{cell}}},
		}}}
		_, _, rows, err := GetDataSetValues(m)
		if err != nil {
			return nil, err
		}
		return rows[0][0], nil
	},
	func(dt pb.DataType, wire uint64, long bool) (any, error) {
		d := uint32(dt)
		p := &pb.Payload_Template_Parameter{Type: &d}
		if long {
			p.Value = &pb.Payload_Template_Parameter_LongValue{LongValue: wire}
		} else {
			p.Value = &pb.Payload_Template_Parameter_IntValue{IntValue: uint32(wire)}
		}
		return GetTemplateParameterValue(p)
	},
	func(dt pb.DataType, wire uint64, long bool) (any, error) {
		d := uint32(dt)
		p := &pb.Payload_PropertyValue{Type: &d}
		if long {
			p.Value = &pb.Payload_PropertyValue_LongValue{LongValue: wire}
		} else {
			p.Value = &pb.Payload_PropertyValue_IntValue{IntValue: uint32(wire)}
		}
		return GetPropertyValue(p)
	},
}

var pathNames = []string{"metric", "dataset", "parameter", "property"}

func TestIntegerDecoding(t *testing.T) {
	const signExt32 = 0xFFFFFFF8
	const signExt64 = math.MaxUint64 - 7 // -8
	for _, tc := range []struct {
		dt   pb.DataType
		wire uint64
		long bool
		want any // nil: ErrTypeMismatch
	}{
		// The forms of a negative Int8 and Int16.
		{pb.DataType_Int8, 0xF8, false, int8(-8)},
		{pb.DataType_Int8, signExt32, false, int8(-8)},
		{pb.DataType_Int8, 0xF8, true, int8(-8)},
		{pb.DataType_Int8, signExt32, true, int8(-8)},
		{pb.DataType_Int8, signExt64, true, int8(-8)},
		{pb.DataType_Int8, 0x7F, false, int8(127)},
		{pb.DataType_Int8, 0x80, false, int8(-128)},
		{pb.DataType_Int16, 0xFFF8, false, int16(-8)},
		{pb.DataType_Int16, signExt32, false, int16(-8)},
		{pb.DataType_Int16, signExt64, true, int16(-8)},
		{pb.DataType_Int16, 0x8000, false, int16(-32768)},
		// Neither form.
		{pb.DataType_Int8, 0x100, false, nil},
		{pb.DataType_Int8, 0xFFF8, false, nil},
		{pb.DataType_Int8, 0xFFFFFF7F, false, nil},
		{pb.DataType_Int8, 0x1FFFFFFF8, true, nil},
		{pb.DataType_Int16, 0x10000, false, nil},
		{pb.DataType_Int16, 0xFFFF7FFF, false, nil},
		{pb.DataType_Int16, 0xFFFFFFFF00000000, true, nil},
		// Int32.
		{pb.DataType_Int32, 0x7FFFFFFF, false, int32(math.MaxInt32)},
		{pb.DataType_Int32, 0x80000000, false, int32(math.MinInt32)},
		{pb.DataType_Int32, signExt32, false, int32(-8)},
		{pb.DataType_Int32, signExt32, true, int32(-8)},
		{pb.DataType_Int32, signExt64, true, int32(-8)},
		{pb.DataType_Int32, 0x100000000, true, nil},
		{pb.DataType_Int32, 0xFFFFFFFF00000000, true, nil},
		{pb.DataType_Int32, math.MaxInt64, true, nil},
		// Int64 and UInt64 have the whole of long_value.
		{pb.DataType_Int64, signExt64, true, int64(-8)},
		{pb.DataType_Int64, 1 << 63, true, int64(math.MinInt64)},
		{pb.DataType_Int64, 5, false, int64(5)},
		{pb.DataType_UInt64, math.MaxUint64, true, uint64(math.MaxUint64)},
		{pb.DataType_UInt64, 5, false, uint64(5)},
		// Unsigned types have no second form.
		{pb.DataType_UInt8, 255, false, uint8(255)},
		{pb.DataType_UInt8, 255, true, uint8(255)},
		{pb.DataType_UInt8, 256, false, nil},
		{pb.DataType_UInt8, 256, true, nil},
		{pb.DataType_UInt8, signExt32, false, nil},
		{pb.DataType_UInt16, 65535, false, uint16(65535)},
		{pb.DataType_UInt16, 65536, false, nil},
		{pb.DataType_UInt16, 65536, true, nil},
		{pb.DataType_UInt32, math.MaxUint32, false, uint32(math.MaxUint32)},
		{pb.DataType_UInt32, math.MaxUint32, true, uint32(math.MaxUint32)},
		{pb.DataType_UInt32, 1 << 32, true, nil},
		// DateTime is a uint64, but a time.Time holds an int64 of milliseconds.
		{pb.DataType_DateTime, 1000, true, time.UnixMilli(1000).UTC()},
		{pb.DataType_DateTime, math.MaxInt64, true, time.UnixMilli(math.MaxInt64).UTC()},
		{pb.DataType_DateTime, 1 << 63, true, nil},
	} {
		for i, read := range integerReaders {
			got, err := read(tc.dt, tc.wire, tc.long)
			if tc.want == nil {
				if !errors.Is(err, ErrTypeMismatch) || got != nil {
					t.Errorf("%s %v from %#x (long %v): got %v, %v; want ErrTypeMismatch", pathNames[i], tc.dt, tc.wire, tc.long, got, err)
				}
			} else if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Errorf("%s %v from %#x (long %v): got %v (%T), %v; want %v", pathNames[i], tc.dt, tc.wire, tc.long, got, got, err, tc.want)
			}
		}
	}
}

// integerWriters write a Go value through each place and return what is on the
// wire, and whether it is in long_value.
var integerWriters = []func(dt pb.DataType, v any) (uint64, bool, error){
	func(dt pb.DataType, v any) (uint64, bool, error) {
		var m pb.Payload_Metric
		if err := SetValue(&m, dt, v); err != nil {
			return 0, false, err
		}
		_, long := m.Value.(*pb.Payload_Metric_LongValue)
		w, err := integer(&m)
		return w, long, err
	},
	func(dt pb.DataType, v any) (uint64, bool, error) {
		m, err := NewDataSet("d", []string{"c"}, []pb.DataType{dt}, [][]any{{v}})
		if err != nil {
			return 0, false, err
		}
		switch c := m.GetDatasetValue().Rows[0].Elements[0].Value.(type) {
		case *pb.Payload_DataSet_DataSetValue_IntValue:
			return uint64(c.IntValue), false, nil
		case *pb.Payload_DataSet_DataSetValue_LongValue:
			return c.LongValue, true, nil
		}
		return 0, false, errors.New("no integer in the cell")
	},
	func(dt pb.DataType, v any) (uint64, bool, error) {
		p, err := NewTemplateParameter("p", dt, v)
		if err != nil {
			return 0, false, err
		}
		switch c := p.Value.(type) {
		case *pb.Payload_Template_Parameter_IntValue:
			return uint64(c.IntValue), false, nil
		case *pb.Payload_Template_Parameter_LongValue:
			return c.LongValue, true, nil
		}
		return 0, false, errors.New("no integer in the parameter")
	},
	func(dt pb.DataType, v any) (uint64, bool, error) {
		p, err := NewPropertyValue(dt, v)
		if err != nil {
			return 0, false, err
		}
		switch c := p.Value.(type) {
		case *pb.Payload_PropertyValue_IntValue:
			return uint64(c.IntValue), false, nil
		case *pb.Payload_PropertyValue_LongValue:
			return c.LongValue, true, nil
		}
		return 0, false, errors.New("no integer in the property")
	},
}

func TestIntegerEncoding(t *testing.T) {
	for _, tc := range []struct {
		dt   pb.DataType
		v    any
		wire uint64
		long bool
	}{
		{pb.DataType_Int8, int8(-8), 0xFFFFFFF8, false},
		{pb.DataType_Int8, int8(-128), 0xFFFFFF80, false},
		{pb.DataType_Int8, int8(127), 0x7F, false},
		{pb.DataType_Int16, int16(-8), 0xFFFFFFF8, false},
		{pb.DataType_Int16, int16(math.MinInt16), 0xFFFF8000, false},
		{pb.DataType_Int32, int32(-8), 0xFFFFFFF8, false},
		{pb.DataType_Int64, int64(-8), math.MaxUint64 - 7, true},
		{pb.DataType_UInt8, uint8(255), 255, false},
		{pb.DataType_UInt16, uint16(65535), 65535, false},
		{pb.DataType_UInt32, uint32(math.MaxUint32), math.MaxUint32, false},
		{pb.DataType_UInt64, uint64(math.MaxUint64), math.MaxUint64, true},
		{pb.DataType_DateTime, time.UnixMilli(1000), 1000, true},
	} {
		for i, write := range integerWriters {
			wire, long, err := write(tc.dt, tc.v)
			if err != nil || wire != tc.wire || long != tc.long {
				t.Errorf("%s %v %v: wire %#x long %v, %v; want %#x, %v", pathNames[i], tc.dt, tc.v, wire, long, err, tc.wire, tc.long)
			}
		}
	}
}

// The wire type stays valid in the composite constructors: it has to be a value
// of the datatype, and it comes out in the form the encoder writes.
func TestIntegerRawValues(t *testing.T) {
	for _, tc := range []struct {
		dt   pb.DataType
		raw  any
		wire uint64
	}{
		{pb.DataType_Int8, uint32(0xFFFFFFF8), 0xFFFFFFF8},
		{pb.DataType_Int8, uint32(0xF8), 0xFFFFFFF8},
		{pb.DataType_Int16, uint32(0xFFF8), 0xFFFFFFF8},
		{pb.DataType_Int32, uint32(0xFFFFFFF8), 0xFFFFFFF8},
		{pb.DataType_UInt8, uint32(255), 255},
		{pb.DataType_UInt32, uint32(7), 7},
		{pb.DataType_Int64, uint64(math.MaxUint64), math.MaxUint64},
		{pb.DataType_UInt64, uint64(7), 7},
		{pb.DataType_DateTime, uint64(1000), 1000},
	} {
		for i, write := range integerWriters[1:] {
			wire, _, err := write(tc.dt, tc.raw)
			if err != nil || wire != tc.wire {
				t.Errorf("%s %v raw %v: %#x, %v; want %#x", pathNames[i+1], tc.dt, tc.raw, wire, err, tc.wire)
			}
		}
	}
	for _, tc := range []struct {
		dt  pb.DataType
		raw any
	}{
		{pb.DataType_Int8, uint32(0x100)},
		{pb.DataType_Int8, uint32(300)},
		{pb.DataType_Int16, uint32(0x10000)},
		{pb.DataType_UInt8, uint32(256)},
		{pb.DataType_UInt16, uint32(65536)},
		{pb.DataType_DateTime, uint64(1 << 63)},
		// The wire type of another width, and the Go type of another datatype.
		{pb.DataType_Int8, uint64(1)},
		{pb.DataType_Int64, uint32(1)},
		{pb.DataType_Int8, int16(1)},
		{pb.DataType_Int8, 1},
		{pb.DataType_DateTime, "now"},
	} {
		for i, write := range integerWriters[1:] {
			if wire, _, err := write(tc.dt, tc.raw); !errors.Is(err, ErrTypeMismatch) {
				t.Errorf("%s %v raw %v: %#x, %v; want ErrTypeMismatch", pathNames[i+1], tc.dt, tc.raw, wire, err)
			}
		}
	}
}

func TestIntegerOfAnotherKind(t *testing.T) {
	dt := uint32(pb.DataType_Int8)
	m := &pb.Payload_Metric{Datatype: &dt, Value: &pb.Payload_Metric_StringValue{StringValue: "8"}}
	if got, err := GetValue(m); !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("metric: %v, %v", got, err)
	}
	d := &pb.Payload_Metric{Value: &pb.Payload_Metric_DatasetValue{DatasetValue: &pb.Payload_DataSet{
		Types: []uint32{dt},
		Rows: []*pb.Payload_DataSet_Row{{Elements: []*pb.Payload_DataSet_DataSetValue{
			{Value: &pb.Payload_DataSet_DataSetValue_StringValue{StringValue: "8"}},
		}}},
	}}}
	if _, _, rows, err := GetDataSetValues(d); !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("dataset: %v, %v", rows, err)
	}
	p := &pb.Payload_Template_Parameter{Type: &dt, Value: &pb.Payload_Template_Parameter_StringValue{StringValue: "8"}}
	if got, err := GetTemplateParameterValue(p); !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("parameter: %v, %v", got, err)
	}
	v := &pb.Payload_PropertyValue{Type: &dt, Value: &pb.Payload_PropertyValue_StringValue{StringValue: "8"}}
	if got, err := GetPropertyValue(v); !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("property: %v, %v", got, err)
	}
}

func TestIntegerWithoutValue(t *testing.T) {
	dt := uint32(pb.DataType_Int8)
	if got, err := GetValue(&pb.Payload_Metric{Datatype: &dt}); got != int8(0) || err != nil {
		t.Errorf("metric: %v, %v", got, err)
	}
	if got, err := GetTemplateParameterValue(&pb.Payload_Template_Parameter{Type: &dt}); got != int8(0) || err != nil {
		t.Errorf("parameter: %v, %v", got, err)
	}
	if got, err := GetPropertyValue(&pb.Payload_PropertyValue{Type: &dt}); got != int8(0) || err != nil {
		t.Errorf("property: %v, %v", got, err)
	}
}

func TestDataSetTypedRoundTrip(t *testing.T) {
	columns := []string{"i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "t"}
	types := []pb.DataType{
		pb.DataType_Int8, pb.DataType_Int16, pb.DataType_Int32, pb.DataType_Int64,
		pb.DataType_UInt8, pb.DataType_UInt16, pb.DataType_UInt32, pb.DataType_UInt64,
		pb.DataType_DateTime,
	}
	when := time.UnixMilli(1700000000123).UTC()
	rows := [][]any{
		{int8(-128), int16(-32768), int32(math.MinInt32), int64(math.MinInt64),
			uint8(255), uint16(65535), uint32(math.MaxUint32), uint64(math.MaxUint64), when},
		{int8(127), int16(32767), int32(math.MaxInt32), int64(math.MaxInt64),
			uint8(0), uint16(0), uint32(0), uint64(0), time.UnixMilli(0).UTC()},
	}
	m, err := NewDataSet("ds", columns, types, rows)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Encode(NewPayload(0, m))
	if err != nil {
		t.Fatal(err)
	}
	p, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	_, _, got, err := GetDataSetValues(p.Metrics[0])
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Errorf("got %v, want %v", got, rows)
	}
}

func TestDataSetCellBeyondTypes(t *testing.T) {
	m := &pb.Payload_Metric{Value: &pb.Payload_Metric_DatasetValue{DatasetValue: &pb.Payload_DataSet{
		Types: []uint32{uint32(pb.DataType_Int8)},
		Rows: []*pb.Payload_DataSet_Row{{Elements: []*pb.Payload_DataSet_DataSetValue{
			{Value: &pb.Payload_DataSet_DataSetValue_IntValue{IntValue: 1}},
			{Value: &pb.Payload_DataSet_DataSetValue_IntValue{IntValue: 2}},
		}}},
	}}}
	if _, _, _, err := GetDataSetValues(m); !errors.Is(err, ErrColumnMismatch) {
		t.Errorf("got %v, want ErrColumnMismatch", err)
	}
}

func TestTemplateMembersAreMetrics(t *testing.T) {
	member, err := NewMetric("offset", pb.DataType_Int8, int8(-8))
	if err != nil {
		t.Fatal(err)
	}
	param, err := NewTemplateParameter("gain", pb.DataType_Int16, int16(-300))
	if err != nil {
		t.Fatal(err)
	}
	inst, err := NewTemplateInstance("i", "T", []*pb.Payload_Metric{member}, []*pb.Payload_Template_Parameter{param})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Encode(NewPayload(0, inst))
	if err != nil {
		t.Fatal(err)
	}
	p, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := p.Metrics[0].GetTemplateValue()
	if got, err := GetValue(tmpl.Metrics[0]); got != int8(-8) || err != nil {
		t.Errorf("member: %v, %v", got, err)
	}
	if got, err := GetTemplateParameterValue(tmpl.Parameters[0]); got != int16(-300) || err != nil {
		t.Errorf("parameter: %v, %v", got, err)
	}
	// A member from a stack that sends the width.
	tmpl.Metrics[0].Value = &pb.Payload_Metric_IntValue{IntValue: 0xF8}
	if got, err := GetValue(tmpl.Metrics[0]); got != int8(-8) || err != nil {
		t.Errorf("width-sized member: %v, %v", got, err)
	}
	tmpl.Metrics[0].Value = &pb.Payload_Metric_IntValue{IntValue: 0x100}
	if got, err := GetValue(tmpl.Metrics[0]); !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("out of range member: %v, %v", got, err)
	}
}

func TestPropertyValueTypes(t *testing.T) {
	set := &pb.Payload_PropertySet{Keys: []string{"k"}}
	list := &pb.Payload_PropertySetList{Propertyset: []*pb.Payload_PropertySet{set}}
	for _, tc := range []struct {
		dt pb.DataType
		v  any
	}{
		{pb.DataType_Float, float32(1.5)},
		{pb.DataType_Double, 2.5},
		{pb.DataType_Boolean, true},
		{pb.DataType_String, "s"},
		{pb.DataType_Text, "t"},
		{pb.DataType_PropertySet, set},
		{pb.DataType_PropertySetList, list},
	} {
		p, err := NewPropertyValue(tc.dt, tc.v)
		if err != nil {
			t.Fatalf("%v: %v", tc.dt, err)
		}
		if got, err := GetPropertyValue(p); got != tc.v || err != nil {
			t.Errorf("%v: got %v, %v", tc.dt, got, err)
		}
		if _, err := NewPropertyValue(tc.dt, struct{}{}); !errors.Is(err, ErrTypeMismatch) {
			t.Errorf("%v of a struct: %v", tc.dt, err)
		}
	}
	p, err := NewPropertyValue(pb.DataType_Int8, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := GetPropertyValue(p); got != nil || err != nil {
		t.Errorf("null property: %v, %v", got, err)
	}
	if _, err := NewPropertyValue(pb.DataType_Bytes, []byte{1}); !errors.Is(err, ErrUnknownType) {
		t.Errorf("Bytes property: %v", err)
	}
	if _, err := GetPropertyValue(nil); !errors.Is(err, ErrNilMetric) {
		t.Errorf("nil property: %v", err)
	}
	d := uint32(pb.DataType_Bytes)
	if _, err := GetPropertyValue(&pb.Payload_PropertyValue{Type: &d}); !errors.Is(err, ErrUnknownType) {
		t.Errorf("Bytes property: %v", err)
	}
}

func TestGetQualityStrict(t *testing.T) {
	long := func(v uint64) *pb.Payload_Metric {
		q := uint32(pb.DataType_Int32)
		return &pb.Payload_Metric{Properties: &pb.Payload_PropertySet{
			Keys:   []string{"quality"},
			Values: []*pb.Payload_PropertyValue{{Type: &q, Value: &pb.Payload_PropertyValue_LongValue{LongValue: v}}},
		}}
	}
	if q, ok := GetQuality(long(192)); q != 192 || !ok {
		t.Errorf("192 in long_value: %d, %v", q, ok)
	}
	if q, ok := GetQuality(long(1 << 40)); ok {
		t.Errorf("1<<40 read as quality %d", q)
	}
	s := long(0)
	s.Properties.Values[0].Value = &pb.Payload_PropertyValue_StringValue{StringValue: "GOOD"}
	if q, ok := GetQuality(s); ok {
		t.Errorf("a string read as quality %d", q)
	}
}

func TestDateTimeLimits(t *testing.T) {
	before := time.UnixMilli(-1)
	var m pb.Payload_Metric
	if err := SetValue(&m, pb.DataType_DateTime, before); !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("DateTime before the epoch: %v", err)
	}
	if err := SetValue(&m, pb.DataType_DateTimeArray, []time.Time{time.UnixMilli(0), before}); !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("DateTimeArray before the epoch: %v", err)
	}
	if _, err := dateTimeMillis(time.Unix(math.MaxInt64/1000+1, 0)); !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("DateTime past int64 milliseconds: %v", err)
	}
	dt := uint32(pb.DataType_DateTimeArray)
	a := &pb.Payload_Metric{Datatype: &dt, Value: &pb.Payload_Metric_BytesValue{BytesValue: []byte{0, 0, 0, 0, 0, 0, 0, 0x80}}}
	if got, err := GetValue(a); !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("DateTimeArray of 1<<63: %v, %v", got, err)
	}
}

func TestIntegerHelpersUnknownType(t *testing.T) {
	if _, err := decodeInteger(pb.DataType_String, 0); !errors.Is(err, ErrUnknownType) {
		t.Errorf("decodeInteger: %v", err)
	}
	if _, err := encodeInteger(pb.DataType_String, 0); !errors.Is(err, ErrUnknownType) {
		t.Errorf("encodeInteger: %v", err)
	}
}
