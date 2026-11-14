// SPDX-License-Identifier: MIT

package payload

import (
	"errors"
	"reflect"
	"testing"
	"time"

	pb "github.com/otfabric/go-sparkplugb/proto"
	"google.golang.org/protobuf/proto"
)

// goType is the Go type GetValue documents for dt, nil for a datatype it
// does not decode.
func goType(dt pb.DataType) reflect.Type {
	switch dt {
	case pb.DataType_Int8Array:
		return reflect.TypeFor[[]int8]()
	case pb.DataType_Int16Array:
		return reflect.TypeFor[[]int16]()
	case pb.DataType_Int32Array:
		return reflect.TypeFor[[]int32]()
	case pb.DataType_Int64Array:
		return reflect.TypeFor[[]int64]()
	case pb.DataType_UInt8Array:
		return reflect.TypeFor[[]uint8]()
	case pb.DataType_UInt16Array:
		return reflect.TypeFor[[]uint16]()
	case pb.DataType_UInt32Array:
		return reflect.TypeFor[[]uint32]()
	case pb.DataType_UInt64Array:
		return reflect.TypeFor[[]uint64]()
	case pb.DataType_FloatArray:
		return reflect.TypeFor[[]float32]()
	case pb.DataType_DoubleArray:
		return reflect.TypeFor[[]float64]()
	case pb.DataType_BooleanArray:
		return reflect.TypeFor[[]bool]()
	case pb.DataType_StringArray:
		return reflect.TypeFor[[]string]()
	case pb.DataType_DateTimeArray:
		return reflect.TypeFor[[]time.Time]()
	case pb.DataType_PropertySet:
		return reflect.TypeFor[*pb.Payload_PropertySet]()
	case pb.DataType_PropertySetList:
		return reflect.TypeFor[*pb.Payload_PropertySetList]()
	}
	if z := zeroOf(dt); z != nil {
		return reflect.TypeOf(z)
	}
	return nil
}

// documented reports whether err is one of the errors the getters document.
func documented(err error) bool {
	for _, e := range []error{ErrTypeMismatch, ErrInvalidArray, ErrUnknownType, ErrNilMetric,
		ErrArrayTooLarge, ErrStringTooLarge, ErrColumnMismatch} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// checkGot fails t unless the getter returned a documented error, or a value
// of the Go type of dt (nil where the getter documents nil).
func checkGot(t *testing.T, what string, dt pb.DataType, v any, err error, nilOK bool) {
	t.Helper()
	if err != nil {
		if !documented(err) {
			t.Fatalf("%s of %v: undocumented error %v", what, dt, err)
		}
		return
	}
	if v == nil && nilOK {
		return
	}
	if want := goType(dt); want == nil || reflect.TypeOf(v) != want {
		t.Fatalf("%s of %v: %T, want %v", what, dt, v, want)
	}
}

// checkMetric checks the getters of a decoded metric: GetValue, and the
// getters of what the metric carries.
func checkMetric(t *testing.T, m *pb.Payload_Metric) {
	t.Helper()
	dt := pb.DataType(m.GetDatatype())
	v, err := GetValue(m)
	checkGot(t, "GetValue", dt, v, err, m.GetIsNull())
	if dt == pb.DataType_DataSet {
		checkDataSet(t, m)
	}
	for _, pv := range m.GetProperties().GetValues() {
		v, err := GetPropertyValue(pv)
		checkGot(t, "GetPropertyValue", pb.DataType(pv.GetType()), v, err, pv.GetIsNull())
	}
	if tpl := m.GetTemplateValue(); tpl != nil {
		for _, p := range tpl.GetParameters() {
			v, err := GetTemplateParameterValue(p)
			checkGot(t, "GetTemplateParameterValue", pb.DataType(p.GetType()), v, err, false)
		}
	}
}

// checkDataSet checks GetDataSetValues of a DataSet metric: columns and
// types of one count, rows no longer than the types, and every cell nil or of
// the Go type of its column.
func checkDataSet(t *testing.T, m *pb.Payload_Metric) {
	t.Helper()
	columns, types, rows, err := GetDataSetValues(m)
	if err != nil {
		if !documented(err) {
			t.Fatalf("GetDataSetValues: undocumented error %v", err)
		}
		return
	}
	if len(columns) != len(types) {
		t.Fatalf("GetDataSetValues: %d columns, %d types", len(columns), len(types))
	}
	for r, row := range rows {
		if len(row) > len(types) {
			t.Fatalf("GetDataSetValues: row %d has %d cells, %d types", r, len(row), len(types))
		}
		for c, cell := range row {
			checkGot(t, "GetDataSetValues", types[c], cell, nil, true)
		}
	}
}

func FuzzDataSetValues(f *testing.F) {
	for _, ds := range [][]pb.DataType{
		{pb.DataType_Int32, pb.DataType_String},
		{pb.DataType_Float, pb.DataType_Boolean, pb.DataType_DateTime},
	} {
		cols := make([]string, len(ds))
		row := make([]any, len(ds))
		for i, dt := range ds {
			cols[i] = dt.String()
			switch dt {
			case pb.DataType_Int32:
				row[i] = int32(-7)
			case pb.DataType_String:
				row[i] = "x"
			case pb.DataType_Float:
				row[i] = float32(1.5)
			case pb.DataType_Boolean:
				row[i] = true
			case pb.DataType_DateTime:
				row[i] = time.UnixMilli(1000).UTC()
			}
		}
		m, err := NewDataSet("ds", cols, ds, [][]any{row})
		if err != nil {
			f.Fatal(err)
		}
		b, _ := proto.Marshal(m.GetDatasetValue())
		f.Add(b)
	}
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		var ds pb.Payload_DataSet
		if proto.Unmarshal(data, &ds) != nil {
			return
		}
		dt := uint32(pb.DataType_DataSet)
		checkDataSet(t, &pb.Payload_Metric{Datatype: &dt, Value: &pb.Payload_Metric_DatasetValue{DatasetValue: &ds}})
	})
}

func FuzzTemplateParameterValue(f *testing.F) {
	for _, p := range []struct {
		dt pb.DataType
		v  any
	}{{pb.DataType_Int16, int16(-2)}, {pb.DataType_Double, 2.5}, {pb.DataType_Text, "t"}, {pb.DataType_Boolean, true}} {
		tp, err := NewTemplateParameter("p", p.dt, p.v)
		if err != nil {
			f.Fatal(err)
		}
		b, _ := proto.Marshal(tp)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var p pb.Payload_Template_Parameter
		if proto.Unmarshal(data, &p) != nil {
			return
		}
		v, err := GetTemplateParameterValue(&p)
		checkGot(t, "GetTemplateParameterValue", pb.DataType(p.GetType()), v, err, false)
	})
}

func FuzzPropertyValue(f *testing.F) {
	for _, p := range []struct {
		dt pb.DataType
		v  any
	}{{pb.DataType_Int32, int32(192)}, {pb.DataType_UInt64, uint64(1) << 40}, {pb.DataType_String, "s"},
		{pb.DataType_PropertySet, &pb.Payload_PropertySet{Keys: []string{"k"}, Values: []*pb.Payload_PropertyValue{{}}}}} {
		pv, err := NewPropertyValue(p.dt, p.v)
		if err != nil {
			f.Fatal(err)
		}
		b, _ := proto.Marshal(pv)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var p pb.Payload_PropertyValue
		if proto.Unmarshal(data, &p) != nil {
			return
		}
		v, err := GetPropertyValue(&p)
		checkGot(t, "GetPropertyValue", pb.DataType(p.GetType()), v, err, p.GetIsNull())
	})
}
