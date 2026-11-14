// SPDX-License-Identifier: MIT

package payload

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/otfabric/go-sparkplugb/proto"
)

// The fields a value can be carried in, by name. A getter decodes the field
// of the datatype, reads no value as the zero value, and returns
// ErrTypeMismatch for every other field.

var metricKinds = map[string]func() any{
	"int":       func() any { return &pb.Payload_Metric_IntValue{} },
	"long":      func() any { return &pb.Payload_Metric_LongValue{} },
	"float":     func() any { return &pb.Payload_Metric_FloatValue{} },
	"double":    func() any { return &pb.Payload_Metric_DoubleValue{} },
	"boolean":   func() any { return &pb.Payload_Metric_BooleanValue{} },
	"string":    func() any { return &pb.Payload_Metric_StringValue{} },
	"bytes":     func() any { return &pb.Payload_Metric_BytesValue{BytesValue: make([]byte, 8)} },
	"dataset":   func() any { return &pb.Payload_Metric_DatasetValue{DatasetValue: &pb.Payload_DataSet{}} },
	"template":  func() any { return &pb.Payload_Metric_TemplateValue{TemplateValue: &pb.Payload_Template{}} },
	"extension": func() any { return &pb.Payload_Metric_ExtensionValue{} },
}

// setValue sets the oneof field Value of the message msg points to.
func setValue(msg any, v any) {
	reflect.ValueOf(msg).Elem().FieldByName("Value").Set(reflect.ValueOf(v))
}

// metricField is the field a value of dt is carried in.
func metricField(dt pb.DataType) []string {
	switch {
	case isIntegerType(dt):
		return []string{"int", "long"}
	case isArrayType(dt):
		return []string{"bytes"}
	}
	switch dt {
	case pb.DataType_Float:
		return []string{"float"}
	case pb.DataType_Double:
		return []string{"double"}
	case pb.DataType_Boolean:
		return []string{"boolean"}
	case pb.DataType_String, pb.DataType_Text, pb.DataType_UUID:
		return []string{"string"}
	case pb.DataType_Bytes, pb.DataType_File:
		return []string{"bytes"}
	case pb.DataType_DataSet:
		return []string{"dataset"}
	case pb.DataType_Template:
		return []string{"template"}
	}
	return nil
}

func TestGetValueOfAnotherKind(t *testing.T) {
	for d := pb.DataType_Int8; d <= pb.DataType_DateTimeArray; d++ {
		if d == pb.DataType_PropertySet || d == pb.DataType_PropertySetList {
			continue // not a metric datatype: ErrUnknownType
		}
		fields := metricField(d)
		dt := uint32(d)
		for name, kind := range metricKinds {
			m := &pb.Payload_Metric{Datatype: &dt}
			setValue(m, kind())
			v, err := GetValue(m)
			want := false
			for _, f := range fields {
				want = want || f == name
			}
			if want && err != nil {
				t.Errorf("%v in %s: %v", d, name, err)
			}
			if !want && !errors.Is(err, ErrTypeMismatch) {
				t.Errorf("%v in %s: %#v, %v; want ErrTypeMismatch", d, name, v, err)
			}
		}

		// No value: the zero value, an array as decoded from no bytes.
		got, err := GetValue(&pb.Payload_Metric{Datatype: &dt})
		var want any
		var wantErr error
		if isArrayType(d) {
			want, wantErr = GetValue(&pb.Payload_Metric{Datatype: &dt, Value: &pb.Payload_Metric_BytesValue{}})
		} else {
			want = zeroOf(d)
		}
		if !reflect.DeepEqual(got, want) || !errors.Is(err, wantErr) {
			t.Errorf("%v without a value: %#v, %v; want %#v, %v", d, got, err, want, wantErr)
		}
	}
}

// zeroOf is the zero value of the Go type of a scalar datatype.
func zeroOf(dt pb.DataType) any {
	switch dt {
	case pb.DataType_Int8:
		return int8(0)
	case pb.DataType_Int16:
		return int16(0)
	case pb.DataType_Int32:
		return int32(0)
	case pb.DataType_Int64:
		return int64(0)
	case pb.DataType_UInt8:
		return uint8(0)
	case pb.DataType_UInt16:
		return uint16(0)
	case pb.DataType_UInt32:
		return uint32(0)
	case pb.DataType_UInt64:
		return uint64(0)
	case pb.DataType_DateTime:
		return time.UnixMilli(0).UTC()
	case pb.DataType_Float:
		return float32(0)
	case pb.DataType_Double:
		return float64(0)
	case pb.DataType_Boolean:
		return false
	case pb.DataType_String, pb.DataType_Text, pb.DataType_UUID:
		return ""
	case pb.DataType_Bytes, pb.DataType_File:
		return []byte(nil)
	case pb.DataType_DataSet:
		return (*pb.Payload_DataSet)(nil)
	case pb.DataType_Template:
		return (*pb.Payload_Template)(nil)
	}
	return nil
}

// basicField is the field of a DataSet cell, template parameter or
// property of a basic datatype.
func basicField(dt pb.DataType) string {
	switch {
	case isIntegerType(dt):
		return "int long"
	case dt == pb.DataType_Float:
		return "float"
	case dt == pb.DataType_Double:
		return "double"
	case dt == pb.DataType_Boolean:
		return "boolean"
	}
	return "string" // String, Text
}

// carries reports whether name is one of the space-separated fields.
func carries(fields, name string) bool {
	for _, f := range strings.Fields(fields) {
		if f == name {
			return true
		}
	}
	return false
}

func TestTemplateParameterOfAnotherKind(t *testing.T) {
	kinds := map[string]func() any{
		"int":       func() any { return &pb.Payload_Template_Parameter_IntValue{} },
		"long":      func() any { return &pb.Payload_Template_Parameter_LongValue{} },
		"float":     func() any { return &pb.Payload_Template_Parameter_FloatValue{} },
		"double":    func() any { return &pb.Payload_Template_Parameter_DoubleValue{} },
		"boolean":   func() any { return &pb.Payload_Template_Parameter_BooleanValue{} },
		"string":    func() any { return &pb.Payload_Template_Parameter_StringValue{} },
		"extension": func() any { return &pb.Payload_Template_Parameter_ExtensionValue{} },
	}
	for d := pb.DataType_Int8; d <= pb.DataType_Text; d++ {
		dt := uint32(d)
		for name, kind := range kinds {
			p := &pb.Payload_Template_Parameter{Type: &dt}
			setValue(p, kind())
			v, err := GetTemplateParameterValue(p)
			if carries(basicField(d), name) {
				if err != nil {
					t.Errorf("%v in %s: %v", d, name, err)
				}
			} else if !errors.Is(err, ErrTypeMismatch) {
				t.Errorf("%v in %s: %#v, %v; want ErrTypeMismatch", d, name, v, err)
			}
		}
		if v, err := GetTemplateParameterValue(&pb.Payload_Template_Parameter{Type: &dt}); err != nil || !reflect.DeepEqual(v, zeroOf(d)) {
			t.Errorf("%v without a value: %#v, %v; want %#v", d, v, err, zeroOf(d))
		}
	}
}

func TestPropertyValueOfAnotherKind(t *testing.T) {
	kinds := map[string]func() any{
		"int":     func() any { return &pb.Payload_PropertyValue_IntValue{} },
		"long":    func() any { return &pb.Payload_PropertyValue_LongValue{} },
		"float":   func() any { return &pb.Payload_PropertyValue_FloatValue{} },
		"double":  func() any { return &pb.Payload_PropertyValue_DoubleValue{} },
		"boolean": func() any { return &pb.Payload_PropertyValue_BooleanValue{} },
		"string":  func() any { return &pb.Payload_PropertyValue_StringValue{} },
		"propertyset": func() any {
			return &pb.Payload_PropertyValue_PropertysetValue{PropertysetValue: &pb.Payload_PropertySet{}}
		},
		"propertysets": func() any {
			return &pb.Payload_PropertyValue_PropertysetsValue{PropertysetsValue: &pb.Payload_PropertySetList{}}
		},
		"extension": func() any { return &pb.Payload_PropertyValue_ExtensionValue{} },
	}
	types := []pb.DataType{pb.DataType_PropertySet, pb.DataType_PropertySetList}
	for d := pb.DataType_Int8; d <= pb.DataType_Text; d++ {
		types = append(types, d)
	}
	for _, d := range types {
		dt := uint32(d)
		fields := basicField(d)
		zero := zeroOf(d)
		switch d {
		case pb.DataType_PropertySet:
			fields, zero = "propertyset", (*pb.Payload_PropertySet)(nil)
		case pb.DataType_PropertySetList:
			fields, zero = "propertysets", (*pb.Payload_PropertySetList)(nil)
		}
		for name, kind := range kinds {
			p := &pb.Payload_PropertyValue{Type: &dt}
			setValue(p, kind())
			v, err := GetPropertyValue(p)
			if carries(fields, name) {
				if err != nil {
					t.Errorf("%v in %s: %v", d, name, err)
				}
			} else if !errors.Is(err, ErrTypeMismatch) {
				t.Errorf("%v in %s: %#v, %v; want ErrTypeMismatch", d, name, v, err)
			}
		}
		if v, err := GetPropertyValue(&pb.Payload_PropertyValue{Type: &dt}); err != nil || !reflect.DeepEqual(v, zero) {
			t.Errorf("%v without a value: %#v, %v; want %#v", d, v, err, zero)
		}
	}
}

func TestDataSetCellOfAnotherKind(t *testing.T) {
	kinds := map[string]func() any{
		"int":       func() any { return &pb.Payload_DataSet_DataSetValue_IntValue{} },
		"long":      func() any { return &pb.Payload_DataSet_DataSetValue_LongValue{} },
		"float":     func() any { return &pb.Payload_DataSet_DataSetValue_FloatValue{} },
		"double":    func() any { return &pb.Payload_DataSet_DataSetValue_DoubleValue{} },
		"boolean":   func() any { return &pb.Payload_DataSet_DataSetValue_BooleanValue{} },
		"string":    func() any { return &pb.Payload_DataSet_DataSetValue_StringValue{} },
		"extension": func() any { return &pb.Payload_DataSet_DataSetValue_ExtensionValue{} },
	}
	dataset := func(dt pb.DataType, cell *pb.Payload_DataSet_DataSetValue) *pb.Payload_Metric {
		return &pb.Payload_Metric{Value: &pb.Payload_Metric_DatasetValue{DatasetValue: &pb.Payload_DataSet{
			NumOfColumns: uint64Ptr(1),
			Columns:      []string{"c"},
			Types:        []uint32{uint32(dt)},
			Rows:         []*pb.Payload_DataSet_Row{{Elements: []*pb.Payload_DataSet_DataSetValue{cell}}},
		}}}
	}
	for d := pb.DataType_Int8; d <= pb.DataType_Text; d++ {
		for name, kind := range kinds {
			cell := &pb.Payload_DataSet_DataSetValue{}
			setValue(cell, kind())
			_, _, rows, err := GetDataSetValues(dataset(d, cell))
			if carries(basicField(d), name) {
				if err != nil {
					t.Errorf("%v in %s: %v", d, name, err)
				} else if got := reflect.TypeOf(rows[0][0]); got != reflect.TypeOf(zeroOf(d)) {
					t.Errorf("%v in %s: a %v, want the Go type of the column, %T", d, name, got, zeroOf(d))
				}
			} else if !errors.Is(err, ErrTypeMismatch) {
				t.Errorf("%v in %s: %v, %v; want ErrTypeMismatch", d, name, rows, err)
			}
		}
		if _, _, rows, err := GetDataSetValues(dataset(d, &pb.Payload_DataSet_DataSetValue{})); err != nil || rows[0][0] != nil {
			t.Errorf("%v cell without a value: %v, %v; want nil", d, rows, err)
		}
	}
	// A cell with a value in a column of a type that is not basic.
	str := &pb.Payload_DataSet_DataSetValue{Value: &pb.Payload_DataSet_DataSetValue_StringValue{StringValue: "x"}}
	for _, d := range []pb.DataType{pb.DataType_Unknown, pb.DataType_UUID, pb.DataType_DataSet, pb.DataType_StringArray} {
		if _, _, rows, err := GetDataSetValues(dataset(d, str)); !errors.Is(err, ErrUnknownType) {
			t.Errorf("%v column with a string cell: %v, %v; want ErrUnknownType", d, rows, err)
		}
		if _, _, rows, err := GetDataSetValues(dataset(d, &pb.Payload_DataSet_DataSetValue{})); err != nil || rows[0][0] != nil {
			t.Errorf("%v column, cell without a value: %v, %v; want nil", d, rows, err)
		}
	}
}

func TestDataSetColumnCounts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		columns []string
		types   []pb.DataType
		num     *uint64
		want    error
	}{
		{"two columns, one type", []string{"a", "b"}, []pb.DataType{pb.DataType_Int32}, nil, ErrColumnMismatch},
		{"one column, two types", []string{"a"}, []pb.DataType{pb.DataType_Int32, pb.DataType_Int32}, nil, ErrColumnMismatch},
		{"types without columns", nil, []pb.DataType{pb.DataType_Int32}, nil, ErrColumnMismatch},
		{"num_of_columns above", []string{"a"}, []pb.DataType{pb.DataType_Int32}, uint64Ptr(2), ErrColumnMismatch},
		{"num_of_columns below", []string{"a", "b"}, []pb.DataType{pb.DataType_Int32, pb.DataType_Int32}, uint64Ptr(1), ErrColumnMismatch},
		{"num_of_columns 0 of 1", []string{"a"}, []pb.DataType{pb.DataType_Int32}, uint64Ptr(0), ErrColumnMismatch},
		{"agreeing", []string{"a", "b"}, []pb.DataType{pb.DataType_Int32, pb.DataType_String}, uint64Ptr(2), nil},
		{"agreeing without num_of_columns", []string{"a"}, []pb.DataType{pb.DataType_Int32}, nil, nil},
		{"empty", nil, nil, uint64Ptr(0), nil},
	} {
		ds := &pb.Payload_DataSet{NumOfColumns: tc.num, Columns: tc.columns}
		for _, dt := range tc.types {
			ds.Types = append(ds.Types, uint32(dt))
		}
		m := &pb.Payload_Metric{Value: &pb.Payload_Metric_DatasetValue{DatasetValue: ds}}
		if _, _, _, err := GetDataSetValues(m); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, err, tc.want)
		}
	}
}

func TestQualityOfAnotherType(t *testing.T) {
	quality := func(dt *uint32, v *pb.Payload_PropertyValue) *pb.Payload_Metric {
		v.Type = dt
		return &pb.Payload_Metric{Properties: &pb.Payload_PropertySet{Keys: []string{"quality"}, Values: []*pb.Payload_PropertyValue{v}}}
	}
	int32Type, int64Type, stringType := uint32(pb.DataType_Int32), uint32(pb.DataType_Int64), uint32(pb.DataType_String)
	if q, ok := GetQuality(quality(&int32Type, &pb.Payload_PropertyValue{Value: &pb.Payload_PropertyValue_IntValue{IntValue: 192}})); !ok || q != QualityGood {
		t.Errorf("Int32 quality: %d, %v", q, ok)
	}
	for name, m := range map[string]*pb.Payload_Metric{
		"Int64":   quality(&int64Type, &pb.Payload_PropertyValue{Value: &pb.Payload_PropertyValue_LongValue{LongValue: 192}}),
		"String":  quality(&stringType, &pb.Payload_PropertyValue{Value: &pb.Payload_PropertyValue_IntValue{IntValue: 192}}),
		"no type": quality(nil, &pb.Payload_PropertyValue{Value: &pb.Payload_PropertyValue_IntValue{IntValue: 192}}),
	} {
		if q, ok := GetQuality(m); ok {
			t.Errorf("quality of type %s: %d, true; want false", name, q)
		}
	}
}
