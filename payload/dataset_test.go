// SPDX-License-Identifier: MIT

package payload

import (
	"testing"

	pb "github.com/otfabric/go-sparkplugb/proto"
)

func TestDataSetRoundTrip(t *testing.T) {
	columns := []string{"name", "value", "active"}
	types := []pb.DataType{pb.DataType_String, pb.DataType_Double, pb.DataType_Boolean}
	rows := [][]any{
		{"sensor1", float64(22.5), true},
		{"sensor2", float64(33.1), false},
	}

	m, err := NewDataSet("readings", columns, types, rows)
	if err != nil {
		t.Fatal(err)
	}

	// Encode/decode round trip
	p := NewPayload(0, m)
	b, err := Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}

	cols, dts, gotRows, err := GetDataSetValues(p2.Metrics[0])
	if err != nil {
		t.Fatal(err)
	}

	if len(cols) != 3 {
		t.Fatalf("columns = %d, want 3", len(cols))
	}
	if cols[0] != "name" || cols[1] != "value" || cols[2] != "active" {
		t.Errorf("columns = %v", cols)
	}
	if len(dts) != 3 {
		t.Fatalf("types = %d, want 3", len(dts))
	}
	if len(gotRows) != 2 {
		t.Fatalf("rows = %d, want 2", len(gotRows))
	}
	if gotRows[0][0] != "sensor1" {
		t.Errorf("row[0][0] = %v, want sensor1", gotRows[0][0])
	}
	if gotRows[0][1] != float64(22.5) {
		t.Errorf("row[0][1] = %v, want 22.5", gotRows[0][1])
	}
	if gotRows[0][2] != true {
		t.Errorf("row[0][2] = %v, want true", gotRows[0][2])
	}
	if gotRows[1][0] != "sensor2" {
		t.Errorf("row[1][0] = %v, want sensor2", gotRows[1][0])
	}
}

func TestDataSetAllColumnTypes(t *testing.T) {
	columns := []string{"u32", "u64", "f32", "f64", "b", "s"}
	types := []pb.DataType{
		pb.DataType_UInt32, pb.DataType_UInt64,
		pb.DataType_Float, pb.DataType_Double,
		pb.DataType_Boolean, pb.DataType_String,
	}
	rows := [][]any{
		{uint32(42), uint64(100), float32(3.14), float64(2.718), true, "hello"},
	}

	m, err := NewDataSet("all_types", columns, types, rows)
	if err != nil {
		t.Fatal(err)
	}

	p := NewPayload(0, m)
	b, _ := Encode(p)
	p2, _ := Decode(b)
	_, _, gotRows, err := GetDataSetValues(p2.Metrics[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(gotRows) != 1 || len(gotRows[0]) != 6 {
		t.Fatalf("unexpected rows shape")
	}
	if gotRows[0][0] != uint32(42) {
		t.Errorf("u32 = %v", gotRows[0][0])
	}
	if gotRows[0][1] != uint64(100) {
		t.Errorf("u64 = %v", gotRows[0][1])
	}
	if gotRows[0][4] != true {
		t.Errorf("bool = %v", gotRows[0][4])
	}
}

func TestDataSetIntegerTypes(t *testing.T) {
	for _, dt := range []pb.DataType{
		pb.DataType_Int8, pb.DataType_Int16, pb.DataType_Int32,
		pb.DataType_UInt8, pb.DataType_UInt16,
	} {
		t.Run(dt.String(), func(t *testing.T) {
			m, err := NewDataSet("ds", []string{"v"}, []pb.DataType{dt}, [][]any{{uint32(5)}})
			if err != nil {
				t.Fatal(err)
			}
			if m.GetDatasetValue() == nil {
				t.Fatal("expected dataset value")
			}
		})
	}
}

func TestDataSetInt64Types(t *testing.T) {
	for _, dt := range []pb.DataType{pb.DataType_Int64} {
		t.Run(dt.String(), func(t *testing.T) {
			m, err := NewDataSet("ds", []string{"v"}, []pb.DataType{dt}, [][]any{{uint64(99)}})
			if err != nil {
				t.Fatal(err)
			}
			if m.GetDatasetValue() == nil {
				t.Fatal("expected dataset value")
			}
		})
	}
}

func TestDataSetTextType(t *testing.T) {
	m, err := NewDataSet("ds", []string{"v"}, []pb.DataType{pb.DataType_Text}, [][]any{{"text"}})
	if err != nil {
		t.Fatal(err)
	}
	if m.GetDatasetValue() == nil {
		t.Fatal("expected dataset value")
	}
}

func TestDataSetColumnMismatch(t *testing.T) {
	_, err := NewDataSet("x", []string{"a", "b"}, []pb.DataType{pb.DataType_String}, nil)
	if err != ErrColumnMismatch {
		t.Errorf("expected ErrColumnMismatch, got %v", err)
	}
}

func TestDataSetRowColumnMismatch(t *testing.T) {
	_, err := NewDataSet("x",
		[]string{"a"},
		[]pb.DataType{pb.DataType_String},
		[][]any{{"one", "two"}})
	if err != ErrColumnMismatch {
		t.Errorf("expected ErrColumnMismatch for row mismatch, got %v", err)
	}
}

func TestDataSetTypeMismatch(t *testing.T) {
	_, err := NewDataSet("x",
		[]string{"a"},
		[]pb.DataType{pb.DataType_UInt32},
		[][]any{{"not uint32"}})
	if err != ErrTypeMismatch {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
}

func TestDataSetUnknownType(t *testing.T) {
	_, err := NewDataSet("x",
		[]string{"a"},
		[]pb.DataType{pb.DataType_Bytes},
		[][]any{{[]byte{1}}})
	if err != ErrUnknownType {
		t.Errorf("expected ErrUnknownType, got %v", err)
	}
}

func TestGetDataSetValuesNilMetric(t *testing.T) {
	_, _, _, err := GetDataSetValues(nil)
	if err != ErrNilMetric {
		t.Errorf("expected ErrNilMetric, got %v", err)
	}
}

func TestGetDataSetValuesNoDataset(t *testing.T) {
	m, _ := NewMetric("x", pb.DataType_Int32, int32(0))
	_, _, _, err := GetDataSetValues(m)
	if err != ErrTypeMismatch {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
}

func TestDataSetEmptyRows(t *testing.T) {
	m, err := NewDataSet("empty",
		[]string{"a"},
		[]pb.DataType{pb.DataType_String},
		nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, rows, err := GetDataSetValues(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestExtractDataSetValueNil(t *testing.T) {
	v, err := extractDataSetValue(pb.DataType_Int8, nil)
	if v != nil || err != nil {
		t.Errorf("expected nil, got %v, %v", v, err)
	}
}

func TestExtractDataSetValueUnknown(t *testing.T) {
	dsv := &pb.Payload_DataSet_DataSetValue{}
	for _, dt := range []pb.DataType{pb.DataType_Int8, pb.DataType_String} {
		v, err := extractDataSetValue(dt, dsv)
		if v != nil || err != nil {
			t.Errorf("%v: expected nil for empty value, got %v, %v", dt, v, err)
		}
	}
}

func TestNewDataSetValueAllMismatches(t *testing.T) {
	tests := []struct {
		dt  pb.DataType
		val any
	}{
		{pb.DataType_UInt64, "not uint64"},
		{pb.DataType_Float, "not float"},
		{pb.DataType_Double, "not double"},
		{pb.DataType_Boolean, "not bool"},
		{pb.DataType_String, 42},
	}
	for _, tc := range tests {
		_, err := newDataSetValue(tc.dt, tc.val)
		if err != ErrTypeMismatch {
			t.Errorf("dt=%v: expected ErrTypeMismatch, got %v", tc.dt, err)
		}
	}
}
