// SPDX-License-Identifier: MIT

package payload

import (
	"errors"

	pb "github.com/otfabric/go-sparkplugb/proto"
)

var ErrColumnMismatch = errors.New("sparkplugb: column count mismatch")

// NewDataSet creates a DataSet metric. columns defines the column names,
// types the DataType for each column, and rows is a [][]any where each
// inner slice has one value per column.
//
// A value has the Go type GetValue returns for its column's datatype. For an
// integer column the wire type, uint32 or uint64, is also taken, as long as it
// is a value of the column's datatype. A value that is not returns
// ErrTypeMismatch.
func NewDataSet(name string, columns []string, types []pb.DataType, rows [][]any) (*pb.Payload_Metric, error) {
	if len(columns) != len(types) {
		return nil, ErrColumnMismatch
	}

	typeUints := make([]uint32, len(types))
	for i, t := range types {
		typeUints[i] = uint32(t)
	}

	ds := &pb.Payload_DataSet{
		NumOfColumns: uint64Ptr(uint64(len(columns))),
		Columns:      columns,
		Types:        typeUints,
	}

	for _, row := range rows {
		if len(row) != len(columns) {
			return nil, ErrColumnMismatch
		}
		dsRow := &pb.Payload_DataSet_Row{
			Elements: make([]*pb.Payload_DataSet_DataSetValue, len(row)),
		}
		for j, v := range row {
			dsv, err := newDataSetValue(types[j], v)
			if err != nil {
				return nil, err
			}
			dsRow.Elements[j] = dsv
		}
		ds.Rows = append(ds.Rows, dsRow)
	}

	dt := uint32(pb.DataType_DataSet)
	m := &pb.Payload_Metric{
		Name:     &name,
		Datatype: &dt,
		Value:    &pb.Payload_Metric_DatasetValue{DatasetValue: ds},
	}
	return m, nil
}

// GetDataSetValues extracts all rows from a DataSet metric as [][]any. A value
// has the Go type GetValue returns for its column's datatype: integers and
// DateTime are decoded by the rules of GetValue, and one that is not a value of
// its column returns ErrTypeMismatch. A cell without a value is nil. A row with
// more cells than the DataSet has types returns ErrColumnMismatch.
func GetDataSetValues(m *pb.Payload_Metric) (columns []string, types []pb.DataType, rows [][]any, err error) {
	if m == nil {
		return nil, nil, nil, ErrNilMetric
	}
	ds := m.GetDatasetValue()
	if ds == nil {
		return nil, nil, nil, ErrTypeMismatch
	}

	columns = ds.GetColumns()
	for _, t := range ds.GetTypes() {
		types = append(types, pb.DataType(t))
	}

	for _, row := range ds.GetRows() {
		vals := make([]any, len(row.GetElements()))
		for j, elem := range row.GetElements() {
			if j >= len(types) {
				return nil, nil, nil, ErrColumnMismatch
			}
			vals[j], err = extractDataSetValue(types[j], elem)
			if err != nil {
				return nil, nil, nil, err
			}
		}
		rows = append(rows, vals)
	}
	return columns, types, rows, nil
}

func newDataSetValue(dt pb.DataType, v any) (*pb.Payload_DataSet_DataSetValue, error) {
	dsv := &pb.Payload_DataSet_DataSetValue{}
	if isIntegerType(dt) {
		u, err := encodeIntegerOrRaw(dt, v)
		if err != nil {
			return nil, err
		}
		if usesLongValue(dt) {
			dsv.Value = &pb.Payload_DataSet_DataSetValue_LongValue{LongValue: u}
		} else {
			dsv.Value = &pb.Payload_DataSet_DataSetValue_IntValue{IntValue: uint32(u)}
		}
		return dsv, nil
	}
	switch dt {
	case pb.DataType_Float:
		val, ok := v.(float32)
		if !ok {
			return nil, ErrTypeMismatch
		}
		dsv.Value = &pb.Payload_DataSet_DataSetValue_FloatValue{FloatValue: val}
	case pb.DataType_Double:
		val, ok := v.(float64)
		if !ok {
			return nil, ErrTypeMismatch
		}
		dsv.Value = &pb.Payload_DataSet_DataSetValue_DoubleValue{DoubleValue: val}
	case pb.DataType_Boolean:
		val, ok := v.(bool)
		if !ok {
			return nil, ErrTypeMismatch
		}
		dsv.Value = &pb.Payload_DataSet_DataSetValue_BooleanValue{BooleanValue: val}
	case pb.DataType_String, pb.DataType_Text:
		val, ok := v.(string)
		if !ok {
			return nil, ErrTypeMismatch
		}
		dsv.Value = &pb.Payload_DataSet_DataSetValue_StringValue{StringValue: val}
	default:
		return nil, ErrUnknownType
	}
	return dsv, nil
}

func extractDataSetValue(dt pb.DataType, dsv *pb.Payload_DataSet_DataSetValue) (any, error) {
	if dsv == nil {
		return nil, nil
	}
	if isIntegerType(dt) {
		switch v := dsv.Value.(type) {
		case nil:
			return nil, nil
		case *pb.Payload_DataSet_DataSetValue_IntValue:
			return decodeInteger(dt, uint64(v.IntValue))
		case *pb.Payload_DataSet_DataSetValue_LongValue:
			return decodeInteger(dt, v.LongValue)
		}
		return nil, ErrTypeMismatch
	}
	switch v := dsv.Value.(type) {
	case *pb.Payload_DataSet_DataSetValue_IntValue:
		return v.IntValue, nil
	case *pb.Payload_DataSet_DataSetValue_LongValue:
		return v.LongValue, nil
	case *pb.Payload_DataSet_DataSetValue_FloatValue:
		return v.FloatValue, nil
	case *pb.Payload_DataSet_DataSetValue_DoubleValue:
		return v.DoubleValue, nil
	case *pb.Payload_DataSet_DataSetValue_BooleanValue:
		return v.BooleanValue, nil
	case *pb.Payload_DataSet_DataSetValue_StringValue:
		return v.StringValue, nil
	default:
		return nil, nil
	}
}

func uint64Ptr(v uint64) *uint64 { return &v }
