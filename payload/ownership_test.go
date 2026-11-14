// SPDX-License-Identifier: MIT

package payload

import (
	"testing"

	pb "github.com/otfabric/go-sparkplugb/proto"
)

// GetValue returns the []byte of Bytes and File, the DataSet and the
// Template of the metric itself, and arrays as new slices, as its doc
// comment says.
func TestGetValueOwnership(t *testing.T) {
	for _, dt := range []pb.DataType{pb.DataType_Bytes, pb.DataType_File} {
		m, err := NewMetric("b", dt, []byte{1, 2})
		if err != nil {
			t.Fatal(err)
		}
		v, err := GetValue(m)
		if err != nil {
			t.Fatal(err)
		}
		v.([]byte)[0] = 9
		if m.GetBytesValue()[0] != 9 {
			t.Errorf("%v: the []byte GetValue returned is not the metric's own", dt)
		}
	}

	arr, err := NewMetric("a", pb.DataType_Int32Array, []int32{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	v, err := GetValue(arr)
	if err != nil {
		t.Fatal(err)
	}
	v.([]int32)[0] = 9
	if again, _ := GetValue(arr); again.([]int32)[0] != 1 {
		t.Error("changing the slice of an array changed the metric")
	}

	ds, err := NewDataSet("ds", []string{"c"}, []pb.DataType{pb.DataType_Int32}, [][]any{{int32(1)}})
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := GetValue(ds); v.(*pb.Payload_DataSet) != ds.GetDatasetValue() {
		t.Error("the DataSet GetValue returned is not the metric's own")
	}
	tpl, err := NewTemplateDefinition("T", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := GetValue(tpl); v.(*pb.Payload_Template) != tpl.GetTemplateValue() {
		t.Error("the Template GetValue returned is not the metric's own")
	}
}
