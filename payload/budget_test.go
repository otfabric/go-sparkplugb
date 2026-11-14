// SPDX-License-Identifier: MIT

package payload

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	pb "github.com/otfabric/go-sparkplugb/proto"
)

// The getters refuse a string above MaxStringBytes, and a BooleanArray above
// MaxArrayElements, before they allocate for it, and allocate in proportion
// to the bytes of a value they accept. A StringArray is counted as it is
// decoded: it allocates for at most MaxArrayElements elements, 16 bytes of
// string header each and the growth of the slice, before one more is
// refused.
func TestDecodeLimitsAllocationBudget(t *testing.T) {
	metric := func(dt pb.DataType, v any) *pb.Payload_Metric {
		d := uint32(dt)
		m := &pb.Payload_Metric{Datatype: &d}
		setValue(m, v)
		return m
	}
	byteValue := func(dt pb.DataType, b []byte) *pb.Payload_Metric {
		return metric(dt, &pb.Payload_Metric_BytesValue{BytesValue: b})
	}
	count := func(n uint32, bits int) []byte {
		b := binary.LittleEndian.AppendUint32(nil, n)
		return append(b, make([]byte, bits)...)
	}
	long := strings.Repeat("s", MaxStringBytes+1)
	const small = 1 << 10 // what refusing a value may allocate

	for _, tc := range []struct {
		name   string
		m      *pb.Payload_Metric
		err    error
		budget uint64
	}{
		{"BooleanArray of 2^32-1 elements", byteValue(pb.DataType_BooleanArray, count(1<<32-1, 1)), ErrArrayTooLarge, small},
		{"BooleanArray of MaxArrayElements+1 elements", byteValue(pb.DataType_BooleanArray, count(MaxArrayElements+1, 1)), ErrArrayTooLarge, small},
		{"BooleanArray of MaxArrayElements elements without their bits", byteValue(pb.DataType_BooleanArray, count(MaxArrayElements, 1)), ErrInvalidArray, small},
		{"BooleanArray of MaxArrayElements elements", byteValue(pb.DataType_BooleanArray, count(MaxArrayElements, MaxArrayElements/8)), nil, 2 * MaxArrayElements},
		{"String of MaxStringBytes+1 bytes", metric(pb.DataType_String, &pb.Payload_Metric_StringValue{StringValue: long}), ErrStringTooLarge, small},
		{"StringArray element of MaxStringBytes+1 bytes", byteValue(pb.DataType_StringArray, []byte(long+"\x00")), ErrStringTooLarge, small},
		{"StringArray of MaxArrayElements elements", byteValue(pb.DataType_StringArray, bytes.Repeat([]byte{0}, MaxArrayElements)), nil, 96 * MaxArrayElements},
		{"StringArray of MaxArrayElements+1 elements", byteValue(pb.DataType_StringArray, bytes.Repeat([]byte{0}, MaxArrayElements+1)), ErrArrayTooLarge, 96 * MaxArrayElements},
		{"Int64Array of 1 MiB", byteValue(pb.DataType_Int64Array, make([]byte, 1<<20)), nil, 2 << 20},
		{"Int64Array of 1 MiB + 1 byte", byteValue(pb.DataType_Int64Array, make([]byte, 1<<20+1)), ErrInvalidArray, small},
	} {
		var err error
		n := allocated(func() { _, err = GetValue(tc.m) })
		if !errors.Is(err, tc.err) {
			t.Errorf("%s: %v, want %v", tc.name, err, tc.err)
		}
		if n > tc.budget {
			t.Errorf("%s: %d bytes allocated, want at most %d", tc.name, n, tc.budget)
		}
	}
}
