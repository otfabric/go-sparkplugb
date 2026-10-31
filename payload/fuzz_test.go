// SPDX-License-Identifier: MIT

package payload

import (
	"errors"
	"testing"

	pb "github.com/otfabric/go-sparkplugb/proto"
)

func FuzzDecode(f *testing.F) {
	// Seed with a valid minimal payload
	p := &pb.Payload{}
	ts := uint64(1000)
	seq := uint64(0)
	p.Timestamp = &ts
	p.Seq = &seq
	b, _ := Encode(p)
	f.Add(b)

	// Seed with a payload containing metrics
	m, _ := NewMetric("test", pb.DataType_Int32, int32(42))
	p2 := NewPayload(0, m)
	b2, _ := Encode(p2)
	f.Add(b2)

	// Seed with boolean array
	boolM, _ := NewMetric("bools", pb.DataType_BooleanArray, []bool{true, false, true})
	p3 := NewPayload(0, boolM)
	b3, _ := Encode(p3)
	f.Add(b3)

	// Seed with string array
	strM, _ := NewMetric("strs", pb.DataType_StringArray, []string{"hello", "world"})
	p4 := NewPayload(0, strM)
	b4, _ := Encode(p4)
	f.Add(b4)

	// Seed with an Int8 in each of its two forms
	for _, wire := range []uint32{0xFFFFFFF8, 0xF8} {
		dt := uint32(pb.DataType_Int8)
		b, _ := Encode(NewPayload(0, &pb.Payload_Metric{Datatype: &dt, Value: &pb.Payload_Metric_IntValue{IntValue: wire}}))
		f.Add(b)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := Decode(data)
		if err != nil {
			return
		}
		for _, m := range p.Metrics {
			_, _ = GetValue(m)
		}
	})
}

// FuzzDecodeSignedInt checks the Int8 and Int16 rule of GetValue against a
// statement of it in terms of the two wire forms, and that every value it
// accepts encodes back to the sign-extended form.
func FuzzDecodeSignedInt(f *testing.F) {
	for _, wire := range []uint32{0, 0x7F, 0x80, 0xFF, 0x100, 0x7FFF, 0x8000, 0xFFFF, 0x10000,
		0xFFFF7FFF, 0xFFFF8000, 0xFFFFFF7F, 0xFFFFFF80, 0xFFFFFFFF, 0x80000000} {
		f.Add(wire)
	}

	check := func(t *testing.T, dt pb.DataType, bits int, wire uint32, val func(any) int32) {
		d := uint32(dt)
		got, err := GetValue(&pb.Payload_Metric{Datatype: &d, Value: &pb.Payload_Metric_IntValue{IntValue: wire}})

		width := int64(wire) < int64(1)<<bits
		extended := int32(wire) < 0 && int32(wire) >= -(int32(1)<<(bits-1))
		if !width && !extended {
			if !errors.Is(err, ErrTypeMismatch) {
				t.Fatalf("%v from %#x: got %v, %v; want ErrTypeMismatch", dt, wire, got, err)
			}
			return
		}
		if err != nil {
			t.Fatalf("%v from %#x: %v", dt, wire, err)
		}
		// Either form keeps the bits of the width.
		v := val(got)
		if mask := uint32(1)<<bits - 1; uint32(v)&mask != wire&mask {
			t.Fatalf("%v from %#x decoded to %d", dt, wire, v)
		}
		var m pb.Payload_Metric
		if err := SetValue(&m, dt, got); err != nil {
			t.Fatal(err)
		}
		if m.GetIntValue() != uint32(v) {
			t.Fatalf("%v %d encoded to %#x", dt, v, m.GetIntValue())
		}
	}

	f.Fuzz(func(t *testing.T, wire uint32) {
		check(t, pb.DataType_Int8, 8, wire, func(v any) int32 { return int32(v.(int8)) })
		check(t, pb.DataType_Int16, 16, wire, func(v any) int32 { return int32(v.(int16)) })
	})
}

func FuzzDecodeBooleanArray(f *testing.F) {
	f.Add(encodeBooleanArray([]bool{true, false, true, true}))
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0})
	f.Add([]byte{255, 255, 255, 255, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		result, err := decodeBooleanArray(data)
		if err != nil {
			return
		}
		roundTrip := encodeBooleanArray(result)
		result2, err := decodeBooleanArray(roundTrip)
		if err != nil {
			t.Fatalf("round-trip failed: %v", err)
		}
		if len(result) != len(result2) {
			t.Fatalf("round-trip length mismatch: %d vs %d", len(result), len(result2))
		}
		for i := range result {
			if result[i] != result2[i] {
				t.Fatalf("round-trip mismatch at index %d", i)
			}
		}
	})
}

func FuzzDecodeStringArray(f *testing.F) {
	f.Add(encodeStringArray([]string{"hello", "world"}))
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte{0x61, 0, 0x62, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeStringArray(data)
	})
}
