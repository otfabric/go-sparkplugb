// SPDX-License-Identifier: MIT

package payload

import (
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
