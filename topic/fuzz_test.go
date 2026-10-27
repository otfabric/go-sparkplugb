// SPDX-License-Identifier: MIT

package topic

import "testing"

func FuzzParse(f *testing.F) {
	f.Add("spBv1.0/group1/NBIRTH/node1")
	f.Add("spBv1.0/group1/DBIRTH/node1/device1")
	f.Add("spBv1.0/STATE/host1")
	f.Add("spBv1.0/group1/NDATA/node1")
	f.Add("")
	f.Add("spBv1.0")
	f.Add("spBv1.0/a/b/c/d/e")
	f.Add("invalid/topic")

	f.Fuzz(func(t *testing.T, input string) {
		parsed, err := Parse(input)
		if err != nil {
			return
		}
		roundTrip := parsed.String()
		reparsed, err := Parse(roundTrip)
		if err != nil {
			t.Fatalf("round-trip failed for %q -> %q: %v", input, roundTrip, err)
		}
		if reparsed.String() != roundTrip {
			t.Fatalf("round-trip mismatch: %q vs %q", reparsed.String(), roundTrip)
		}
	})
}
