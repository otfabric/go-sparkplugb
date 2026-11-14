// SPDX-License-Identifier: MIT

package topic

import (
	"strings"
	"testing"
	"unicode/utf8"
)

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
		// An accepted topic is valid UTF-8, and its IDs hold no '/', '+'
		// or '#'.
		if !utf8.ValidString(input) {
			t.Fatalf("accepted %q, which is not valid UTF-8", input)
		}
		for _, id := range []string{parsed.GroupID, parsed.EdgeNodeID, parsed.DeviceID} {
			if strings.ContainsAny(id, "/+#") {
				t.Fatalf("accepted %q with the ID %q", input, id)
			}
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
