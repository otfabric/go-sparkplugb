// SPDX-License-Identifier: MIT

package topic

import (
	"errors"
	"testing"
)

// The IDs of a topic are valid UTF-8, as MQTT and Sparkplug require.
func TestParseRefusesInvalidUTF8(t *testing.T) {
	for _, tc := range []struct {
		topic string
		ok    bool
	}{
		{"spBv1.0/grüße/NDATA/n", true},
		{"spBv1.0/g/NDATA/nœud", true},
		{"spBv1.0/g/DDATA/n/d€", true},
		{"spBv1.0/STATE/hôte", true},
		{"spBv1.0/\xff/NDATA/n", false},
		{"spBv1.0/g/NDATA/\xc3", false},
		{"spBv1.0/g/DDATA/n/\xed\xa0\x80", false},
		{"spBv1.0/STATE/h\xfe", false},
	} {
		_, err := Parse(tc.topic)
		if tc.ok && err != nil {
			t.Errorf("Parse(%q): %v", tc.topic, err)
		}
		if !tc.ok && !errors.Is(err, ErrInvalidID) {
			t.Errorf("Parse(%q): %v, want ErrInvalidID", tc.topic, err)
		}
	}
}
