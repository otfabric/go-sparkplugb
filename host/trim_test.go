// SPDX-License-Identifier: MIT

package host

import (
	"testing"
	"time"
)

// trimHeld drops the held messages that arrived first, of any edge node; on
// equal arrival times by key, then by position, and keeps the others in
// order.
func TestHostTrimHeldOrder(t *testing.T) {
	heldTest(t, func(t *testing.T, a *App, _ *mockTransport) {
		const mb = 1 << 20
		t0 := time.Now()
		msg := func(seq uint64, at time.Time, size int) heldMsg {
			return heldMsg{seq: seq, at: at, data: make([]byte, size)}
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		dropped := a.heldDropped
		a.setHeld("k1", []heldMsg{msg(1, t0, 3*mb), msg(2, t0.Add(2*time.Second), mb)})
		a.setHeld("k2", []heldMsg{msg(3, t0, 3*mb), msg(4, t0, mb)})
		if a.heldBytes != maxHeldBytes {
			t.Fatalf("held %d bytes, want %d", a.heldBytes, maxHeldBytes)
		}

		a.trimHeld(1, 4*mb)

		if got := a.heldDropped - dropped; got != 2 {
			t.Errorf("dropped %d held messages, want 2", got)
		}
		if a.heldCount != 2 || a.heldBytes != 2*mb {
			t.Errorf("held %d messages, %d bytes, want 2, %d", a.heldCount, a.heldBytes, 2*mb)
		}
		for key, seq := range map[string]uint64{"k1": 2, "k2": 4} {
			if msgs := a.held[key]; len(msgs) != 1 || msgs[0].seq != seq {
				t.Errorf("%s: held %d messages, want only seq %d", key, len(msgs), seq)
			}
		}
		a.setHeld("k1", nil)
		a.setHeld("k2", nil)
	})
}
