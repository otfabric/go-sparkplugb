// SPDX-License-Identifier: MIT

package edge

import (
	"context"
	"testing"
)

func (m *mockTransport) isConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connected
}

// A subscription that fails after the CONNACK ended Connect with the
// connection still up: its will armed, the node in Subscribing, and the next
// Connect opened a second connection beside it. The connection is closed
// now, nothing is published on it, and Connect can be called again.
func TestConnectSetupFailureDisconnects(t *testing.T) {
	for _, c := range []struct {
		name, fail, primary string
	}{
		{"NCMD", "spBv1.0/testGroup/NCMD/testNode", ""},
		{"DCMD", "spBv1.0/testGroup/DCMD/testNode/+", ""},
		{"STATE", "spBv1.0/STATE/myHost", "myHost"},
	} {
		t.Run(c.name, func(t *testing.T) {
			tr := &failSubscribeTransport{mockTransport: newMockTransport(), failTopic: c.fail}
			cfg := testConfig()
			cfg.PrimaryHostID = c.primary
			n, err := New(tr, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := n.Connect(context.Background()); err == nil {
				t.Fatal("Connect succeeded")
			}
			if s := n.State(); s != Disconnected {
				t.Errorf("state = %s, want %s", s, Disconnected)
			}
			if tr.isConnected() {
				t.Error("the connection is still up")
			}
			if p := tr.getPublished(); len(p) != 0 {
				t.Errorf("published %d messages on it, want none", len(p))
			}

			tr.failTopic = ""
			if err := n.Connect(context.Background()); err != nil {
				t.Fatalf("Connect again: %v", err)
			}
			if !tr.isConnected() {
				t.Error("not connected after the retry")
			}
		})
	}
}
