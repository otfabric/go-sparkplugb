// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/otfabric/go-sparkplugb/mqtt"
)

func (m *mockTransport) isConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connected
}

type failSubscribeTransport struct {
	*mockTransport
	failTopic string
}

func (f *failSubscribeTransport) Subscribe(ctx context.Context, t string, qos byte, h mqtt.MessageHandler) error {
	if t == f.failTopic {
		return errors.New("subscribe failed")
	}
	return f.mockTransport.Subscribe(ctx, t, qos, h)
}

// A subscription that failed after the CONNACK ended Connect with the
// connection still up, the application in Connecting, and the next Connect
// opened a second connection beside it. The connection is closed now and
// Connect can be called again.
func TestApplicationSubscribeFailureDisconnects(t *testing.T) {
	tr := &failSubscribeTransport{mockTransport: newMockTransport(), failTopic: "spBv1.0/#"}
	a, err := New(tr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded")
	}
	if s := a.State(); s != Disconnected {
		t.Errorf("state = %s, want %s", s, Disconnected)
	}
	if tr.isConnected() {
		t.Error("the connection is still up")
	}

	tr.failTopic = ""
	if err := a.Connect(context.Background()); err != nil {
		t.Fatalf("Connect again: %v", err)
	}
	if s := a.State(); s != Connected {
		t.Errorf("state after the retry = %s, want %s", s, Connected)
	}
}

// A broker disconnects a client when another connects with its ID, so the
// IDs of applications that name none must not collide: the clock modulo
// 100000 gave a pair 1 chance in 100000. Twenty IDs of 32 random bits
// collide about once in 20 million runs.
func TestApplicationDefaultClientID(t *testing.T) {
	format := regexp.MustCompile(`^sparkplug_app_[0-9a-f]{8}$`)
	seen := map[string]bool{}
	for range 20 {
		a, err := New(newMockTransport(), testConfig())
		if err != nil {
			t.Fatal(err)
		}
		id := a.cfg.ClientID
		if !format.MatchString(id) {
			t.Fatalf("client ID %q, want sparkplug_app_ and 8 hex digits", id)
		}
		if seen[id] {
			t.Fatalf("client ID %q drawn twice", id)
		}
		seen[id] = true
	}
	cfg := testConfig()
	cfg.ClientID = "mine"
	if a, _ := New(newMockTransport(), cfg); a.cfg.ClientID != "mine" {
		t.Errorf("client ID = %q, want the configured one", a.cfg.ClientID)
	}
}
