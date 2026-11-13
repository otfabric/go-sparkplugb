// SPDX-License-Identifier: MIT

package host

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/otfabric/go-sparkplugb/mqtt"
)

func (m *mockTransport) isConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connected
}

// setupFailTransport fails one Subscribe or one Publish by topic.
type setupFailTransport struct {
	*mockTransport
	subscribe, publish string
}

func (f *setupFailTransport) Subscribe(ctx context.Context, t string, qos byte, h mqtt.MessageHandler) error {
	if t == f.subscribe {
		return (&failSubscribeTransport{mockTransport: f.mockTransport, failTopic: t}).Subscribe(ctx, t, qos, h)
	}
	return f.mockTransport.Subscribe(ctx, t, qos, h)
}

func (f *setupFailTransport) Publish(ctx context.Context, t string, qos byte, retained bool, p []byte) error {
	if t == f.publish {
		f.publish = "" // the STATE death that follows goes out
		return (&failPublishTransport{mockTransport: f.mockTransport, failTopic: t}).Publish(ctx, t, qos, retained, p)
	}
	return f.mockTransport.Publish(ctx, t, qos, retained, p)
}

// A step after the CONNACK that failed ended Connect with the connection
// still up: its will armed, the host in Connecting, and the next Connect
// opened a second connection beside it. The connection is closed now. A
// DISCONNECT discards the will, so when the STATE birth may have reached the
// broker the STATE death follows it; otherwise nothing is published.
func TestHostConnectSetupFailureDisconnects(t *testing.T) {
	const stateTopic = "spBv1.0/STATE/testHost"
	for _, c := range []struct {
		name               string
		subscribe, publish string
		deaths             int
	}{
		{name: "subscribe STATE", subscribe: stateTopic},
		{name: "subscribe namespace", subscribe: "spBv1.0/#"},
		{name: "publish STATE birth", publish: stateTopic, deaths: 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			tr := &setupFailTransport{mockTransport: newMockTransport(), subscribe: c.subscribe, publish: c.publish}
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
			var published, deaths int
			for _, p := range tr.getPublished() {
				published++
				var sp statePayload
				if p.Topic == stateTopic && json.Unmarshal(p.Payload, &sp) == nil && !sp.Online && p.Retained {
					deaths++
				}
			}
			if published != c.deaths || deaths != c.deaths {
				t.Errorf("published %d messages, %d of them a retained STATE death; want %d", published, deaths, c.deaths)
			}

			tr.subscribe, tr.publish = "", ""
			if err := a.Connect(context.Background()); err != nil {
				t.Fatalf("Connect again: %v", err)
			}
			if s := a.State(); s != Connected {
				t.Errorf("state after the retry = %s, want %s", s, Connected)
			}
		})
	}
}
