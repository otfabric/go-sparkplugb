// SPDX-License-Identifier: MIT

package mqtt_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/mqtt"
)

// countingBroker is the fake broker counting the connections it has open.
func countingBroker(t *testing.T, live *atomic.Int32) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	b := &fakeBroker{ln: ln, open: true}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			live.Add(1)
			go func() { b.serve(c); live.Add(-1) }()
		}
	}()
	return "tcp://" + ln.Addr().String()
}

func waitLive(t *testing.T, what string, live *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for live.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d connections, want %d", what, live.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// Connect on a live connection ends it before opening the next one, without
// reporting it lost: the transport has one connection at a time, and
// Disconnect leaves none up.
func TestConnectReplacesTheConnection(t *testing.T) {
	var liveA, liveB atomic.Int32
	a, b := countingBroker(t, &liveA), countingBroker(t, &liveB)
	tr := mqtt.NewPahoTransport()
	ctx := context.Background()
	var lost atomic.Int32
	opts := func(url string) mqtt.ConnectOpts {
		return mqtt.ConnectOpts{BrokerURL: url, ClientID: "replace", CleanSession: true,
			OnConnectionLost: func(error) { lost.Add(1) }}
	}
	if err := tr.Connect(ctx, opts(a)); err != nil {
		t.Fatal(err)
	}
	waitLive(t, "first connection", &liveA, 1)
	if err := tr.Connect(ctx, opts(b)); err != nil {
		t.Fatal(err)
	}
	waitLive(t, "the replaced connection", &liveA, 0)
	waitLive(t, "second connection", &liveB, 1)
	if n := lost.Load(); n != 0 {
		t.Errorf("OnConnectionLost called %d times for the replaced connection, want 0", n)
	}
	if err := tr.Publish(ctx, "spBv1.0/g/NDATA/n", 0, false, []byte{1}); err != nil {
		t.Errorf("Publish on the new connection: %v", err)
	}
	if err := tr.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	waitLive(t, "after Disconnect", &liveB, 0)
	if n := lost.Load(); n != 1 {
		t.Errorf("OnConnectionLost called %d times after Disconnect, want 1", n)
	}
}
