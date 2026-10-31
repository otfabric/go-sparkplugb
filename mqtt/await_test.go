// SPDX-License-Identifier: MIT

package mqtt_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/mqtt"
)

// silentBroker accepts one client and CONNACKs it. It then ignores everything
// until the test ends, or closes the connection when drop is set.
func silentBroker(t *testing.T, drop bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop); _ = ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = c.Read(make([]byte, 1024))              // CONNECT
		_, _ = c.Write([]byte{0x20, 0x02, 0x00, 0x00}) // CONNACK, accepted
		if !drop {
			<-stop
		}
	}()
	return "tcp://" + ln.Addr().String()
}

func connectTo(t *testing.T, url string) *mqtt.PahoTransport {
	t.Helper()
	tr := mqtt.NewPahoTransport()
	tr.PublishTimeout = 30 * time.Second // the outcome must not come from this
	if err := tr.Connect(context.Background(), mqtt.ConnectOpts{BrokerURL: url, ClientID: "await", CleanSession: true}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Disconnect(context.Background()) })
	return tr
}

// A connection that drops around the operation fails it at once, never after
// a timeout.
func TestOperationsEndWithTheConnection(t *testing.T) {
	for i := 0; i < 50; i++ {
		tr := connectTo(t, silentBroker(t, true))
		start := time.Now()
		err := tr.Subscribe(context.Background(), "spBv1.0/#", 0, func(string, []byte) {})
		if err == nil {
			t.Fatal("subscribe on a dropped connection succeeded")
		}
		if d := time.Since(start); d > 5*time.Second {
			t.Fatalf("subscribe took %v: %v", d, err)
		}
	}
}

func TestOperationsHonourContext(t *testing.T) {
	tr := connectTo(t, silentBroker(t, false))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := tr.Subscribe(ctx, "spBv1.0/#", 0, func(string, []byte) {})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("subscribe: %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = tr.Publish(ctx, "spBv1.0/g/NDATA/n", 1, false, []byte{1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("publish: %v", err)
	}
}

func TestOperationsTimeOut(t *testing.T) {
	tr := connectTo(t, silentBroker(t, false))
	tr.PublishTimeout = 50 * time.Millisecond
	if err := tr.Subscribe(context.Background(), "spBv1.0/#", 0, func(string, []byte) {}); err == nil {
		t.Error("subscribe to a silent broker succeeded")
	}
}
