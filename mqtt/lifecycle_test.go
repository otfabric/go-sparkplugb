// SPDX-License-Identifier: MIT

package mqtt_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/otfabric/go-sparkplugb/mqtt"
)

// L2: Disconnect from two goroutines at once. Both return nil, the
// connection ends once and reports its end once, and the transport connects
// again afterwards.
func TestDisconnectFromTwoGoroutines(t *testing.T) {
	var live atomic.Int32
	url := countingBroker(t, &live)
	tr := mqtt.NewPahoTransport()
	ctx := context.Background()
	var lost atomic.Int32
	opts := mqtt.ConnectOpts{BrokerURL: url, ClientID: "twice", CleanSession: true,
		OnConnectionLost: func(error) { lost.Add(1) }}
	if err := tr.Connect(ctx, opts); err != nil {
		t.Fatal(err)
	}
	waitLive(t, "connection", &live, 1)

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			<-start
			errs <- tr.Disconnect(ctx)
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("Disconnect: %v", err)
		}
	}
	waitLive(t, "after both Disconnects", &live, 0)
	if n := lost.Load(); n != 1 {
		t.Errorf("OnConnectionLost called %d times, want 1", n)
	}
	if err := tr.Publish(ctx, "spBv1.0/g/NDATA/n", 0, false, []byte{1}); err == nil {
		t.Error("Publish after Disconnect: nil, want an error")
	}

	if err := tr.Connect(ctx, opts); err != nil {
		t.Fatalf("Connect after Disconnect: %v", err)
	}
	waitLive(t, "connection again", &live, 1)
	if err := tr.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	waitLive(t, "after the last Disconnect", &live, 0)
}
