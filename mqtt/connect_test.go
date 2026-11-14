// SPDX-License-Identifier: MIT

package mqtt_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/mqtt"
)

// lateBroker reads the CONNECT of each client, and answers that of its nth
// client with a CONNACK, only once answer is closed; the other clients get
// no answer. It reports when that CONNECT arrived, and how the client ended
// that connection once the CONNACK was due: by a DISCONNECT or by closing
// it.
type lateBroker struct {
	url        string
	gotConnect chan struct{}
	answer     chan struct{}
	ended      chan string
}

func startLateBroker(t *testing.T, nth int) *lateBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &lateBroker{
		url:        "tcp://" + ln.Addr().String(),
		gotConnect: make(chan struct{}),
		answer:     make(chan struct{}),
		ended:      make(chan string, 1),
	}
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop); _ = ln.Close() })
	go func() {
		for n := 1; ; n++ {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { <-stop; _ = c.Close() }()
			go b.serve(c, n == nth, stop)
		}
	}()
	return b
}

func (b *lateBroker) serve(c net.Conn, answers bool, stop <-chan struct{}) {
	defer c.Close()
	r := bufio.NewReader(c)
	kind, err := readPacket(r)
	if err != nil || kind != 1 {
		return
	}
	if !answers {
		_, _ = io.Copy(io.Discard, r) // until the client or the test closes it
		return
	}
	close(b.gotConnect)
	select {
	case <-b.answer:
	case <-stop:
		return
	}
	how := "closed"
	if _, err := c.Write([]byte{0x20, 0x02, 0x00, 0x00}); err == nil { // CONNACK, accepted
		for {
			kind, err := readPacket(r)
			if err != nil {
				break
			}
			if kind == 14 {
				how = "DISCONNECT"
				break
			}
		}
	}
	select {
	case <-stop: // the test ended, and closed the connection itself
	default:
		b.ended <- how
	}
}

// readPacket reads one MQTT packet and returns its type.
func readPacket(r *bufio.Reader) (byte, error) {
	first, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	size, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, err
	}
	_, err = io.CopyN(io.Discard, r, int64(size))
	return first >> 4, err
}

func waitFor[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: not within 10s", what)
		panic("unreachable")
	}
}

// Connect returns when its context ends, or ConnectTimeout passes, while the
// broker has not answered, and the attempt does not connect afterwards when
// the broker answers late: the first CONNECT, or the one of Paho's second
// attempt, with MQTT 3.1, which starts when the first has timed out.
func TestConnectThatEndsDoesNotConnectLater(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ctx     time.Duration
		timeout time.Duration
		answers int // the CONNECT the broker answers
		want    error
	}{
		{name: "context", ctx: 100 * time.Millisecond, timeout: time.Minute, answers: 1, want: context.DeadlineExceeded},
		{name: "ConnectTimeout", ctx: time.Minute, timeout: 100 * time.Millisecond, answers: 1},
		{name: "ConnectTimeout, second attempt answered", ctx: time.Minute, timeout: 100 * time.Millisecond, answers: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := startLateBroker(t, tc.answers)
			tr := mqtt.NewPahoTransport()
			tr.ConnectTimeout = tc.timeout
			ctx, cancel := context.WithTimeout(context.Background(), tc.ctx)
			defer cancel()
			lost := make(chan error, 1)
			done := make(chan error, 1)
			go func() {
				done <- tr.Connect(ctx, mqtt.ConnectOpts{BrokerURL: b.url, ClientID: "late", CleanSession: true,
					OnConnectionLost: func(err error) { lost <- err }})
			}()
			err := waitFor(t, "Connect to a broker that does not answer", done)
			switch {
			case err == nil:
				t.Fatal("Connect to a broker that does not answer succeeded")
			case tc.want != nil && !errors.Is(err, tc.want):
				t.Errorf("Connect: %v, want %v", err, tc.want)
			}
			waitFor(t, "the CONNECT at the broker", b.gotConnect)

			close(b.answer) // the CONNACK, late
			if how := waitFor(t, "the end of the connection after the late CONNACK", b.ended); how == "" {
				t.Error("connection not ended")
			}
			if err := tr.Publish(context.Background(), "spBv1.0/g/NDATA/n", 0, false, nil); err == nil {
				t.Error("Publish after the attempt ended succeeded")
			}
			select {
			case err := <-lost:
				t.Errorf("OnConnectionLost called for a Connect that failed: %v", err)
			default:
			}
		})
	}
}

// Disconnect does not wait for a Connect in progress: it ends the attempt,
// and Connect returns an error.
func TestDisconnectDuringConnect(t *testing.T) {
	b := startLateBroker(t, 1)
	tr := mqtt.NewPahoTransport()
	tr.ConnectTimeout = time.Minute
	done := make(chan error, 1)
	go func() {
		done <- tr.Connect(context.Background(), mqtt.ConnectOpts{BrokerURL: b.url, ClientID: "late", CleanSession: true})
	}()
	waitFor(t, "the CONNECT at the broker", b.gotConnect)

	disconnected := make(chan error, 1)
	go func() { disconnected <- tr.Disconnect(context.Background()) }()
	if err := waitFor(t, "Disconnect during Connect", disconnected); err != nil {
		t.Errorf("Disconnect: %v", err)
	}
	if err := waitFor(t, "Connect after Disconnect", done); !errors.Is(err, mqtt.ErrConnectionLost) {
		t.Errorf("Connect ended by Disconnect: %v, want ErrConnectionLost", err)
	}
	close(b.answer)
	waitFor(t, "the end of the connection after the late CONNACK", b.ended)
}

// A Connect whose context has ended does not dial.
func TestConnectWithEndedContext(t *testing.T) {
	b := startLateBroker(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mqtt.NewPahoTransport().Connect(ctx, mqtt.ConnectOpts{BrokerURL: b.url, ClientID: "late"}); !errors.Is(err, context.Canceled) {
		t.Errorf("Connect: %v, want context.Canceled", err)
	}
	select {
	case <-b.gotConnect:
		t.Error("the broker got a CONNECT")
	default:
	}
}

// The zero value of PahoTransport has the timeouts of NewPahoTransport.
func TestZeroValuePahoTransport(t *testing.T) {
	b := startBroker(t, true)
	var tr mqtt.PahoTransport
	ctx := context.Background()
	if err := tr.Connect(ctx, mqtt.ConnectOpts{BrokerURL: b.url(), ClientID: "zero", CleanSession: true}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := tr.Subscribe(ctx, "spBv1.0/#", 1, func(string, []byte) {}); err != nil {
		t.Errorf("Subscribe: %v", err)
	}
	if err := tr.Publish(ctx, "spBv1.0/g/NDATA/n", 1, false, []byte{1}); err != nil {
		t.Errorf("Publish: %v", err)
	}
	if err := tr.Disconnect(ctx); err != nil {
		t.Errorf("Disconnect: %v", err)
	}
	if err := tr.Disconnect(ctx); err != nil {
		t.Errorf("Disconnect again: %v", err)
	}
}
