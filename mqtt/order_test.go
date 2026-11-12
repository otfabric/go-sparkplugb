// SPDX-License-Identifier: MIT

package mqtt_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/mqtt"
)

// orderBroker accepts one client, CONNACKs it, and records the topics of its
// PUBLISH packets in the order they arrive. It reads the start of the first
// one, closes started, and reads no further until gate is closed: the
// client's writes back up meanwhile.
type orderBroker struct {
	url     string
	gate    chan struct{}
	started chan struct{}

	mu     sync.Mutex
	topics []string
}

func newOrderBroker(t *testing.T) *orderBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &orderBroker{url: "tcp://" + ln.Addr().String(), gate: make(chan struct{}), started: make(chan struct{})}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		r := bufio.NewReaderSize(c, 64)                                              // reads no further ahead than a header
		if _, rest, _, err := readHeader(r); err != nil || discard(r, rest) != nil { // CONNECT
			return
		}
		_, _ = c.Write([]byte{0x20, 0x02, 0x00, 0x00}) // CONNACK, accepted
		for first := true; ; {
			kind, rest, topic, err := readHeader(r)
			if err != nil {
				return
			}
			if kind == 3 {
				b.mu.Lock()
				b.topics = append(b.topics, topic)
				b.mu.Unlock()
				if first {
					first = false
					close(b.started)
					<-b.gate
				}
			}
			if discard(r, rest) != nil {
				return
			}
		}
	}()
	return b
}

func (b *orderBroker) seen() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.topics)
}

// readHeader reads the fixed header of an MQTT packet, and the topic of a
// PUBLISH; rest is what is left of the packet.
func readHeader(r *bufio.Reader) (kind byte, rest int, topic string, err error) {
	h, err := r.ReadByte()
	if err != nil {
		return 0, 0, "", err
	}
	for shift := 0; ; shift += 7 {
		d, err := r.ReadByte()
		if err != nil {
			return 0, 0, "", err
		}
		rest |= int(d&0x7f) << shift
		if d&0x80 == 0 {
			break
		}
	}
	kind = h >> 4
	if kind == 3 {
		l := make([]byte, 2)
		if _, err := io.ReadFull(r, l); err != nil {
			return 0, 0, "", err
		}
		name := make([]byte, int(l[0])<<8|int(l[1]))
		if _, err := io.ReadFull(r, name); err != nil {
			return 0, 0, "", err
		}
		topic, rest = string(name), rest-2-len(name)
	}
	return kind, rest, topic, nil
}

func discard(r *bufio.Reader, n int) error {
	_, err := io.CopyN(io.Discard, r, int64(n))
	return err
}

func waitSeen(t *testing.T, b *orderBroker, n int) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(b.seen()) < n {
		if time.Now().After(deadline) {
			t.Fatalf("broker saw %v, want %d publishes", b.seen(), n)
		}
		time.Sleep(time.Millisecond)
	}
	return b.seen()
}

// A publish whose context has already ended is not sent: started, it could
// go out after a later publish.
func TestPublishWithEndedContextIsNotSent(t *testing.T) {
	b := newOrderBroker(t)
	close(b.gate)
	tr := connectTo(t, b.url)
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	var want []string
	for i := range 20 {
		late, next := "late/"+string(rune('a'+i)), "next/"+string(rune('a'+i))
		if err := tr.Publish(ended, late, 0, false, []byte{1}); !errors.Is(err, context.Canceled) {
			t.Fatalf("publish with an ended context: %v", err)
		}
		if err := tr.Publish(context.Background(), next, 0, false, []byte{1}); err != nil {
			t.Fatal(err)
		}
		want = append(want, next)
	}
	if got := waitSeen(t, b, len(want)); !slices.Equal(got, want) {
		t.Errorf("broker saw %v, want %v", got, want)
	}
}

// A publish whose context ends while paho still holds it back returns only
// once paho has it in line: a later publish cannot overtake it.
func TestPublishOnEndedContextKeepsItsPlace(t *testing.T) {
	b := newOrderBroker(t)
	tr := mqtt.NewPahoTransport()
	if err := tr.Connect(context.Background(), mqtt.ConnectOpts{BrokerURL: b.url, ClientID: "order", CleanSession: true}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Disconnect(context.Background()) })

	// More than the socket buffers hold: once the broker has its start, the
	// writer of the connection stays in it until the gate opens, and paho's
	// queue to the writer takes one more publish before Publish blocks.
	go func() { _ = tr.Publish(context.Background(), "big", 0, false, make([]byte, 32<<20)) }()
	select {
	case <-b.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the broker never saw the first publish")
	}

	short := func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		t.Cleanup(cancel)
		return ctx
	}
	if err := tr.Publish(short(), "queued", 0, false, []byte{1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued: %v", err)
	}

	// paho cannot take held until the gate opens, so a correct Publish cannot
	// return before then, whatever the timing. After its context ends, a
	// Publish that wrongly returns on it gets 100 ms to do so: that bounds
	// what the test detects, not whether it passes.
	ctx := short()
	held := make(chan error, 1)
	go func() { held <- tr.Publish(ctx, "held", 0, false, []byte{1}) }()
	<-ctx.Done()
	select {
	case err := <-held:
		t.Fatalf("held returned (%v) while paho still held it back: a later publish could overtake it", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(b.gate)
	select {
	case err := <-held:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("held: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("held did not return once paho had it")
	}
	if err := tr.Publish(context.Background(), "after", 0, false, []byte{1}); err != nil {
		t.Fatal(err)
	}
	want := []string{"big", "queued", "held", "after"}
	if got := waitSeen(t, b, len(want)); !slices.Equal(got, want) {
		t.Errorf("broker saw %v, want %v", got, want)
	}
}
