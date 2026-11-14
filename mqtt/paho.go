// SPDX-License-Identifier: MIT

package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// PahoTransport is a Transport over the Eclipse Paho MQTT client, one
// connection at a time. Its zero value is ready to use, with the timeouts of
// NewPahoTransport. Set its fields before the first Connect.
type PahoTransport struct {
	mu         sync.Mutex
	client     paho.Client
	opts       *paho.ClientOptions
	onLost     func(error)
	closed     chan struct{} // closed when the current connection ends
	connecting chan struct{} // holds a token while a Connect runs: one at a time

	// TLSConfig, if set, is the TLS configuration of a connection to an
	// ssl://, tls://, mqtts:// or wss:// broker. It is used as it is.
	TLSConfig *tls.Config
	// ConnectTimeout bounds a Connect: the dial, the handshake and Paho's
	// fallback to MQTT 3.1 together. Zero or less means 10 seconds.
	ConnectTimeout time.Duration
	// PublishTimeout bounds a Publish and a Subscribe, and the write of a
	// packet to the connection. Zero or less means 5 seconds.
	PublishTimeout time.Duration
}

const (
	defaultConnectTimeout = 10 * time.Second
	defaultPublishTimeout = 5 * time.Second
)

func (t *PahoTransport) connectTimeout() time.Duration {
	if t.ConnectTimeout > 0 {
		return t.ConnectTimeout
	}
	return defaultConnectTimeout
}

func (t *PahoTransport) publishTimeout() time.Duration {
	if t.PublishTimeout > 0 {
		return t.PublishTimeout
	}
	return defaultPublishTimeout
}

// NewPahoTransport returns a transport with a ConnectTimeout of 10 seconds
// and a PublishTimeout of 5 seconds.
func NewPahoTransport() *PahoTransport {
	return &PahoTransport{
		ConnectTimeout: defaultConnectTimeout,
		PublishTimeout: defaultPublishTimeout,
	}
}

// Connect opens a connection to opts.BrokerURL, in place of the current one,
// and returns once the broker accepted it, ctx ended or ConnectTimeout
// passed. An attempt that ends with ctx or the timeout is aborted: it does
// not connect later. A Disconnect meanwhile aborts it too, and Connect
// returns an error wrapping ErrConnectionLost. Connect does not hold the
// transport's lock while it waits, so Disconnect, Publish and Subscribe
// return at once; a second Connect waits for the first, or for its own ctx.
func (t *PahoTransport) Connect(ctx context.Context, opts ConnectOpts) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sparkplugb: connect: %w", err)
	}
	t.mu.Lock()
	if t.connecting == nil {
		t.connecting = make(chan struct{}, 1)
	}
	connecting := t.connecting
	t.mu.Unlock()
	select {
	case connecting <- struct{}{}:
		defer func() { <-connecting }()
	case <-ctx.Done():
		return fmt.Errorf("sparkplugb: connect: %w", ctx.Err())
	}

	connectTimeout := t.connectTimeout()

	pahoOpts := paho.NewClientOptions()
	pahoOpts.AddBroker(opts.BrokerURL)
	pahoOpts.SetClientID(opts.ClientID)
	pahoOpts.SetCleanSession(opts.CleanSession)
	pahoOpts.SetAutoReconnect(false)

	if opts.Username != "" {
		pahoOpts.SetUsername(opts.Username)
		pahoOpts.SetPassword(opts.Password)
	}

	if t.TLSConfig != nil {
		pahoOpts.SetTLSConfig(t.TLSConfig)
	}

	pahoOpts.SetConnectTimeout(connectTimeout)

	// Without this paho waits 30s to hand a packet to a connection that has
	// just dropped, before any token exists to wait on.
	pahoOpts.SetWriteTimeout(t.publishTimeout())

	if opts.WillTopic != "" {
		pahoOpts.SetBinaryWill(opts.WillTopic, opts.WillPayload, opts.WillQoS, opts.WillRetained)
	}

	// Paho calls the handler on a goroutine of its own. It waits for the
	// outcome of this Connect: the end of a connection that Connect returned
	// is reported after it, through OnConnectionLost.
	decided := make(chan struct{})
	defer close(decided)
	pahoOpts.SetConnectionLostHandler(func(c paho.Client, err error) {
		<-decided
		if lost := t.release(c); lost != nil {
			lost(fmt.Errorf("%w: %w", ErrConnectionLost, err))
		}
	})

	client := paho.NewClient(pahoOpts)
	t.mu.Lock()
	t.opts = pahoOpts
	t.client = client
	t.onLost = nil
	t.closed = make(chan struct{})
	closed := t.closed
	t.mu.Unlock()

	token := client.Connect()
	timer := time.NewTimer(connectTimeout)
	defer timer.Stop()
	var err error
	select {
	case <-token.Done():
		if err := token.Error(); err != nil {
			return fmt.Errorf("sparkplugb: connect: %w", err)
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		if t.client != client { // Disconnect was called meanwhile
			return fmt.Errorf("sparkplugb: connect: %w", ErrConnectionLost)
		}
		t.onLost = opts.OnConnectionLost
		return nil
	case <-closed: // Disconnect was called, and ended the attempt
		return fmt.Errorf("sparkplugb: connect: %w", ErrConnectionLost)
	case <-ctx.Done():
		err = fmt.Errorf("sparkplugb: connect: %w", ctx.Err())
	case <-timer.C:
		err = fmt.Errorf("sparkplugb: connect timeout")
	}
	// Paho goes on with the attempt, and with its MQTT 3.1 fallback after
	// it: end the attempt, so that it does not connect later.
	t.release(client)
	end(client)
	return err
}

// end ends the connection of client: with a DISCONNECT, which makes the
// broker discard the will, when it is connected. Otherwise it ends the
// attempt in progress: Paho finishes the handshake on its own goroutine and
// then closes the connection.
func end(client paho.Client) {
	if client.IsConnected() {
		client.Disconnect(250)
	} else {
		client.Disconnect(0)
	}
}

// release detaches c if it is still the current client and returns the
// connection-lost callback to fire, so each connection reports its end once.
func (t *PahoTransport) release(c paho.Client) func(error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c == nil || t.client != c {
		return nil
	}
	lost := t.onLost
	t.client = nil
	t.onLost = nil
	close(t.closed)
	return lost
}

func (t *PahoTransport) Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte) error {
	t.mu.Lock()
	client, closed := t.client, t.closed
	t.mu.Unlock()

	if client == nil || !client.IsConnected() {
		return fmt.Errorf("sparkplugb: not connected")
	}

	// A publish whose context has ended does not start: started, paho could
	// still send it after Publish returns.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sparkplugb: publish: %w", err)
	}
	return t.await(ctx, closed, "publish", true, func() paho.Token {
		return client.Publish(topic, qos, retained, payload)
	})
}

func (t *PahoTransport) Subscribe(ctx context.Context, topic string, qos byte, handler MessageHandler) error {
	t.mu.Lock()
	client, closed := t.client, t.closed
	t.mu.Unlock()

	if client == nil || !client.IsConnected() {
		return fmt.Errorf("sparkplugb: not connected")
	}

	callback := func(_ paho.Client, msg paho.Message) {
		handler(msg.Topic(), msg.Payload())
	}

	return t.await(ctx, closed, "subscribe", false, func() paho.Token {
		return client.Subscribe(topic, qos, callback)
	})
}

// await runs a paho operation and returns when it completes, ctx ends, the
// connection ends or PublishTimeout passes. op runs in its own goroutine
// because paho can block in the call itself, before it returns a token.
//
// With ordered set, await does not return before op has: paho hands a
// publish to the one writer of the connection when Client.Publish returns,
// and keeps that order. Returning earlier, on ctx or the timeout, would let
// a later publish overtake this one, which still goes out. So when Publish
// returns, its message is either ahead of every later publish or not sent:
// op failed, or the connection ended. The wait is bounded by the write
// timeout paho has from PublishTimeout.
func (t *PahoTransport) await(ctx context.Context, closed <-chan struct{}, what string, ordered bool, op func() paho.Token) error {
	done := make(chan error, 1)
	handed := make(chan struct{})
	timeout := fmt.Errorf("sparkplugb: %s timeout", what)
	go func() {
		token := op()
		close(handed)
		if !token.WaitTimeout(t.publishTimeout()) {
			done <- timeout
			return
		}
		done <- token.Error()
	}()

	timer := time.NewTimer(t.publishTimeout())
	defer timer.Stop()
	var err error
	select {
	case err = <-done:
		return err
	case <-closed:
		return fmt.Errorf("sparkplugb: %s: %w", what, ErrConnectionLost)
	case <-ctx.Done():
		err = fmt.Errorf("sparkplugb: %s: %w", what, ctx.Err())
	case <-timer.C:
		err = timeout
	}
	if ordered {
		select {
		case <-handed:
		case <-closed: // the message cannot go out on a later connection
		}
	}
	return err
}

// Disconnect ends the current connection, and aborts a Connect in progress.
func (t *PahoTransport) Disconnect(ctx context.Context) error {
	t.mu.Lock()
	client := t.client
	t.mu.Unlock()

	lost := t.release(client)
	if client != nil {
		end(client)
	}
	if lost != nil {
		lost(ErrConnectionLost)
	}
	return nil
}
