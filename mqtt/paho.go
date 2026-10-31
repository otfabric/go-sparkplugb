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

type PahoTransport struct {
	mu     sync.Mutex
	client paho.Client
	opts   *paho.ClientOptions
	onLost func(error)
	closed chan struct{} // closed when the current connection ends

	TLSConfig      *tls.Config
	ConnectTimeout time.Duration
	PublishTimeout time.Duration
}

func NewPahoTransport() *PahoTransport {
	return &PahoTransport{
		ConnectTimeout: 10 * time.Second,
		PublishTimeout: 5 * time.Second,
	}
}

func (t *PahoTransport) Connect(ctx context.Context, opts ConnectOpts) error {
	t.mu.Lock()
	defer t.mu.Unlock()

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

	if t.ConnectTimeout > 0 {
		pahoOpts.SetConnectTimeout(t.ConnectTimeout)
	}

	// Without this paho waits 30s to hand a packet to a connection that has
	// just dropped, before any token exists to wait on.
	if t.PublishTimeout > 0 {
		pahoOpts.SetWriteTimeout(t.PublishTimeout)
	}

	if opts.WillTopic != "" {
		pahoOpts.SetBinaryWill(opts.WillTopic, opts.WillPayload, opts.WillQoS, opts.WillRetained)
	}

	pahoOpts.SetConnectionLostHandler(func(c paho.Client, err error) {
		if lost := t.release(c); lost != nil {
			lost(fmt.Errorf("%w: %w", ErrConnectionLost, err))
		}
	})

	t.opts = pahoOpts
	t.client = paho.NewClient(pahoOpts)
	t.onLost = nil
	t.closed = make(chan struct{})

	token := t.client.Connect()
	if !token.WaitTimeout(t.ConnectTimeout) {
		return fmt.Errorf("sparkplugb: connect timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("sparkplugb: connect: %w", err)
	}

	t.onLost = opts.OnConnectionLost
	return nil
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

	return t.await(ctx, closed, "publish", func() paho.Token {
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

	return t.await(ctx, closed, "subscribe", func() paho.Token {
		return client.Subscribe(topic, qos, callback)
	})
}

// await runs a paho operation and returns when it completes, ctx ends, the
// connection ends or PublishTimeout passes. op runs in its own goroutine
// because paho can block in the call itself, before it returns a token.
func (t *PahoTransport) await(ctx context.Context, closed <-chan struct{}, what string, op func() paho.Token) error {
	done := make(chan error, 1)
	timeout := fmt.Errorf("sparkplugb: %s timeout", what)
	go func() {
		token := op()
		if !token.WaitTimeout(t.PublishTimeout) {
			done <- timeout
			return
		}
		done <- token.Error()
	}()

	timer := time.NewTimer(t.PublishTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("sparkplugb: %s: %w", what, ctx.Err())
	case <-closed:
		return fmt.Errorf("sparkplugb: %s: %w", what, ErrConnectionLost)
	case <-timer.C:
		return timeout
	}
}

func (t *PahoTransport) Disconnect(ctx context.Context) error {
	t.mu.Lock()
	client := t.client
	t.mu.Unlock()

	lost := t.release(client)
	if client != nil && client.IsConnected() {
		client.Disconnect(250)
	}
	if lost != nil {
		lost(ErrConnectionLost)
	}
	return nil
}
