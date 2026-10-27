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

	if opts.WillTopic != "" {
		pahoOpts.SetBinaryWill(opts.WillTopic, opts.WillPayload, opts.WillQoS, opts.WillRetained)
	}

	t.opts = pahoOpts
	t.client = paho.NewClient(pahoOpts)

	token := t.client.Connect()
	if !token.WaitTimeout(t.ConnectTimeout) {
		return fmt.Errorf("sparkplugb: connect timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("sparkplugb: connect: %w", err)
	}

	return nil
}

func (t *PahoTransport) Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte) error {
	t.mu.Lock()
	client := t.client
	t.mu.Unlock()

	if client == nil || !client.IsConnected() {
		return fmt.Errorf("sparkplugb: not connected")
	}

	token := client.Publish(topic, qos, retained, payload)
	if !token.WaitTimeout(t.PublishTimeout) {
		return fmt.Errorf("sparkplugb: publish timeout")
	}
	return token.Error()
}

func (t *PahoTransport) Subscribe(ctx context.Context, topic string, qos byte, handler MessageHandler) error {
	t.mu.Lock()
	client := t.client
	t.mu.Unlock()

	if client == nil || !client.IsConnected() {
		return fmt.Errorf("sparkplugb: not connected")
	}

	callback := func(_ paho.Client, msg paho.Message) {
		handler(msg.Topic(), msg.Payload())
	}

	token := client.Subscribe(topic, qos, callback)
	if !token.WaitTimeout(t.PublishTimeout) {
		return fmt.Errorf("sparkplugb: subscribe timeout")
	}
	return token.Error()
}

func (t *PahoTransport) Disconnect(ctx context.Context) error {
	t.mu.Lock()
	client := t.client
	t.client = nil
	t.mu.Unlock()

	if client != nil && client.IsConnected() {
		client.Disconnect(250)
	}
	return nil
}
