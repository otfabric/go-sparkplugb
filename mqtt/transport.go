// SPDX-License-Identifier: MIT

package mqtt

import (
	"context"
	"errors"
)

// ErrConnectionLost is passed to ConnectOpts.OnConnectionLost when the
// transport stops being connected, whether the broker dropped the session or
// Disconnect was called on the transport directly.
var ErrConnectionLost = errors.New("sparkplugb: connection lost")

type Transport interface {
	Connect(ctx context.Context, opts ConnectOpts) error
	// Publish sends a message. When Publish returns, with or without an
	// error, the message is either ahead of every message published after
	// that on the connection, or never sent: a Publish that gives up on ctx
	// or a timeout while the message may still go out must not return before
	// its place is fixed. The edge node depends on this to keep the seq of a
	// session in order on the wire across a rebirth.
	Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte) error
	Subscribe(ctx context.Context, topic string, qos byte, handler MessageHandler) error
	Disconnect(ctx context.Context) error
}

type ConnectOpts struct {
	BrokerURL    string
	ClientID     string
	CleanSession bool

	WillTopic    string
	WillPayload  []byte
	WillQoS      byte
	WillRetained bool

	Username string
	Password string

	// OnConnectionLost, if set, is called at most once per Connect when that
	// connection ends. The error wraps ErrConnectionLost. Implementations must
	// not hold internal locks while calling it.
	OnConnectionLost func(err error)
}

type MessageHandler func(topic string, payload []byte)
