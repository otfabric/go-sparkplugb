// SPDX-License-Identifier: MIT

package edge

import "time"

// Metrics receives the events of an edge node, to count and time them:
// set it as Config.Metrics. Each method is called synchronously from the
// goroutines of the node: they must be safe for concurrent use, quick,
// and must not block. Embed NopMetrics to implement only some.
type Metrics interface {
	OnConnect(brokerURL string, d time.Duration, err error)
	// OnPublish is called for each message the node hands to the transport,
	// the NDEATH of Disconnect and Close included, once the transport's
	// Publish has returned: with the message type ("NBIRTH", "DDATA"), how
	// long Publish took, and the error the publish returned. It is called on
	// the goroutine that published, with none of the node's locks held. A
	// message the node refuses before the transport is not reported.
	OnPublish(messageType string, d time.Duration, err error)
	OnMessage(messageType string)
	OnStateChange(old, new State)
}

// NopMetrics implements Metrics with methods that do nothing; embed it.
type NopMetrics struct{}

func (NopMetrics) OnConnect(string, time.Duration, error) {}
func (NopMetrics) OnPublish(string, time.Duration, error) {}
func (NopMetrics) OnMessage(string)                       {}
func (NopMetrics) OnStateChange(State, State)             {}
