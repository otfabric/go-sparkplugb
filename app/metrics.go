// SPDX-License-Identifier: MIT

package app

import "time"

// Metrics receives the events of an application, to count and time them:
// set it as Config.Metrics. Each method is called synchronously from the
// goroutines of the application: they must be safe for concurrent use,
// quick, and must not block. Embed NopMetrics to implement only some.
type Metrics interface {
	OnConnect(brokerURL string, d time.Duration, err error)
	OnMessage(messageType string)
	OnStateChange(old, new State)
	OnNodeBirth(groupID, nodeID string)
	OnNodeDeath(groupID, nodeID string)
}

// NopMetrics implements Metrics with methods that do nothing; embed it.
type NopMetrics struct{}

func (NopMetrics) OnConnect(string, time.Duration, error) {}
func (NopMetrics) OnMessage(string)                       {}
func (NopMetrics) OnStateChange(State, State)             {}
func (NopMetrics) OnNodeBirth(string, string)             {}
func (NopMetrics) OnNodeDeath(string, string)             {}
