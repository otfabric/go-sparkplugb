// SPDX-License-Identifier: MIT

// Package interop holds tests of go-sparkplugb's edge node, host application
// and application against real MQTT brokers: the container images pinned in
// images.go.
//
//   - Edge lifecycle: connect, birth, data, device birth/death, disconnect.
//   - Host discovery: host sees edge births and tracks node state.
//   - Alias round-trip: birth with aliases, data with alias-only metrics.
//   - Rebirth cycle: host requests rebirth, edge re-publishes birth.
//
// Each test runs against every broker in the matrix (Mosquitto, HiveMQ CE,
// EMQX, NanoMQ). The images are consumed as published: nothing of
// sparkplugb-interop is cloned or built here.
//
// It is a module of its own, as it needs Docker:
//
//	make test-interop
package interop
