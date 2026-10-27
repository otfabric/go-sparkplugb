// SPDX-License-Identifier: MIT

// Package interop runs black-box tests of go-sparkplugb against independent
// Sparkplug B 3.0.0 implementations: Eclipse Tahu (Java), pysparkplug,
// SparkplugNet, srad, Eclipse Tahu sparkplug-client (JavaScript) and
// sparkplughost, packaged as container images by
// otfabric/sparkplugb-interop, on the MQTT brokers that release pins
// (Mosquitto, HiveMQ CE, EMQX, NanoMQ).
//
// Every role of go-sparkplugb meets every reference that has the other role:
//
//   - the go-sparkplugb host application (package host) against the
//     reference edge nodes (host_test.go)
//   - the go-sparkplugb edge node (package edge) against the reference host
//     applications (edge_test.go)
//   - the go-sparkplugb application (package app) against the reference
//     edge nodes (app_test.go)
//
// on every fixture the reference serves and every broker: births with
// aliases, alias-only data, writes, rebirth on request and on the edge's own
// initiative, device death and birth, NDEATH, the will, and the primary
// host's STATE.
//
// The images are the only thing shared with sparkplugb-interop: their
// container contract (commands, JSON events and results, exit codes) and the
// fixtures baked into them, which the tests read with "print-fixture". The
// scenarios, expected values and assertions are owned here.
//
// It is a module of its own, as it needs Docker:
//
//	make interop
//
// Environment: SPARKPLUGB_INTEROP_ADAPTERS and SPARKPLUGB_INTEROP_BROKERS
// select a comma-separated subset; SPARKPLUGB_INTEROP_<NAME>_IMAGE overrides
// one image (images.go), and an empty value drops that adapter;
// SPARKPLUGB_INTEROP_PARALLEL bounds how many brokers run at once (default 4).
package interop
