// SPDX-License-Identifier: MIT

// Package app implements a Sparkplug B Application that is not a Primary
// Host: it subscribes to the spBv1.0 namespace, tracks edge nodes and devices,
// resolves metric aliases and sends NCMD/DCMD commands, but never publishes
// STATE.
package app
