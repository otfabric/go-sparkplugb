// SPDX-License-Identifier: MIT

// Package host implements a Sparkplug B Primary Host Application: it publishes
// STATE birth and death certificates, keeps a registry of edge nodes and
// devices, validates seq ordering with reordering and rebirth requests, and
// sends NCMD/DCMD commands.
package host
