// SPDX-License-Identifier: MIT

package sparkplugb

// SessionChecks selects stricter checks that a host application (package
// host) or an application (package app) makes on the data messages of an edge
// node around its NBIRTH, to drop a message of the session that ended instead
// of applying it to the new one. The zero value makes none: that is the
// default, and what Sparkplug B 3.0 asks for.
//
// Sparkplug carries no session in NDATA and DDATA, and MQTT orders messages
// per topic only, so a broker may deliver a message of the ending session
// among those of the new one (go-sparkplugb#24, LIMITATIONS.md). Without these
// checks two such messages are applied to the new session; each check drops
// one of them, at the cost of dropping valid messages of edge nodes that do
// not meet its assumption. A dropped message of the new session leaves a gap
// in its sequence, and the gap costs a rebirth. Births and deaths are never
// dropped by these checks.
//
// Use them only for edge nodes whose payload timestamp is the time of
// publishing, from a clock that does not go back, and that stamp no birth
// before the NBIRTH it follows, as this module's edge package does. host.Config
// and app.Config take them for every edge node (SessionChecks) or per edge
// node (SessionChecksFor).
type SessionChecks uint8

const (
	// CheckBirthMillisecond drops an NDATA or DDATA that arrived before the
	// NBIRTH of its edge node and is stamped in the millisecond of that
	// NBIRTH, or was applied before it with that timestamp. By default it is
	// taken for the new session's: births and the data right after them share
	// a millisecond. With this check, a new-session NDATA or DDATA stamped in
	// the NBIRTH's millisecond that the broker delivers before the NBIRTH is
	// dropped.
	CheckBirthMillisecond SessionChecks = 1 << iota

	// CheckOlderInSequence drops an NDATA or DDATA that carries the sequence
	// number the session expects next but is stamped before the session's
	// NBIRTH, within 30 s of that NBIRTH. By default a message in sequence is
	// applied whatever its timestamp. With this check, data of an edge node
	// that stamps a payload earlier than its time of publishing (the time a
	// value was sampled, say) or whose clock goes back is dropped after each
	// NBIRTH.
	CheckOlderInSequence

	// SessionChecksAll is every check.
	SessionChecksAll = CheckBirthMillisecond | CheckOlderInSequence
)

// SessionDropMetrics is implemented by the Metrics of a host or application
// that counts the data messages and births it drops as belonging to a session
// that has ended: those published before the current NBIRTH and out of
// sequence, which are always dropped, and those the SessionChecks drop.
// OnSessionDrop is called with the edge node and the message type (NDATA,
// DDATA, DBIRTH or DDEATH), with none of the runtime's locks held.
type SessionDropMetrics interface {
	OnSessionDrop(groupID, edgeNodeID, messageType string)
}

// HeldDropMetrics is implemented by the Metrics of a host or application
// that counts the messages it drops from those it holds for reordering. The
// held messages are bounded over all edge nodes, at 4096 messages and 8 MiB
// of payload (SECURITY.md); to make room for the next one, the oldest held
// message of any edge node is dropped. OnHeldDrop is called for each message
// so dropped, with its edge node and message type (NDATA, DDATA, DBIRTH or
// DDEATH), with none of the runtime's locks held. A held message that waited
// longer than 30 s for the gap before it is dropped without this call.
type HeldDropMetrics interface {
	OnHeldDrop(groupID, edgeNodeID, messageType string)
}
