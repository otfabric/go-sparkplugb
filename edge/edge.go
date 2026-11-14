// SPDX-License-Identifier: MIT

package edge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	sparkplugb "github.com/otfabric/go-sparkplugb"
	"github.com/otfabric/go-sparkplugb/internal/callback"
	"github.com/otfabric/go-sparkplugb/mqtt"
	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
	"google.golang.org/protobuf/proto"
)

var (
	ErrClosed       = errors.New("sparkplugb: closed")
	ErrNotConnected = errors.New("sparkplugb: not connected")
	ErrNoBirth      = errors.New("sparkplugb: birth not published")
	// ErrDuplicateID is returned by PublishBirth and PublishDeviceBirth when an
	// alias is used twice in one birth or by another part of the edge node.
	ErrDuplicateID = errors.New("sparkplugb: duplicate device or alias ID")
	// ErrInvalidState is not returned by this package.
	//
	// Deprecated: operations in the wrong state return ErrNotConnected, ErrNoBirth
	// or ErrClosed. It stays for source compatibility.
	ErrInvalidState = errors.New("sparkplugb: invalid state transition")
)

type State uint8

const (
	Disconnected State = iota
	Connecting
	Subscribing
	WaitingForPrimaryHost
	Online
	Closed
)

var stateStrings = [...]string{
	"disconnected", "connecting", "subscribing",
	"waiting_for_primary_host", "online", "closed",
}

func (s State) String() string {
	if int(s) < len(stateStrings) {
		return stateStrings[s]
	}
	return "unknown"
}

// StateHandler is called with each transition of the node's state, in the
// order of the transitions and one call at a time, after the node has
// released its locks: on the goroutine that made the transition (the caller
// of Connect, Disconnect or Close, the transport's for a lost connection or
// the primary host's STATE, or the node's own while it walks to another
// server), or on one that was delivering an earlier transition when it was
// made. It may call the node: PublishBirth when the state becomes Online,
// Close, or Connect after Disconnected. A transition such a call makes is
// delivered after the handler returns. Disconnected after Disconnect, or
// after a Connect that failed, is delivered once the node has ended the
// connection, so a Connect from the handler is not ended with it. A handler
// that does not return normally is logged with its stack, and the node goes
// on.
//
// The node does not publish an NBIRTH by itself: every session needs one,
// after Connect and after each walk to another server, so publish it when
// the state becomes Online.
type StateHandler func(old, new State)

// CommandHandler is called with the metrics of an NCMD (deviceID "") or a
// DCMD, on the transport's goroutine and without a lock of the node held,
// so it may call the node; the next message waits until it returns, so it
// should not block. The metrics are the handler's. A handler that does not
// return normally is logged with its stack, and the node goes on.
type CommandHandler func(deviceID string, metrics []*pb.Payload_Metric)

type Config struct {
	GroupID    string
	EdgeNodeID string
	BrokerURL  string
	ClientID   string

	PrimaryHostID string
	ServerURLs    []string

	// Username and Password log in to every broker of the node. A Password
	// needs a Username. They are passed to the transport and never logged; use
	// TLS, since MQTT carries them in clear.
	Username string
	Password string

	Logger      sparkplugb.Logger
	Metrics     Metrics
	RetryPolicy sparkplugb.RetryPolicy
	OnState     StateHandler
	OnNodeCmd   CommandHandler
	OnDeviceCmd CommandHandler

	BdSeqStore BdSeqStore
}

type BdSeqStore interface {
	Load() (uint64, error)
	Store(bdSeq uint64) error
}

type memBdSeqStore struct {
	mu    sync.Mutex
	bdSeq uint64
}

func (s *memBdSeqStore) Load() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bdSeq, nil
}

func (s *memBdSeqStore) Store(bdSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bdSeq = bdSeq
	return nil
}

type Node struct {
	cfg       Config
	transport mqtt.Transport

	mu      sync.Mutex
	state   State
	closed  bool // Close was called: Connect refuses, and one in progress ends with ErrClosed
	bdSeq   uint64
	seq     uint64
	devices map[string]*Device
	order   []string          // device IDs in the order of their first DBIRTH
	aliases map[uint64]string // alias -> metric name
	owners  map[uint64]string // alias -> device ID, "" for the node
	born    bool
	gen     uint64 // connection generation; prevents stale callbacks
	closeCh chan struct{}

	transitions callback.Queue[transition] // state transitions not yet delivered (notify)

	// Birth state for auto-rebirth
	birthMetrics []*pb.Payload_Metric
	// templates are the template definitions of the last NBIRTH: the
	// instances of later messages must refer to them.
	templates payload.TemplateDefs

	// Server walking
	serverIdx int
	walking   bool

	// hostOnlineTS is the timestamp of the latest online STATE of the
	// primary host: an offline STATE older than it is ignored.
	hostOnlineTS int64

	// seqMu keeps the messages of a session in sequence on the wire: it is
	// held from taking a seq until the message is published, and by Rebirth
	// for its whole birth sequence, so no message of the ending session
	// follows the NBIRTH and no data goes between it and the DBIRTHs. Taken
	// before pubMu and mu.
	seqMu sync.Mutex
	// lastTS is the payload timestamp of the last message handed to the
	// transport, under seqMu: the next NBIRTH is stamped later (birthTime).
	lastTS         uint64
	now            func() time.Time // the clock of birthTime
	pubMu          sync.RWMutex     // held to publish; Disconnect takes it to end the session
	rebirthMu      sync.Mutex
	rebirthPending bool
}

// transition is a change of the node's state, for StateHandler.
type transition struct{ old, new State }

type Device struct {
	ID      string
	Metrics []*pb.Payload_Metric
	Online  bool
}

func New(transport mqtt.Transport, cfg Config) (*Node, error) {
	if cfg.GroupID == "" || cfg.EdgeNodeID == "" {
		return nil, errors.New("sparkplugb: GroupID and EdgeNodeID are required")
	}
	if cfg.BrokerURL == "" && len(cfg.ServerURLs) == 0 {
		return nil, errors.New("sparkplugb: BrokerURL or ServerURLs required")
	}
	if cfg.Password != "" && cfg.Username == "" {
		return nil, errors.New("sparkplugb: Password requires Username")
	}
	for _, id := range []struct{ name, value string }{
		{"GroupID", cfg.GroupID},
		{"EdgeNodeID", cfg.EdgeNodeID},
		{"PrimaryHostID", cfg.PrimaryHostID},
	} {
		if id.value != "" && !validID(id.value) {
			return nil, fmt.Errorf("sparkplugb: %s %q: %w", id.name, id.value, topic.ErrInvalidID)
		}
	}
	if cfg.BdSeqStore == nil {
		cfg.BdSeqStore = &memBdSeqStore{}
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NopMetrics{}
	}
	if cfg.RetryPolicy == nil {
		cfg.RetryPolicy = sparkplugb.ExponentialBackoff{}
	}
	cfg.Logger = callback.Logger(sparkplugb.ComponentLogger(cfg.Logger, "edge"))
	if cfg.ClientID == "" {
		cfg.ClientID = fmt.Sprintf("sparkplug_%s_%s", cfg.GroupID, cfg.EdgeNodeID)
	}

	return &Node{
		cfg:       cfg,
		transport: transport,
		devices:   make(map[string]*Device),
		aliases:   make(map[uint64]string),
		owners:    make(map[uint64]string),
		closeCh:   make(chan struct{}),
		now:       time.Now,
	}, nil
}

func (n *Node) Connect(ctx context.Context) error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return ErrClosed
	}
	n.setState(Connecting)

	var bdSeq uint64
	var err error
	fail := callback.Try("BdSeqStore.Load", func() { bdSeq, err = n.cfg.BdSeqStore.Load() })
	if fail != nil {
		err = fail
	}
	if err != nil {
		n.setState(Disconnected)
		n.mu.Unlock()
		callback.Report(n.cfg.Logger, fail)
		n.notify()
		return fmt.Errorf("sparkplugb: load bdSeq: %w", err)
	}
	n.bdSeq = (bdSeq + 1) % 256

	fail = callback.Try("BdSeqStore.Store", func() { err = n.cfg.BdSeqStore.Store(n.bdSeq) })
	if fail != nil {
		err = fail
	}
	if err != nil {
		n.setState(Disconnected)
		n.mu.Unlock()
		callback.Report(n.cfg.Logger, fail)
		n.notify()
		return fmt.Errorf("sparkplugb: store bdSeq: %w", err)
	}

	willPayload := n.buildNDEATH()
	willTopic := topic.Topic{
		GroupID:    n.cfg.GroupID,
		Type:       topic.NDEATH,
		EdgeNodeID: n.cfg.EdgeNodeID,
	}.String()

	willBytes, err := payload.Encode(willPayload)
	if err != nil {
		n.setState(Disconnected)
		n.mu.Unlock()
		n.notify()
		return fmt.Errorf("sparkplugb: encode NDEATH: %w", err)
	}

	n.gen++
	connGen := n.gen
	n.mu.Unlock()
	n.notify()

	brokerURL := n.brokerURL()

	err = n.transport.Connect(ctx, mqtt.ConnectOpts{
		BrokerURL:    brokerURL,
		ClientID:     n.cfg.ClientID,
		CleanSession: true,
		WillTopic:    willTopic,
		WillPayload:  willBytes,
		WillQoS:      1,
		WillRetained: false,
		Username:     n.cfg.Username,
		Password:     n.cfg.Password,

		OnConnectionLost: n.makeConnLostHandler(connGen),
	})
	if err != nil {
		n.mu.Lock()
		closed := n.closed
		if !closed {
			n.setState(Disconnected)
		}
		n.mu.Unlock()
		n.notify()
		if closed {
			return ErrClosed
		}
		return fmt.Errorf("sparkplugb: connect: %w", err)
	}

	if !n.advance(Subscribing) {
		return n.abortConnect(ctx, ErrClosed)
	}

	ncmdTopic := topic.ForNCMD(n.cfg.GroupID, n.cfg.EdgeNodeID)
	if err := n.transport.Subscribe(ctx, ncmdTopic, 1, n.makeHandler(connGen)); err != nil {
		return n.abortConnect(ctx, fmt.Errorf("sparkplugb: subscribe NCMD: %w", err))
	}

	dcmdTopic := topic.Topic{
		GroupID:    n.cfg.GroupID,
		Type:       topic.DCMD,
		EdgeNodeID: n.cfg.EdgeNodeID,
		DeviceID:   "+",
	}.String()
	if err := n.transport.Subscribe(ctx, dcmdTopic, 1, n.makeHandler(connGen)); err != nil {
		return n.abortConnect(ctx, fmt.Errorf("sparkplugb: subscribe DCMD: %w", err))
	}

	if n.cfg.PrimaryHostID != "" {
		// Waiting before subscribing: the retained STATE can arrive before
		// Subscribe returns, and it only counts while the node waits for it.
		if !n.advance(WaitingForPrimaryHost) {
			return n.abortConnect(ctx, ErrClosed)
		}
		stateTopic := topic.ForState(n.cfg.PrimaryHostID)
		if err := n.transport.Subscribe(ctx, stateTopic, 1, n.makeStateHandler(connGen)); err != nil {
			return n.abortConnect(ctx, fmt.Errorf("sparkplugb: subscribe STATE: %w", err))
		}
		n.mu.Lock()
		closed := n.closed
		n.mu.Unlock()
		if closed {
			return n.abortConnect(ctx, ErrClosed)
		}
	} else if !n.advance(Online) {
		return n.abortConnect(ctx, ErrClosed)
	}

	return nil
}

// advance moves a Connect in progress to state s, and reports true, unless
// Close was called meanwhile: then the state stays as Close leaves it.
func (n *Node) advance(s State) bool {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return false
	}
	n.setState(s)
	n.mu.Unlock()
	n.notify()
	return true
}

// abortConnect ends a connection on which the session could not be set up,
// or that Close ended meanwhile, and returns err, or ErrClosed after Close.
// Left up, the connection kept its will armed and the node between
// Connecting and Online, and the next Connect opened a second one beside
// it. No NBIRTH went out on it, so no NDEATH is owed: a DISCONNECT ends it
// and discards the will.
func (n *Node) abortConnect(ctx context.Context, err error) error {
	n.mu.Lock()
	if n.state != Closed {
		n.setState(Disconnected)
	}
	if n.closed {
		err = ErrClosed
	}
	n.mu.Unlock()
	_ = n.transport.Disconnect(context.WithoutCancel(ctx))
	// Delivered once the connection has ended: a StateHandler that calls
	// Connect when the node is Disconnected starts on a transport that is
	// free, and its connection is not the one ended here.
	n.notify()
	return err
}

// PublishBirth publishes the NBIRTH of a new session with nodeMetrics, after
// bdSeq and "Node Control/Rebirth". It sets the timestamp of a metric that
// has none. The node keeps copies of the metrics for Rebirth: a change the
// caller makes to its metrics afterwards is not published by a rebirth
// until it is sent as data, and the caller may go on using its metrics.
func (n *Node) PublishBirth(ctx context.Context, nodeMetrics []*pb.Payload_Metric) error {
	n.seqMu.Lock()
	defer n.seqMu.Unlock()
	return n.publishBirth(ctx, nodeMetrics)
}

func (n *Node) publishBirth(ctx context.Context, nodeMetrics []*pb.Payload_Metric) error {
	n.mu.Lock()
	if n.state != Online {
		n.mu.Unlock()
		return ErrNotConnected
	}
	// An NBIRTH starts a new session, so only its own metrics can collide.
	if err := aliasConflict(nil, "", nodeMetrics); err != nil {
		n.mu.Unlock()
		return err
	}
	// Template instances of this node and its devices refer to the
	// definitions of this NBIRTH (tck-id-topics-nbirth-templates).
	templates, err := payload.NewTemplateDefs(nodeMetrics)
	if err != nil {
		n.mu.Unlock()
		return err
	}
	n.templates = templates
	n.seq = 0
	n.born = true
	n.aliases = make(map[uint64]string)
	n.owners = make(map[uint64]string)

	// Copies for the rebirth, stamped as the NBIRTH is: the caller's
	// metrics stay the caller's.
	payload.StampMetrics(nodeMetrics)
	n.birthMetrics = cloneSlice(nodeMetrics)

	// Register aliases from birth metrics
	for _, m := range nodeMetrics {
		if m.Alias != nil {
			n.aliases[m.GetAlias()] = m.GetName()
			n.owners[m.GetAlias()] = ""
		}
	}

	seq := n.seq
	bdSeq := n.bdSeq
	n.mu.Unlock()

	birthMetrics := make([]*pb.Payload_Metric, 0, len(nodeMetrics)+2)
	birthMetrics = append(birthMetrics, payload.BdSeqMetric(bdSeq))
	// Sparkplug 3.0.0 requires "Node Control/Rebirth" in every NBIRTH, so
	// that a host knows it can ask for one. Added unless the caller has it.
	if !hasMetric(nodeMetrics, rebirthMetricName) {
		birthMetrics = append(birthMetrics, payload.RebirthMetric(false))
	}
	birthMetrics = append(birthMetrics, nodeMetrics...)

	p := payload.NewPayload(seq, birthMetrics...)
	ts := n.birthTime()
	p.Timestamp = &ts
	return n.publish(ctx, topic.NBIRTH, "", p)
}

// birthTime returns the timestamp of an NBIRTH: now, or the next millisecond
// when the last message went out in this one. A host tells a message of the
// ending session from one of the new session by timestamp only, so the
// message before an NBIRTH must be older than it, not of its millisecond
// (go-sparkplugb#24). The wait is a millisecond at most.
//
// A clock that went back is not waited for: the NBIRTH is stamped earlier
// than the message before it, and a host may take that message for one of
// the new session. Waiting would hold every publish of the node until the
// clock caught up; stamping the NBIRTH ahead of the clock would make a host
// drop the reordered messages of the new session as older than it.
// Called with seqMu held.
func (n *Node) birthTime() uint64 {
	for {
		ts := uint64(n.now().UnixMilli())
		if ts != n.lastTS {
			return ts
		}
		time.Sleep(time.Until(time.UnixMilli(int64(ts) + 1)))
	}
}

// PublishDeviceBirth publishes the DBIRTH of a device. It sets the timestamp
// of a metric that has none, and keeps copies of the metrics for Rebirth, as
// PublishBirth does.
func (n *Node) PublishDeviceBirth(ctx context.Context, deviceID string, metrics []*pb.Payload_Metric) error {
	n.seqMu.Lock()
	defer n.seqMu.Unlock()
	return n.publishDeviceBirth(ctx, deviceID, metrics)
}

func (n *Node) publishDeviceBirth(ctx context.Context, deviceID string, metrics []*pb.Payload_Metric) error {
	n.mu.Lock()
	if !n.born {
		n.mu.Unlock()
		return ErrNoBirth
	}
	if err := aliasConflict(n.owners, deviceID, metrics); err != nil {
		n.mu.Unlock()
		return err
	}
	if err := n.templates.Check(metrics, true); err != nil {
		n.mu.Unlock()
		return err
	}

	if _, exists := n.devices[deviceID]; !exists {
		n.devices[deviceID] = &Device{ID: deviceID}
		n.order = append(n.order, deviceID)
	}
	payload.StampMetrics(metrics)
	n.devices[deviceID].Metrics = cloneSlice(metrics)
	n.devices[deviceID].Online = true

	// Register aliases from device birth metrics
	for _, m := range metrics {
		if m.Alias != nil {
			n.aliases[m.GetAlias()] = m.GetName()
			n.owners[m.GetAlias()] = deviceID
		}
	}

	n.seq = (n.seq + 1) % 256
	seq := n.seq
	n.mu.Unlock()

	p := payload.NewPayload(seq, metrics...)
	return n.publish(ctx, topic.DBIRTH, deviceID, p)
}

// aliasConflict returns ErrDuplicateID when metrics use an alias twice, or one
// that owners gives to another part of the edge node: Sparkplug requires an
// alias to be unique across the edge node. A device repeating its own birth
// keeps its aliases.
func aliasConflict(owners map[uint64]string, owner string, metrics []*pb.Payload_Metric) error {
	seen := make(map[uint64]struct{}, len(metrics))
	for _, m := range metrics {
		if m.Alias == nil {
			continue
		}
		alias := m.GetAlias()
		if _, dup := seen[alias]; dup {
			return fmt.Errorf("sparkplugb: alias %d used twice in a birth: %w", alias, ErrDuplicateID)
		}
		seen[alias] = struct{}{}
		if prev, taken := owners[alias]; taken && prev != owner {
			return fmt.Errorf("sparkplugb: alias %d of device %q is already in use: %w", alias, owner, ErrDuplicateID)
		}
	}
	return nil
}

func (n *Node) PublishNodeData(ctx context.Context, metrics []*pb.Payload_Metric) error {
	n.seqMu.Lock()
	defer n.seqMu.Unlock()
	return n.publishNodeData(ctx, metrics)
}

func (n *Node) publishNodeData(ctx context.Context, metrics []*pb.Payload_Metric) error {
	n.mu.Lock()
	if !n.born {
		n.mu.Unlock()
		return ErrNoBirth
	}
	if err := n.templates.Check(metrics, false); err != nil {
		n.mu.Unlock()
		return err
	}
	n.birthMetrics = n.withCurrent(n.birthMetrics, metrics)
	n.seq = (n.seq + 1) % 256
	seq := n.seq
	n.mu.Unlock()

	payload.StampMetrics(metrics)
	p := payload.NewPayload(seq, metrics...)
	return n.publish(ctx, topic.NDATA, "", p)
}

func (n *Node) PublishDeviceData(ctx context.Context, deviceID string, metrics []*pb.Payload_Metric) error {
	n.seqMu.Lock()
	defer n.seqMu.Unlock()
	return n.publishDeviceData(ctx, deviceID, metrics)
}

func (n *Node) publishDeviceData(ctx context.Context, deviceID string, metrics []*pb.Payload_Metric) error {
	n.mu.Lock()
	if !n.born {
		n.mu.Unlock()
		return ErrNoBirth
	}
	if err := n.templates.Check(metrics, false); err != nil {
		n.mu.Unlock()
		return err
	}
	if dev, ok := n.devices[deviceID]; ok {
		dev.Metrics = n.withCurrent(dev.Metrics, metrics)
	}
	n.seq = (n.seq + 1) % 256
	seq := n.seq
	n.mu.Unlock()

	payload.StampMetrics(metrics)
	p := payload.NewPayload(seq, metrics...)
	return n.publish(ctx, topic.DDATA, deviceID, p)
}

func (n *Node) PublishDeviceDeath(ctx context.Context, deviceID string) error {
	n.seqMu.Lock()
	defer n.seqMu.Unlock()
	return n.publishDeviceDeath(ctx, deviceID)
}

func (n *Node) publishDeviceDeath(ctx context.Context, deviceID string) error {
	n.mu.Lock()
	if !n.born {
		n.mu.Unlock()
		return ErrNoBirth
	}
	if dev, ok := n.devices[deviceID]; ok {
		dev.Online = false
	}
	n.seq = (n.seq + 1) % 256
	seq := n.seq
	n.mu.Unlock()

	p := payload.NewPayload(seq)
	return n.publish(ctx, topic.DDEATH, deviceID, p)
}

// Rebirth re-publishes the full birth sequence: NBIRTH followed by DBIRTH
// for each online device, in the order the devices were first born. It
// resets the sequence counter per the spec. Messages published meanwhile wait
// for it, and follow the births with the seq of the new session. The NBIRTH
// is stamped in a later millisecond than the message before it, unless the
// clock went back.
func (n *Node) Rebirth(ctx context.Context) error {
	n.seqMu.Lock()
	defer n.seqMu.Unlock()
	n.mu.Lock()
	if n.state != Online {
		n.mu.Unlock()
		return ErrNotConnected
	}
	birthMetrics := n.birthMetrics
	type birth struct {
		id      string
		metrics []*pb.Payload_Metric
	}
	var births []birth
	for _, id := range n.order {
		if dev := n.devices[id]; dev.Online {
			births = append(births, birth{id, dev.Metrics})
		}
	}
	n.mu.Unlock()

	if err := n.publishBirth(ctx, birthMetrics); err != nil {
		return fmt.Errorf("sparkplugb: rebirth NBIRTH: %w", err)
	}

	for _, b := range births {
		if err := n.publishDeviceBirth(ctx, b.id, b.metrics); err != nil {
			return fmt.Errorf("sparkplugb: rebirth DBIRTH %s: %w", b.id, err)
		}
	}
	return nil
}

func (n *Node) Disconnect(ctx context.Context) error {
	// The session ends first, and a message on its way goes out before the
	// NDEATH: a rebirth requested now must not follow it.
	n.pubMu.Lock()
	n.mu.Lock()
	if n.state == Closed || n.state == Disconnected {
		n.mu.Unlock()
		n.pubMu.Unlock()
		return nil
	}
	var ndeath []byte
	if n.born {
		if b, err := payload.Encode(n.buildNDEATH()); err == nil {
			ndeath = b
		}
	}
	n.born = false
	n.birthMetrics = nil
	n.setState(Disconnected)
	n.mu.Unlock()
	n.pubMu.Unlock()

	if ndeath != nil {
		t := topic.Topic{
			GroupID:    n.cfg.GroupID,
			Type:       topic.NDEATH,
			EdgeNodeID: n.cfg.EdgeNodeID,
		}.String()
		_ = n.transport.Publish(ctx, t, 1, false, ndeath)
	}
	err := n.transport.Disconnect(ctx)
	n.notify() // once the connection has ended, as in abortConnect
	return err
}

// Close disconnects the node, as Disconnect does, and closes it: Connect
// then returns ErrClosed, and a Connect in progress, a walk to another server
// included, ends with ErrClosed and leaves no connection up. Close is safe
// to call more than once and from several goroutines: the first call closes
// the node, the others return nil at once.
func (n *Node) Close(ctx context.Context) error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	n.mu.Unlock()

	err := n.Disconnect(ctx)
	n.mu.Lock()
	n.setState(Closed)
	close(n.closeCh)
	n.mu.Unlock()
	n.notify()
	return err
}

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) BdSeq() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.bdSeq
}

func (n *Node) Seq() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.seq
}

// ResolveAlias returns the metric name for a given alias, if registered.
func (n *Node) ResolveAlias(alias uint64) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	name, ok := n.aliases[alias]
	return name, ok
}

func (n *Node) makeConnLostHandler(connGen uint64) func(error) {
	return func(err error) {
		n.mu.Lock()
		if n.gen != connGen || n.state == Closed || n.state == Disconnected {
			n.mu.Unlock()
			return
		}
		n.cfg.Logger.Warnf("transport lost: %v", err)
		n.born = false
		n.setState(Disconnected)
		n.mu.Unlock()
		n.notify()
	}
}

// setState changes the state and queues the transition for notify, which
// the caller runs once it holds none of the node's locks. Called with n.mu
// held.
func (n *Node) setState(s State) {
	old := n.state
	n.state = s
	if old != s {
		n.cfg.Logger.Infof("state: %s -> %s", old, s)
		n.transitions.Push(transition{old, s})
	}
}

// notify delivers the queued state transitions to Metrics.OnStateChange and
// OnState, in order. Called with none of the node's locks held.
func (n *Node) notify() {
	n.transitions.Deliver(func(tr transition) {
		callback.Run(n.cfg.Logger, "Metrics.OnStateChange", func() { n.cfg.Metrics.OnStateChange(tr.old, tr.new) })
		if n.cfg.OnState != nil {
			callback.Run(n.cfg.Logger, "Config.OnState", func() { n.cfg.OnState(tr.old, tr.new) })
		}
	})
}

func (n *Node) buildNDEATH() *pb.Payload {
	ts := uint64(time.Now().UnixMilli())
	return &pb.Payload{
		Timestamp: &ts,
		Metrics:   []*pb.Payload_Metric{payload.BdSeqMetric(n.bdSeq)},
	}
}

func (n *Node) publish(ctx context.Context, mt topic.MessageType, deviceID string, p *pb.Payload) error {
	t := topic.Topic{
		GroupID:    n.cfg.GroupID,
		Type:       mt,
		EdgeNodeID: n.cfg.EdgeNodeID,
		DeviceID:   deviceID,
	}.String()

	b, err := payload.Encode(p)
	if err != nil {
		return fmt.Errorf("sparkplugb: encode %s: %w", mt, err)
	}

	n.pubMu.RLock()
	defer n.pubMu.RUnlock()
	n.mu.Lock()
	online := n.state == Online
	n.mu.Unlock()
	if !online {
		return ErrNotConnected
	}
	n.lastTS = p.GetTimestamp() // under seqMu, as every caller holds it
	return n.transport.Publish(ctx, t, 0, false, b)
}

func (n *Node) makeHandler(connGen uint64) mqtt.MessageHandler {
	return func(topicStr string, data []byte) {
		n.mu.Lock()
		if n.gen != connGen {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		n.handleMessage(topicStr, data)
	}
}

func (n *Node) makeStateHandler(connGen uint64) mqtt.MessageHandler {
	return func(topicStr string, data []byte) {
		n.mu.Lock()
		if n.gen != connGen {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		n.handleState(topicStr, data)
	}
}

func (n *Node) handleMessage(topicStr string, data []byte) {
	t, err := topic.Parse(topicStr)
	if err != nil {
		n.cfg.Logger.Warnf("ignoring unparseable topic %q: %v", topicStr, err)
		return
	}

	p, err := payload.Decode(data)
	if err != nil {
		n.cfg.Logger.Warnf("ignoring malformed payload on %q: %v", topicStr, err)
		return
	}

	callback.Run(n.cfg.Logger, "Metrics.OnMessage", func() { n.cfg.Metrics.OnMessage(t.Type.String()) })

	switch t.Type {
	case topic.NCMD:
		n.handleNCMD(p)
	case topic.DCMD:
		if n.cfg.OnDeviceCmd != nil {
			callback.Run(n.cfg.Logger, "Config.OnDeviceCmd", func() { n.cfg.OnDeviceCmd(t.DeviceID, p.Metrics) })
		}
	}
}

const rebirthMetricName = "Node Control/Rebirth"

// withCurrent returns the birth metrics with the values of the data metrics
// that name them, by name or by alias, so that a rebirth publishes current
// values as Sparkplug requires. The birth keeps its names, aliases and
// datatypes; neither slice is modified. A template instance in data may have
// a subset of its members: the birth keeps the others, since an instance in a
// birth must have all of them. Called with n.mu held.
func (n *Node) withCurrent(birth, data []*pb.Payload_Metric) []*pb.Payload_Metric {
	out := make([]*pb.Payload_Metric, len(birth))
	copy(out, birth)
	for _, d := range data {
		name := d.GetName()
		if name == "" && d.Alias != nil {
			name = n.aliases[d.GetAlias()]
		}
		for i, b := range out {
			if name == "" || b.GetName() != name {
				continue
			}
			c := proto.Clone(d).(*pb.Payload_Metric)
			c.Name, c.Alias, c.Datatype = b.Name, b.Alias, b.Datatype
			if bt, dt := b.GetTemplateValue(), c.GetTemplateValue(); bt != nil && dt != nil {
				c.Value = &pb.Payload_Metric_TemplateValue{TemplateValue: mergeTemplate(bt, dt)}
			}
			out[i] = c
		}
	}
	return out
}

// mergeTemplate returns the birth instance b with the members and parameters
// that data instance d has, by name; a member without a datatype takes the
// one of b. Members that are templates are merged the same way. b and d are
// not modified.
func mergeTemplate(b, d *pb.Payload_Template) *pb.Payload_Template {
	out := proto.Clone(b).(*pb.Payload_Template)
	for _, dm := range d.GetMetrics() {
		for i, bm := range out.Metrics {
			if dm.GetName() == "" || bm.GetName() != dm.GetName() {
				continue
			}
			c := proto.Clone(dm).(*pb.Payload_Metric)
			if c.Datatype == nil {
				c.Datatype = bm.Datatype
			}
			if bt, dt := bm.GetTemplateValue(), c.GetTemplateValue(); bt != nil && dt != nil {
				c.Value = &pb.Payload_Metric_TemplateValue{TemplateValue: mergeTemplate(bt, dt)}
			}
			out.Metrics[i] = c
		}
	}
	for _, dp := range d.GetParameters() {
		for i, bp := range out.Parameters {
			if bp.GetName() == dp.GetName() {
				out.Parameters[i] = proto.Clone(dp).(*pb.Payload_Template_Parameter)
			}
		}
	}
	return out
}

// validID reports whether id can be an ID segment of a Sparkplug topic: not
// empty, valid UTF-8, and without '/', '+' or '#'.
func validID(id string) bool {
	_, err := topic.Parse(topic.ForState(id))
	return err == nil
}

// cloneSlice returns deep copies of metrics.
func cloneSlice(metrics []*pb.Payload_Metric) []*pb.Payload_Metric {
	out := make([]*pb.Payload_Metric, len(metrics))
	for i, m := range metrics {
		out[i] = proto.Clone(m).(*pb.Payload_Metric)
	}
	return out
}

func hasMetric(metrics []*pb.Payload_Metric, name string) bool {
	for _, m := range metrics {
		if m.GetName() == name {
			return true
		}
	}
	return false
}

func (n *Node) handleNCMD(p *pb.Payload) {
	for _, m := range p.Metrics {
		if m.GetName() == rebirthMetricName && m.GetBooleanValue() {
			n.cfg.Logger.Infof("rebirth requested")
			n.rebirthMu.Lock()
			if !n.rebirthPending {
				n.rebirthPending = true
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := n.Rebirth(ctx); err != nil {
						n.cfg.Logger.Errorf("auto-rebirth failed: %v", err)
					}
					n.rebirthMu.Lock()
					n.rebirthPending = false
					n.rebirthMu.Unlock()
				}()
			}
			n.rebirthMu.Unlock()
			return
		}
	}
	if n.cfg.OnNodeCmd != nil {
		callback.Run(n.cfg.Logger, "Config.OnNodeCmd", func() { n.cfg.OnNodeCmd("", p.Metrics) })
	}
}

type statePayload struct {
	Online    bool  `json:"online"`
	Timestamp int64 `json:"timestamp"`
}

func (n *Node) handleState(topicStr string, data []byte) {
	var sp statePayload
	if err := json.Unmarshal(data, &sp); err != nil {
		n.cfg.Logger.Warnf("ignoring malformed STATE payload: %v", err)
		return
	}

	n.mu.Lock()
	walk := false
	switch {
	case sp.Online:
		n.hostOnlineTS = max(n.hostOnlineTS, sp.Timestamp)
		if n.state == WaitingForPrimaryHost {
			n.setState(Online)
		}
	// The primary host went offline. Sparkplug 3.0: the node disconnects
	// from this server and starts over on the next one of its list (the same
	// one when it has only one), unless the offline STATE is older than the
	// last online one: that is the end of an earlier session of the host.
	case n.state == Online && sp.Timestamp >= n.hostOnlineTS && !n.walking:
		n.walking = true
		walk = true
	}
	n.mu.Unlock()
	n.notify()

	if walk {
		go n.walkServers()
	}
}

func (n *Node) brokerURL() string {
	if len(n.cfg.ServerURLs) > 0 {
		return n.cfg.ServerURLs[n.serverIdx%len(n.cfg.ServerURLs)]
	}
	return n.cfg.BrokerURL
}

func (n *Node) walkServers() {
	defer func() {
		n.mu.Lock()
		n.walking = false
		n.mu.Unlock()
	}()

	// An intentional disconnect: NDEATH first, then DISCONNECT.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = n.Disconnect(ctx)
	cancel()

	for attempt := 0; ; attempt++ {
		n.mu.Lock()
		if n.closed {
			n.mu.Unlock()
			return
		}
		if len(n.cfg.ServerURLs) > 0 {
			n.serverIdx = (n.serverIdx + 1) % len(n.cfg.ServerURLs)
		}
		url := n.brokerURL()
		n.mu.Unlock()

		n.cfg.Logger.Infof("walking to server %s (attempt %d)", url, attempt)

		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := n.Connect(ctx)
		cancel()
		callback.Run(n.cfg.Logger, "Metrics.OnConnect", func() { n.cfg.Metrics.OnConnect(url, time.Since(start), err) })
		if err == nil || errors.Is(err, ErrClosed) {
			return
		}
		n.cfg.Logger.Warnf("walk connect failed: %v", err)

		// A RetryPolicy that does not return normally ends the walk.
		var retry bool
		var wait time.Duration
		callback.Run(n.cfg.Logger, "RetryPolicy.ShouldRetry", func() { retry, wait = n.cfg.RetryPolicy.ShouldRetry(attempt, err) })
		if !retry {
			n.cfg.Logger.Errorf("walk giving up after %d attempts", attempt+1)
			return
		}
		n.cfg.Logger.Infof("retrying in %v", wait)
		select {
		case <-time.After(wait):
		case <-n.closeCh:
			return
		}
	}
}
