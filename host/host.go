// SPDX-License-Identifier: MIT

package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	sparkplugb "github.com/otfabric/go-sparkplugb"
	"github.com/otfabric/go-sparkplugb/mqtt"
	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
	"google.golang.org/protobuf/proto"
)

var (
	ErrClosed       = errors.New("sparkplugb: host closed")
	ErrNotConnected = errors.New("sparkplugb: host not connected")
)

type State uint8

const (
	Disconnected State = iota
	Connecting
	Connected
	Closed
)

var stateStrings = [...]string{"disconnected", "connecting", "connected", "closed"}

func (s State) String() string {
	if int(s) < len(stateStrings) {
		return stateStrings[s]
	}
	return "unknown"
}

type StateHandler func(old, new State)

type NodeHandler func(groupID, nodeID string, metrics []*pb.Payload_Metric)
type DeviceHandler func(groupID, nodeID, deviceID string, metrics []*pb.Payload_Metric)
type NodeEventHandler func(groupID, nodeID string)
type DeviceEventHandler func(groupID, nodeID, deviceID string)

type Config struct {
	HostID    string
	BrokerURL string
	ClientID  string

	ServerURLs []string

	// Username and Password log in to every broker. A Password needs a
	// Username. They are passed to the transport and never logged; use TLS,
	// since MQTT carries them in clear.
	Username string
	Password string

	ReorderTimeout time.Duration

	Logger        sparkplugb.Logger
	Metrics       Metrics
	OnState       StateHandler
	OnNodeBirth   NodeHandler
	OnDeviceBirth DeviceHandler
	OnNodeData    NodeHandler
	OnDeviceData  DeviceHandler
	OnNodeDeath   NodeEventHandler
	OnDeviceDeath DeviceEventHandler
}

type EdgeNodeState struct {
	GroupID    string
	EdgeNodeID string
	Online     bool
	BdSeq      uint64
	LastSeq    uint64
	Metrics    map[string]*pb.Payload_Metric
	Devices    map[string]*DeviceState
	Aliases    map[uint64]string // alias -> metric name
	Stale      bool
}

type DeviceState struct {
	DeviceID string
	Online   bool
	Metrics  map[string]*pb.Payload_Metric
	Aliases  map[uint64]string
	Stale    bool
}

type App struct {
	cfg       Config
	transport mqtt.Transport

	mu            sync.RWMutex
	state         State
	nodes         map[string]*EdgeNodeState
	closeCh       chan struct{}
	reorderTimers map[string]*time.Timer // nodeKey -> pending rebirth timer
	rebirthAsked  map[string]time.Time   // nodeKey -> last rebirth request the host made itself
	rebirthMissed map[string]bool        // nodeKey -> a request was throttled while one was pending
	deaths        map[string]death       // nodeKey -> last NDEATH
	held          map[string][]heldMsg   // nodeKey -> messages ahead of their sequence
	sessions      map[string]*session    // nodeKey -> current session
	sessionTimers map[string]*time.Timer // nodeKey -> pending wait for an NBIRTH
	sessionWait   time.Duration          // sessionWait, shorter in tests
}

func New(transport mqtt.Transport, cfg Config) (*App, error) {
	if cfg.HostID == "" {
		return nil, errors.New("sparkplugb: HostID required")
	}
	if cfg.Password != "" && cfg.Username == "" {
		return nil, errors.New("sparkplugb: Password requires Username")
	}
	if cfg.BrokerURL == "" && len(cfg.ServerURLs) == 0 {
		return nil, errors.New("sparkplugb: BrokerURL or ServerURLs required")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = fmt.Sprintf("sparkplug_host_%s", cfg.HostID)
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NopMetrics{}
	}
	cfg.Logger = sparkplugb.ComponentLogger(cfg.Logger, "host")

	return &App{
		cfg:           cfg,
		transport:     transport,
		nodes:         make(map[string]*EdgeNodeState),
		closeCh:       make(chan struct{}),
		reorderTimers: make(map[string]*time.Timer),
		sessionWait:   sessionWait,
	}, nil
}

func (a *App) Connect(ctx context.Context) error {
	a.mu.Lock()
	if a.state == Closed {
		a.mu.Unlock()
		return ErrClosed
	}
	a.setState(Connecting)
	a.mu.Unlock()

	ts := time.Now().UnixMilli()
	willPayload, _ := json.Marshal(statePayload{Online: false, Timestamp: ts})
	stateTopic := topic.ForState(a.cfg.HostID)

	brokerURL := a.cfg.BrokerURL
	if brokerURL == "" && len(a.cfg.ServerURLs) > 0 {
		brokerURL = a.cfg.ServerURLs[0]
	}

	err := a.transport.Connect(ctx, mqtt.ConnectOpts{
		BrokerURL:    brokerURL,
		ClientID:     a.cfg.ClientID,
		CleanSession: true,
		WillTopic:    stateTopic,
		WillPayload:  willPayload,
		WillQoS:      1,
		WillRetained: true,
		Username:     a.cfg.Username,
		Password:     a.cfg.Password,

		OnConnectionLost: a.onConnectionLost,
	})
	if err != nil {
		a.mu.Lock()
		a.setState(Disconnected)
		a.mu.Unlock()
		return fmt.Errorf("sparkplugb: host connect: %w", err)
	}

	if err := a.transport.Subscribe(ctx, stateTopic, 1, a.handleMessage); err != nil {
		return fmt.Errorf("sparkplugb: subscribe STATE: %w", err)
	}

	if err := a.transport.Subscribe(ctx, topic.Namespace+"/#", 1, a.handleMessage); err != nil {
		return fmt.Errorf("sparkplugb: subscribe spBv1.0/#: %w", err)
	}

	birthPayload, _ := json.Marshal(statePayload{Online: true, Timestamp: ts})
	if err := a.transport.Publish(ctx, stateTopic, 1, true, birthPayload); err != nil {
		return fmt.Errorf("sparkplugb: publish STATE birth: %w", err)
	}

	a.mu.Lock()
	a.setState(Connected)
	a.mu.Unlock()

	return nil
}

func (a *App) Disconnect(ctx context.Context) error {
	a.mu.Lock()
	if a.state == Closed || a.state == Disconnected {
		a.mu.Unlock()
		return nil
	}
	a.setState(Disconnected)
	for k, t := range a.reorderTimers {
		t.Stop()
		delete(a.reorderTimers, k)
	}
	a.stopSessionTimers()
	a.mu.Unlock()

	ts := time.Now().UnixMilli()
	deathPayload, _ := json.Marshal(statePayload{Online: false, Timestamp: ts})
	stateTopic := topic.ForState(a.cfg.HostID)
	_ = a.transport.Publish(ctx, stateTopic, 1, true, deathPayload)

	return a.transport.Disconnect(ctx)
}

func (a *App) Close(ctx context.Context) error {
	a.mu.Lock()
	if a.state == Closed {
		a.mu.Unlock()
		return nil
	}
	a.mu.Unlock()

	err := a.Disconnect(ctx)
	a.mu.Lock()
	a.setState(Closed)
	close(a.closeCh)
	a.mu.Unlock()
	return err
}

func (a *App) SendNodeCommand(ctx context.Context, groupID, nodeID string, metrics []*pb.Payload_Metric) error {
	a.mu.RLock()
	if a.state != Connected {
		a.mu.RUnlock()
		return ErrNotConnected
	}
	a.mu.RUnlock()

	p := &pb.Payload{Metrics: metrics}
	ts := uint64(time.Now().UnixMilli())
	p.Timestamp = &ts

	b, err := payload.Encode(p)
	if err != nil {
		return fmt.Errorf("sparkplugb: encode NCMD: %w", err)
	}

	t := topic.Topic{
		GroupID:    groupID,
		Type:       topic.NCMD,
		EdgeNodeID: nodeID,
	}.String()

	return a.transport.Publish(ctx, t, 0, false, b)
}

func (a *App) SendDeviceCommand(ctx context.Context, groupID, nodeID, deviceID string, metrics []*pb.Payload_Metric) error {
	a.mu.RLock()
	if a.state != Connected {
		a.mu.RUnlock()
		return ErrNotConnected
	}
	a.mu.RUnlock()

	p := &pb.Payload{Metrics: metrics}
	ts := uint64(time.Now().UnixMilli())
	p.Timestamp = &ts

	b, err := payload.Encode(p)
	if err != nil {
		return fmt.Errorf("sparkplugb: encode DCMD: %w", err)
	}

	t := topic.Topic{
		GroupID:    groupID,
		Type:       topic.DCMD,
		EdgeNodeID: nodeID,
		DeviceID:   deviceID,
	}.String()

	return a.transport.Publish(ctx, t, 0, false, b)
}

func (a *App) RequestRebirth(ctx context.Context, groupID, nodeID string) error {
	return a.SendNodeCommand(ctx, groupID, nodeID, []*pb.Payload_Metric{
		payload.RebirthMetric(true),
	})
}

// GetNode returns a snapshot of what is known about an edge node, or nil.
// The snapshot is a copy: it does not change as messages arrive, and reading
// it needs no locking.
func (a *App) GetNode(groupID, nodeID string) *EdgeNodeState {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.nodes[nodeKey(groupID, nodeID)].clone()
}

// Nodes returns a snapshot of every edge node known, as GetNode does.
func (a *App) Nodes() []*EdgeNodeState {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]*EdgeNodeState, 0, len(a.nodes))
	for _, n := range a.nodes {
		out = append(out, n.clone())
	}
	return out
}

func (a *App) State() State {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.state
}

// ResolveAlias resolves a metric alias for a given edge node.
func (a *App) ResolveAlias(groupID, nodeID string, alias uint64) (string, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	ns := a.nodes[nodeKey(groupID, nodeID)]
	if ns == nil {
		return "", false
	}
	if name, ok := ns.Aliases[alias]; ok {
		return name, true
	}
	for _, ds := range ns.Devices {
		if name, ok := ds.Aliases[alias]; ok {
			return name, true
		}
	}
	return "", false
}

func (a *App) onConnectionLost(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state == Closed || a.state == Disconnected {
		return
	}
	a.cfg.Logger.Warnf("transport lost: %v", err)
	for k, t := range a.reorderTimers {
		t.Stop()
		delete(a.reorderTimers, k)
	}
	a.stopSessionTimers()
	a.setState(Disconnected)
}

func (a *App) setState(s State) {
	old := a.state
	a.state = s
	if old != s {
		a.cfg.Logger.Infof("host state: %s -> %s", old, s)
		a.cfg.Metrics.OnStateChange(old, s)
		if a.cfg.OnState != nil {
			a.cfg.OnState(old, s)
		}
	}
}

type statePayload struct {
	Online    bool  `json:"online"`
	Timestamp int64 `json:"timestamp"`
}

func (a *App) handleMessage(topicStr string, data []byte) {
	t, err := topic.Parse(topicStr)
	if err != nil {
		return
	}

	switch t.Type {
	case topic.STATE:
		return
	case topic.NBIRTH:
		a.handleNBIRTH(t, data)
		a.drain(t)
	case topic.NDEATH:
		a.handleNDEATH(t, data)
	case topic.DBIRTH, topic.DDEATH, topic.NDATA, topic.DDATA:
		if a.hold(t, data) {
			return
		}
		a.dispatch(t, data)
		a.drain(t)
	}
}

func (a *App) dispatch(t topic.Topic, data []byte) {
	switch t.Type {
	case topic.DBIRTH:
		a.handleDBIRTH(t, data)
	case topic.DDEATH:
		a.handleDDEATH(t, data)
	case topic.NDATA:
		a.handleNDATA(t, data)
	case topic.DDATA:
		a.handleDDATA(t, data)
	}
}

// maxHeld is how many messages per edge node are held for reordering.
const maxHeld = 256

// heldMsg is a message that arrived ahead of its sequence number, or behind
// it (nextSession): a message of the edge node's next session that the broker
// delivered before that session's NBIRTH.
type heldMsg struct {
	t           topic.Topic
	seq         uint64
	data        []byte
	ts          uint64 // payload timestamp
	at          time.Time
	nextSession bool
}

// session is the current session of an edge node, as far as its NBIRTH tells:
// messages carry no session number but bdSeq in the births and deaths.
type session struct {
	birthTS uint64    // payload timestamp of the NBIRTH
	bornAt  time.Time // when the NBIRTH arrived
	applied []heldMsg // the last messages applied to this session
}

// maxApplied is how many applied messages a session keeps, for the next
// NBIRTH to take back what was its own (see handleNBIRTH).
const maxApplied = 32

func (s *session) remember(m heldMsg) {
	if s == nil {
		return
	}
	if len(s.applied) == maxApplied {
		s.applied = append(s.applied[:0], s.applied[1:]...)
	}
	s.applied = append(s.applied, m)
}

// hold keeps a message that cannot be applied yet: one from an edge node
// whose NBIRTH has not arrived, or one ahead of the next sequence number.
// MQTT orders messages per topic only, so a broker may deliver the messages
// of one edge node out of publishing order (NanoMQ does); they are applied in
// sequence order once the gap is filled (see drain). A rebirth is requested
// as for a message that is lost. A message behind the expected sequence
// number is held for the next session: a restarted node starts again at 0,
// and its DBIRTH may arrive before its NBIRTH. It is applied after that
// NBIRTH. When that NBIRTH does not come, a rebirth is requested (see
// expectSession): either the message is lost, or the host is ahead of the
// edge node and would otherwise hold everything it sends.
//
// A message published before the current NBIRTH, out of sequence, belongs to
// a session that has ended (see superseded). It is dropped: applied to this
// session it would take the place of a message of this session, or carry
// the sequence number past the edge node's.
func (a *App) hold(t topic.Topic, data []byte) bool {
	p, err := payload.Decode(data)
	if err != nil {
		return false // the handler reports it
	}
	seq, ts := p.GetSeq(), p.GetTimestamp()
	key := nodeKey(t.GroupID, t.EdgeNodeID)
	a.mu.Lock()
	defer a.mu.Unlock()
	ns := a.nodes[key]
	behind := false
	if ns != nil {
		expected := (ns.LastSeq + 1) % 256
		if seq == expected {
			a.sessions[key].remember(heldMsg{t: t, seq: seq, data: data, ts: ts})
			return false
		}
		if a.superseded(key, ts) {
			a.cfg.Logger.Debugf("%s/%s: dropped %s, seq %d: published before the NBIRTH",
				t.GroupID, t.EdgeNodeID, t.Type, seq)
			return true
		}
		behind = (seq-expected+256)%256 > 128
	}
	if a.held == nil {
		a.held = make(map[string][]heldMsg)
	}
	if len(a.held[key]) >= maxHeld {
		return false
	}
	a.held[key] = append(a.held[key], heldMsg{t: t, seq: seq, data: data, ts: ts, at: time.Now(), nextSession: behind})
	switch {
	case behind:
		a.expectSession(t)
	case ns == nil:
		a.unknownNode(t)
	default:
		a.validateSeq(ns, seq, t) // starts the reorder timer or requests a rebirth
	}
	return true
}

// heldMaxAge is how long a held message may wait for the gap before it.
const heldMaxAge = 30 * time.Second

// superseded reports whether a message with payload timestamp ts was
// published before the NBIRTH of the edge node's current session, so by an
// earlier session: Sparkplug has the timestamp be the time of publishing, and
// the edge node's clock stamps both. Only for heldMaxAge after the NBIRTH: a
// late message is late by that much at most, and an edge node whose clock
// goes back must not lose messages for longer. A message without a timestamp
// is not judged. Called with a.mu held.
func (a *App) superseded(key string, ts uint64) bool {
	s, ok := a.sessions[key]
	return ok && ts != 0 && ts < s.birthTS && time.Since(s.bornAt) <= heldMaxAge
}

// expectSession gives the NBIRTH that a message behind the expected sequence
// number announces time to arrive, and requests a rebirth when it does not.
// Called with a.mu held.
func (a *App) expectSession(t topic.Topic) {
	key := nodeKey(t.GroupID, t.EdgeNodeID)
	if _, pending := a.sessionTimers[key]; pending {
		return
	}
	if a.sessionTimers == nil {
		a.sessionTimers = make(map[string]*time.Timer)
	}
	a.sessionTimers[key] = time.AfterFunc(a.sessionWait, func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		if _, pending := a.sessionTimers[key]; !pending {
			return
		}
		delete(a.sessionTimers, key)
		a.askRebirth(t, "message behind the sequence, no NBIRTH followed")
	})
}

// stopSessionTimers ends every wait for a NBIRTH. Called with a.mu held.
func (a *App) stopSessionTimers() {
	for k, t := range a.sessionTimers {
		t.Stop()
		delete(a.sessionTimers, k)
	}
}

// drain applies the held messages of the edge node that are next in
// sequence, one after the other, and drops those that waited too long.
func (a *App) drain(t topic.Topic) {
	key := nodeKey(t.GroupID, t.EdgeNodeID)
	for {
		a.mu.Lock()
		ns, held := a.nodes[key], a.held[key]
		if ns == nil || len(held) == 0 {
			a.mu.Unlock()
			return
		}
		expected := (ns.LastSeq + 1) % 256
		var (
			next  *heldMsg
			kept  = make([]heldMsg, 0, len(held))
			found bool
		)
		for i := range held {
			if !held[i].nextSession && held[i].seq == expected && (next == nil || held[i].ts >= next.ts) {
				next, found = &held[i], true // the latest of a seq sent twice
			}
		}
		for i := range held {
			switch {
			case &held[i] == next:
			case time.Since(held[i].at) > heldMaxAge:
			default:
				kept = append(kept, held[i])
			}
		}
		a.held[key] = kept
		if found {
			a.sessions[key].remember(*next)
		} else {
			for i := range kept {
				if !kept[i].nextSession && (kept[i].seq-expected+256)%256 > 128 {
					a.expectSession(t) // two sessions shared this seq
					break
				}
			}
		}
		a.mu.Unlock()
		if !found {
			return
		}
		m := *next
		a.dispatch(m.t, m.data)
	}
}

// death is the last NDEATH of an edge node.
type death struct {
	bdSeq uint64
	at    time.Time
}

func bdSeqOf(p *pb.Payload) (uint64, bool) {
	for _, m := range p.Metrics {
		if m.GetName() == "bdSeq" {
			return m.GetLongValue(), true
		}
	}
	return 0, false
}

// lateBirth reports whether an NBIRTH is of a session whose NDEATH arrived
// just before it. MQTT orders messages per topic only, so a broker may deliver
// a will ahead of a rebirth that was in flight when the edge node died; that
// NBIRTH must not bring a dead node back online. A message carries no proof
// of being late, and an edge node that restarts with the bdSeq of its NDEATH
// looks the same (srad starts at 0), so for sessionWait after the NDEATH such
// an NBIRTH is dropped and a rebirth is requested after that: a live edge node
// answers it, and comes online then. died reports that the node went offline
// here: its NDEATH had a bdSeq ahead of the session the host knew, and was
// ignored until this NBIRTH matched it. Called with a.mu held.
func (a *App) lateBirth(t topic.Topic, p *pb.Payload) (late, died bool) {
	key := nodeKey(t.GroupID, t.EdgeNodeID)
	d, ok := a.deaths[key]
	if !ok {
		return false, false
	}
	if bd, has := bdSeqOf(p); !has || bd != d.bdSeq || time.Since(d.at) > a.sessionWait {
		delete(a.deaths, key)
		return false, false
	}
	delete(a.rebirthAsked, key) // what it answered is dropped with it
	a.expectSession(t)
	if ns := a.nodes[key]; ns != nil && ns.Online {
		setOffline(ns)
		died = true
	}
	return true, died
}

func setOffline(ns *EdgeNodeState) {
	ns.Online = false
	ns.Stale = true
	for _, dev := range ns.Devices {
		dev.Online = false
		dev.Stale = true
	}
}

func (a *App) handleNBIRTH(t topic.Topic, data []byte) {
	p, err := payload.Decode(data)
	if err != nil {
		a.logWarn("decode NBIRTH from %s/%s: %v", t.GroupID, t.EdgeNodeID, err)
		return
	}

	a.mu.Lock()
	if late, died := a.lateBirth(t, p); late {
		a.mu.Unlock()
		a.cfg.Logger.Debugf("%s/%s: dropped NBIRTH: its session has ended", t.GroupID, t.EdgeNodeID)
		if died {
			a.cfg.Metrics.OnNodeDeath(t.GroupID, t.EdgeNodeID)
			if a.cfg.OnNodeDeath != nil {
				a.cfg.OnNodeDeath(t.GroupID, t.EdgeNodeID)
			}
		}
		return
	}
	key := nodeKey(t.GroupID, t.EdgeNodeID)
	ns := &EdgeNodeState{
		GroupID:    t.GroupID,
		EdgeNodeID: t.EdgeNodeID,
		Online:     true,
		LastSeq:    p.GetSeq(),
		Metrics:    make(map[string]*pb.Payload_Metric),
		Devices:    make(map[string]*DeviceState),
		Aliases:    make(map[uint64]string),
	}
	for _, m := range p.Metrics {
		if m.GetName() == "bdSeq" {
			ns.BdSeq = m.GetLongValue()
		} else {
			ns.Metrics[m.GetName()] = m
			if m.Alias != nil {
				ns.Aliases[m.GetAlias()] = m.GetName()
			}
		}
	}
	a.nodes[key] = ns
	if a.sessions == nil {
		a.sessions = make(map[string]*session)
	}
	birthTS := p.GetTimestamp()
	if prev := a.sessions[key]; prev != nil && birthTS != 0 {
		// A message newer than this NBIRTH that the broker delivered before
		// it, whose seq happened to be the next of the session that just
		// ended, is of this session: the state it went into is gone. So is a
		// DBIRTH of the same millisecond, which an edge node's births share.
		var ours []heldMsg
		for _, m := range prev.applied {
			if m.ts > birthTS || m.ts == birthTS && m.t.Type == topic.DBIRTH {
				m.at = time.Now()
				ours = append(ours, m)
			}
		}
		if len(ours) > 0 {
			if a.held == nil {
				a.held = make(map[string][]heldMsg)
			}
			a.held[key] = append(ours, a.held[key]...) // on a tie, drain takes the held one
		}
	}
	a.sessions[key] = &session{birthTS: birthTS, bornAt: time.Now()}
	a.answeredRebirth(t)
	if timer, ok := a.sessionTimers[key]; ok {
		timer.Stop()
		delete(a.sessionTimers, key)
	}
	if held := a.held[key]; len(held) > 0 {
		kept := make([]heldMsg, 0, len(held))
		for _, m := range held {
			if a.superseded(key, m.ts) {
				continue // of a session that has ended
			}
			m.nextSession = false // this NBIRTH starts their session
			kept = append(kept, m)
		}
		a.held[key] = kept
	}
	if timer, ok := a.reorderTimers[key]; ok {
		timer.Stop()
		delete(a.reorderTimers, key)
	}
	a.mu.Unlock()

	a.cfg.Metrics.OnNodeBirth(t.GroupID, t.EdgeNodeID)
	if a.cfg.OnNodeBirth != nil {
		a.cfg.OnNodeBirth(t.GroupID, t.EdgeNodeID, p.Metrics)
	}
}

func (a *App) handleNDEATH(t topic.Topic, data []byte) {
	p, err := payload.Decode(data)
	if err != nil {
		a.logWarn("decode NDEATH from %s/%s: %v", t.GroupID, t.EdgeNodeID, err)
		return
	}

	a.mu.Lock()
	key := nodeKey(t.GroupID, t.EdgeNodeID)
	if bd, ok := bdSeqOf(p); ok {
		if a.deaths == nil {
			a.deaths = make(map[string]death)
		}
		a.deaths[key] = death{bdSeq: bd, at: time.Now()}
	}
	ns := a.nodes[key]
	if ns != nil {
		for _, m := range p.Metrics {
			if m.GetName() == "bdSeq" {
				if m.GetLongValue() != ns.BdSeq {
					a.mu.Unlock()
					a.logWarn("NDEATH bdSeq mismatch for %s/%s: got %d, expected %d",
						t.GroupID, t.EdgeNodeID, m.GetLongValue(), ns.BdSeq)
					return
				}
			}
		}
		setOffline(ns)
	}
	a.mu.Unlock()

	a.cfg.Metrics.OnNodeDeath(t.GroupID, t.EdgeNodeID)
	if a.cfg.OnNodeDeath != nil {
		a.cfg.OnNodeDeath(t.GroupID, t.EdgeNodeID)
	}
}

func (a *App) handleDBIRTH(t topic.Topic, data []byte) {
	p, err := payload.Decode(data)
	if err != nil {
		a.logWarn("decode DBIRTH from %s/%s/%s: %v", t.GroupID, t.EdgeNodeID, t.DeviceID, err)
		return
	}

	a.mu.Lock()
	ns := a.nodes[nodeKey(t.GroupID, t.EdgeNodeID)]
	if ns == nil {
		a.unknownNode(t)
	}
	if ns != nil {
		if !a.validateSeq(ns, p.GetSeq(), t) {
			a.mu.Unlock()
			return
		}
		ds := &DeviceState{
			DeviceID: t.DeviceID,
			Online:   ns.Online,
			Metrics:  make(map[string]*pb.Payload_Metric),
			Aliases:  make(map[uint64]string),
		}
		for _, m := range p.Metrics {
			ds.Metrics[m.GetName()] = m
			if m.Alias != nil {
				ds.Aliases[m.GetAlias()] = m.GetName()
			}
		}
		ns.Devices[t.DeviceID] = ds
	}
	a.mu.Unlock()

	if a.cfg.OnDeviceBirth != nil {
		a.cfg.OnDeviceBirth(t.GroupID, t.EdgeNodeID, t.DeviceID, p.Metrics)
	}
}

func (a *App) handleDDEATH(t topic.Topic, data []byte) {
	p, err := payload.Decode(data)
	if err != nil {
		a.logWarn("decode DDEATH from %s/%s/%s: %v", t.GroupID, t.EdgeNodeID, t.DeviceID, err)
		return
	}

	a.mu.Lock()
	ns := a.nodes[nodeKey(t.GroupID, t.EdgeNodeID)]
	if ns == nil {
		a.unknownNode(t)
	}
	if ns != nil {
		if !a.validateSeq(ns, p.GetSeq(), t) {
			a.mu.Unlock()
			return
		}
		if ds, ok := ns.Devices[t.DeviceID]; ok {
			ds.Online = false
			ds.Stale = true
		}
	}
	a.mu.Unlock()

	if a.cfg.OnDeviceDeath != nil {
		a.cfg.OnDeviceDeath(t.GroupID, t.EdgeNodeID, t.DeviceID)
	}
}

func (a *App) handleNDATA(t topic.Topic, data []byte) {
	p, err := payload.Decode(data)
	if err != nil {
		a.logWarn("decode NDATA from %s/%s: %v", t.GroupID, t.EdgeNodeID, err)
		return
	}

	a.mu.Lock()
	ns := a.nodes[nodeKey(t.GroupID, t.EdgeNodeID)]
	if ns == nil {
		a.unknownNode(t)
	}
	if ns != nil {
		if !a.validateSeq(ns, p.GetSeq(), t) {
			a.mu.Unlock()
			return
		}
		for _, m := range p.Metrics {
			name := a.resolveMetricName(ns, m)
			if name != "" {
				ns.Metrics[name] = withBirth(ns.Metrics[name], m, name)
			}
		}
	}
	a.mu.Unlock()

	if a.cfg.OnNodeData != nil {
		a.cfg.OnNodeData(t.GroupID, t.EdgeNodeID, p.Metrics)
	}
}

func (a *App) handleDDATA(t topic.Topic, data []byte) {
	p, err := payload.Decode(data)
	if err != nil {
		a.logWarn("decode DDATA from %s/%s/%s: %v", t.GroupID, t.EdgeNodeID, t.DeviceID, err)
		return
	}

	a.mu.Lock()
	ns := a.nodes[nodeKey(t.GroupID, t.EdgeNodeID)]
	if ns == nil {
		a.unknownNode(t)
	}
	if ns != nil {
		if !a.validateSeq(ns, p.GetSeq(), t) {
			a.mu.Unlock()
			return
		}
		if ds, ok := ns.Devices[t.DeviceID]; ok {
			for _, m := range p.Metrics {
				name := a.resolveDeviceMetricName(ds, m)
				if name != "" {
					ds.Metrics[name] = withBirth(ds.Metrics[name], m, name)
				}
			}
		}
	}
	a.mu.Unlock()

	if a.cfg.OnDeviceData != nil {
		a.cfg.OnDeviceData(t.GroupID, t.EdgeNodeID, t.DeviceID, p.Metrics)
	}
}

// resolveMetricName returns the metric name, resolving alias if needed.
func (a *App) resolveMetricName(ns *EdgeNodeState, m *pb.Payload_Metric) string {
	if name := m.GetName(); name != "" {
		return name
	}
	if m.Alias != nil {
		if name, ok := ns.Aliases[m.GetAlias()]; ok {
			return name
		}
	}
	return ""
}

// resolveDeviceMetricName returns the metric name, resolving alias if needed.
func (a *App) resolveDeviceMetricName(ds *DeviceState, m *pb.Payload_Metric) string {
	if name := m.GetName(); name != "" {
		return name
	}
	if m.Alias != nil {
		if name, ok := ds.Aliases[m.GetAlias()]; ok {
			return name
		}
	}
	return ""
}

func (a *App) validateSeq(ns *EdgeNodeState, seq uint64, t topic.Topic) bool {
	expected := (ns.LastSeq + 1) % 256
	if seq != expected {
		key := nodeKey(t.GroupID, t.EdgeNodeID)
		if a.cfg.ReorderTimeout > 0 {
			if _, pending := a.reorderTimers[key]; !pending {
				a.logWarn("seq gap on %s/%s: got %d, expected %d — waiting %v for reorder",
					t.GroupID, t.EdgeNodeID, seq, expected, a.cfg.ReorderTimeout)
				a.reorderTimers[key] = time.AfterFunc(a.cfg.ReorderTimeout, func() {
					a.mu.Lock()
					_, stillPending := a.reorderTimers[key]
					delete(a.reorderTimers, key)
					a.mu.Unlock()
					if !stillPending {
						return
					}
					a.mu.Lock()
					a.askRebirth(t, "reorder timeout")
					a.mu.Unlock()
				})
			}
		} else {
			a.askRebirth(t, fmt.Sprintf("seq gap: got %d, expected %d", seq, expected))
		}
		return false
	}
	ns.LastSeq = seq
	key := nodeKey(t.GroupID, t.EdgeNodeID)
	if timer, ok := a.reorderTimers[key]; ok {
		timer.Stop()
		delete(a.reorderTimers, key)
	}
	return true
}

func (a *App) logWarn(format string, args ...any) {
	a.cfg.Logger.Warnf(format, args...)
}

func nodeKey(groupID, nodeID string) string {
	return groupID + "/" + nodeID
}

// withBirth returns the data metric m as the state keeps it: under its name
// and with the datatype of the birth when m carries only an alias or no
// datatype, as Sparkplug allows after the birth. m is not modified.
func withBirth(prev, m *pb.Payload_Metric, name string) *pb.Payload_Metric {
	c := proto.Clone(m).(*pb.Payload_Metric)
	c.Name = &name
	if c.Datatype == nil && prev != nil && prev.Datatype != nil {
		dt := prev.GetDatatype()
		c.Datatype = &dt
	}
	return c
}

// clone returns a deep copy of the state, nil for nil.
func (s *EdgeNodeState) clone() *EdgeNodeState {
	if s == nil {
		return nil
	}
	c := *s
	c.Metrics = cloneMetrics(s.Metrics)
	c.Aliases = make(map[uint64]string, len(s.Aliases))
	for k, v := range s.Aliases {
		c.Aliases[k] = v
	}
	c.Devices = make(map[string]*DeviceState, len(s.Devices))
	for id, d := range s.Devices {
		dc := *d
		dc.Metrics = cloneMetrics(d.Metrics)
		dc.Aliases = make(map[uint64]string, len(d.Aliases))
		for k, v := range d.Aliases {
			dc.Aliases[k] = v
		}
		c.Devices[id] = &dc
	}
	return &c
}

func cloneMetrics(in map[string]*pb.Payload_Metric) map[string]*pb.Payload_Metric {
	out := make(map[string]*pb.Payload_Metric, len(in))
	for k, m := range in {
		out[k] = proto.Clone(m).(*pb.Payload_Metric)
	}
	return out
}

// rebirthInterval is how long a rebirth request the host makes on its own
// (a seq gap, a reorder timeout, a node whose NBIRTH has not been seen) waits for its NBIRTH: until it arrives no other request goes to
// that edge node (see answeredRebirth). A broker that reorders messages
// across topics (HiveMQ CE, NanoMQ) makes several messages in a row look out
// of sequence; one request each would make the node publish one NBIRTH
// each, a rebirth storm.
const rebirthInterval = 2 * time.Second

// sessionWait is how long a message behind the expected sequence number
// waits for an NBIRTH before a rebirth is requested. Reordering across topics
// is a matter of milliseconds: the wait only has to be longer than that, and
// short enough that the host does not sit on a stale view.
const sessionWait = 2 * time.Second

// unknownNode requests a rebirth from an edge node that sent a message
// before any NBIRTH was seen: a host joining while the node is online, or an
// NBIRTH that a broker delivered after the messages that followed it (MQTT
// orders messages per topic only). Called with a.mu held.
func (a *App) unknownNode(t topic.Topic) {
	a.askRebirth(t, fmt.Sprintf("%s before its NBIRTH", t.Type))
}

// askRebirth requests a rebirth from the edge node of t, at most once per
// rebirthInterval. Called with a.mu held.
func (a *App) askRebirth(t topic.Topic, reason string) {
	key := nodeKey(t.GroupID, t.EdgeNodeID)
	if a.rebirthAsked == nil {
		a.rebirthAsked = make(map[string]time.Time)
	}
	if last, ok := a.rebirthAsked[key]; ok && time.Since(last) < rebirthInterval {
		if a.rebirthMissed == nil {
			a.rebirthMissed = make(map[string]bool)
		}
		a.rebirthMissed[key] = true
		return
	}
	a.rebirthAsked[key] = time.Now()
	a.logWarn("%s/%s: %s — requesting rebirth", t.GroupID, t.EdgeNodeID, reason)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.RequestRebirth(ctx, t.GroupID, t.EdgeNodeID)
	}()
}

// answeredRebirth is called with a.mu held when an NBIRTH arrives: the
// pending rebirth request, if any, is answered. A request throttled while it
// was pending is made now: its message may be a DBIRTH of this new session
// that the broker delivered before the NBIRTH, which the NBIRTH has dropped.
func (a *App) answeredRebirth(t topic.Topic) {
	key := nodeKey(t.GroupID, t.EdgeNodeID)
	delete(a.rebirthAsked, key)
	if a.rebirthMissed[key] {
		delete(a.rebirthMissed, key)
		a.askRebirth(t, "out of sequence before this NBIRTH")
	}
}
