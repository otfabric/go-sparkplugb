// SPDX-License-Identifier: MIT

package app

import (
	"context"
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
	ErrClosed       = errors.New("sparkplugb: application closed")
	ErrNotConnected = errors.New("sparkplugb: application not connected")
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

// Config configures a Sparkplug B Application. Unlike a Host Application, an
// Application does not publish STATE messages and is not a Primary Host. It
// subscribes to spBv1.0/#, tracks edge nodes and devices, and can send commands.
type Config struct {
	BrokerURL  string
	ClientID   string
	ServerURLs []string

	// Username and Password log in to every broker. A Password needs a
	// Username. They are passed to the transport and never logged; use TLS,
	// since MQTT carries them in clear.
	Username string
	Password string

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
	Aliases    map[uint64]string
	Stale      bool
}

type DeviceState struct {
	DeviceID string
	Online   bool
	Metrics  map[string]*pb.Payload_Metric
	Aliases  map[uint64]string
	Stale    bool
}

// Application is a Sparkplug B Application (non-primary-host consumer). It
// subscribes to the Sparkplug namespace, tracks node/device state, resolves
// aliases, and can send NCMD/DCMD commands — but never publishes STATE.
type Application struct {
	cfg       Config
	transport mqtt.Transport

	mu      sync.RWMutex
	state   State
	nodes   map[string]*EdgeNodeState
	closeCh chan struct{}

	rebirthAsked  map[string]time.Time // nodeKey -> last rebirth request the application made itself
	rebirthMissed map[string]bool      // nodeKey -> a request was throttled while one was pending
	held          map[string][]heldMsg // nodeKey -> messages ahead of their sequence
}

func New(transport mqtt.Transport, cfg Config) (*Application, error) {
	if cfg.BrokerURL == "" && len(cfg.ServerURLs) == 0 {
		return nil, errors.New("sparkplugb: BrokerURL or ServerURLs required")
	}
	if cfg.Password != "" && cfg.Username == "" {
		return nil, errors.New("sparkplugb: Password requires Username")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = fmt.Sprintf("sparkplug_app_%d", time.Now().UnixNano()%100000)
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NopMetrics{}
	}
	cfg.Logger = sparkplugb.ComponentLogger(cfg.Logger, "app")

	return &Application{
		cfg:       cfg,
		transport: transport,
		nodes:     make(map[string]*EdgeNodeState),
		closeCh:   make(chan struct{}),
	}, nil
}

func (a *Application) Connect(ctx context.Context) error {
	a.mu.Lock()
	if a.state == Closed {
		a.mu.Unlock()
		return ErrClosed
	}
	a.setState(Connecting)
	a.mu.Unlock()

	brokerURL := a.cfg.BrokerURL
	if brokerURL == "" && len(a.cfg.ServerURLs) > 0 {
		brokerURL = a.cfg.ServerURLs[0]
	}

	err := a.transport.Connect(ctx, mqtt.ConnectOpts{
		BrokerURL:    brokerURL,
		ClientID:     a.cfg.ClientID,
		CleanSession: true,
		Username:     a.cfg.Username,
		Password:     a.cfg.Password,

		OnConnectionLost: a.onConnectionLost,
	})
	if err != nil {
		a.mu.Lock()
		a.setState(Disconnected)
		a.mu.Unlock()
		return fmt.Errorf("sparkplugb: application connect: %w", err)
	}

	if err := a.transport.Subscribe(ctx, topic.Namespace+"/#", 0, a.handleMessage); err != nil {
		return fmt.Errorf("sparkplugb: subscribe spBv1.0/#: %w", err)
	}

	a.mu.Lock()
	a.setState(Connected)
	a.mu.Unlock()

	return nil
}

func (a *Application) Disconnect(ctx context.Context) error {
	a.mu.Lock()
	if a.state == Closed || a.state == Disconnected {
		a.mu.Unlock()
		return nil
	}
	a.setState(Disconnected)
	a.mu.Unlock()

	return a.transport.Disconnect(ctx)
}

func (a *Application) Close(ctx context.Context) error {
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

func (a *Application) SendNodeCommand(ctx context.Context, groupID, nodeID string, metrics []*pb.Payload_Metric) error {
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

func (a *Application) SendDeviceCommand(ctx context.Context, groupID, nodeID, deviceID string, metrics []*pb.Payload_Metric) error {
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

func (a *Application) RequestRebirth(ctx context.Context, groupID, nodeID string) error {
	return a.SendNodeCommand(ctx, groupID, nodeID, []*pb.Payload_Metric{
		payload.RebirthMetric(true),
	})
}

// GetNode returns a snapshot of what is known about an edge node, or nil.
// The snapshot is a copy: it does not change as messages arrive, and reading
// it needs no locking.
func (a *Application) GetNode(groupID, nodeID string) *EdgeNodeState {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.nodes[nodeKey(groupID, nodeID)].clone()
}

// Nodes returns a snapshot of every edge node known, as GetNode does.
func (a *Application) Nodes() []*EdgeNodeState {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]*EdgeNodeState, 0, len(a.nodes))
	for _, n := range a.nodes {
		out = append(out, n.clone())
	}
	return out
}

func (a *Application) State() State {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.state
}

func (a *Application) ResolveAlias(groupID, nodeID string, alias uint64) (string, bool) {
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

func (a *Application) onConnectionLost(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state == Closed || a.state == Disconnected {
		return
	}
	a.cfg.Logger.Warnf("transport lost: %v", err)
	a.setState(Disconnected)
}

func (a *Application) setState(s State) {
	old := a.state
	a.state = s
	if old != s {
		a.cfg.Logger.Infof("application state: %s -> %s", old, s)
		a.cfg.Metrics.OnStateChange(old, s)
		if a.cfg.OnState != nil {
			a.cfg.OnState(old, s)
		}
	}
}

func (a *Application) handleMessage(topicStr string, data []byte) {
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

func (a *Application) dispatch(t topic.Topic, data []byte) {
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
	at          time.Time
	nextSession bool
}

// hold keeps a message that cannot be applied yet: one from an edge node
// whose NBIRTH has not arrived, or one ahead of the next sequence number.
// MQTT orders messages per topic only, so a broker may deliver the messages
// of one edge node out of publishing order (NanoMQ does); they are applied in
// sequence order once the gap is filled (see drain). A rebirth is requested
// as for a message that is lost. A message behind the expected sequence
// number is held for the next session: a restarted node starts again at 0,
// and its DBIRTH may arrive before its NBIRTH. It is applied after that
// NBIRTH, and requests no rebirth: when the NBIRTH does not come, it was a
// late message of the current session.
func (a *Application) hold(t topic.Topic, data []byte) bool {
	p, err := payload.Decode(data)
	if err != nil {
		return false // the handler reports it
	}
	seq := p.GetSeq()
	key := nodeKey(t.GroupID, t.EdgeNodeID)
	a.mu.Lock()
	defer a.mu.Unlock()
	ns := a.nodes[key]
	behind := false
	if ns != nil {
		expected := (ns.LastSeq + 1) % 256
		if seq == expected {
			return false
		}
		behind = (seq-expected+256)%256 > 128
	}
	if a.held == nil {
		a.held = make(map[string][]heldMsg)
	}
	if len(a.held[key]) >= maxHeld {
		return false
	}
	a.held[key] = append(a.held[key], heldMsg{t: t, seq: seq, data: data, at: time.Now(), nextSession: behind})
	switch {
	case behind:
	case ns == nil:
		a.unknownNode(t)
	default:
		a.validateSeq(ns, seq, t) // starts the reorder timer or requests a rebirth
	}
	return true
}

// heldMaxAge is how long a held message may wait for the gap before it.
const heldMaxAge = 30 * time.Second

// drain applies the held messages of the edge node that are next in
// sequence, one after the other, and drops those that waited too long.
func (a *Application) drain(t topic.Topic) {
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
			switch {
			case !found && !held[i].nextSession && held[i].seq == expected:
				next, found = &held[i], true
			case time.Since(held[i].at) > heldMaxAge:
			default:
				kept = append(kept, held[i])
			}
		}
		a.held[key] = kept
		a.mu.Unlock()
		if !found {
			return
		}
		m := *next
		a.dispatch(m.t, m.data)
	}
}

func (a *Application) handleNBIRTH(t topic.Topic, data []byte) {
	p, err := payload.Decode(data)
	if err != nil {
		a.logWarn("decode NBIRTH from %s/%s: %v", t.GroupID, t.EdgeNodeID, err)
		return
	}

	a.mu.Lock()
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
	a.answeredRebirth(t)
	for i := range a.held[key] {
		a.held[key][i].nextSession = false // this NBIRTH starts their session
	}
	a.mu.Unlock()

	a.cfg.Metrics.OnNodeBirth(t.GroupID, t.EdgeNodeID)
	if a.cfg.OnNodeBirth != nil {
		a.cfg.OnNodeBirth(t.GroupID, t.EdgeNodeID, p.Metrics)
	}
}

func (a *Application) handleNDEATH(t topic.Topic, data []byte) {
	p, err := payload.Decode(data)
	if err != nil {
		a.logWarn("decode NDEATH from %s/%s: %v", t.GroupID, t.EdgeNodeID, err)
		return
	}

	a.mu.Lock()
	key := nodeKey(t.GroupID, t.EdgeNodeID)
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
		ns.Online = false
		ns.Stale = true
		for _, dev := range ns.Devices {
			dev.Online = false
			dev.Stale = true
		}
	}
	a.mu.Unlock()

	a.cfg.Metrics.OnNodeDeath(t.GroupID, t.EdgeNodeID)
	if a.cfg.OnNodeDeath != nil {
		a.cfg.OnNodeDeath(t.GroupID, t.EdgeNodeID)
	}
}

func (a *Application) handleDBIRTH(t topic.Topic, data []byte) {
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
			Online:   true,
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

func (a *Application) handleDDEATH(t topic.Topic, data []byte) {
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

func (a *Application) handleNDATA(t topic.Topic, data []byte) {
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
			name := resolveMetricName(ns, m)
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

func (a *Application) handleDDATA(t topic.Topic, data []byte) {
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
				name := resolveDeviceMetricName(ds, m)
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

func (a *Application) validateSeq(ns *EdgeNodeState, seq uint64, t topic.Topic) bool {
	expected := (ns.LastSeq + 1) % 256
	if seq != expected {
		a.askRebirth(t, fmt.Sprintf("seq gap: got %d, expected %d", seq, expected))
		return false
	}
	ns.LastSeq = seq
	return true
}

func resolveMetricName(ns *EdgeNodeState, m *pb.Payload_Metric) string {
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

func resolveDeviceMetricName(ds *DeviceState, m *pb.Payload_Metric) string {
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

func (a *Application) logWarn(format string, args ...any) {
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

// rebirthInterval is how long a rebirth request the application makes on its own
// (a seq gap, a node whose NBIRTH has not been seen) waits for its NBIRTH: until it arrives no other request goes to
// that edge node (see answeredRebirth). A broker that reorders messages
// across topics (HiveMQ CE, NanoMQ) makes several messages in a row look out
// of sequence; one request each would make the node publish one NBIRTH
// each, a rebirth storm.
const rebirthInterval = 2 * time.Second

// unknownNode requests a rebirth from an edge node that sent a message
// before any NBIRTH was seen: a host joining while the node is online, or an
// NBIRTH that a broker delivered after the messages that followed it (MQTT
// orders messages per topic only). Called with a.mu held.
func (a *Application) unknownNode(t topic.Topic) {
	a.askRebirth(t, fmt.Sprintf("%s before its NBIRTH", t.Type))
}

// askRebirth requests a rebirth from the edge node of t, at most once per
// rebirthInterval. Called with a.mu held.
func (a *Application) askRebirth(t topic.Topic, reason string) {
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
func (a *Application) answeredRebirth(t topic.Topic) {
	key := nodeKey(t.GroupID, t.EdgeNodeID)
	delete(a.rebirthAsked, key)
	if a.rebirthMissed[key] {
		delete(a.rebirthMissed, key)
		a.askRebirth(t, "out of sequence before this NBIRTH")
	}
}
