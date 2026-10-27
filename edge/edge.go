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
	ErrDuplicateID  = errors.New("sparkplugb: duplicate device or alias ID")
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

type StateHandler func(old, new State)

type CommandHandler func(deviceID string, metrics []*pb.Payload_Metric)

type Config struct {
	GroupID    string
	EdgeNodeID string
	BrokerURL  string
	ClientID   string

	PrimaryHostID string
	ServerURLs    []string

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
	bdSeq   uint64
	seq     uint64
	devices map[string]*Device
	aliases map[uint64]string // alias -> metric name for uniqueness
	born    bool
	gen     uint64 // connection generation; prevents stale callbacks
	closeCh chan struct{}

	// Birth state for auto-rebirth
	birthMetrics []*pb.Payload_Metric

	// Server walking
	serverIdx int

	rebirthMu      sync.Mutex
	rebirthPending bool
}

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
	if cfg.BdSeqStore == nil {
		cfg.BdSeqStore = &memBdSeqStore{}
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NopMetrics{}
	}
	if cfg.RetryPolicy == nil {
		cfg.RetryPolicy = sparkplugb.ExponentialBackoff{}
	}
	cfg.Logger = sparkplugb.ComponentLogger(cfg.Logger, "edge")
	if cfg.ClientID == "" {
		cfg.ClientID = fmt.Sprintf("sparkplug_%s_%s", cfg.GroupID, cfg.EdgeNodeID)
	}

	return &Node{
		cfg:       cfg,
		transport: transport,
		devices:   make(map[string]*Device),
		aliases:   make(map[uint64]string),
		closeCh:   make(chan struct{}),
	}, nil
}

func (n *Node) Connect(ctx context.Context) error {
	n.mu.Lock()
	if n.state == Closed {
		n.mu.Unlock()
		return ErrClosed
	}
	n.setState(Connecting)

	bdSeq, err := n.cfg.BdSeqStore.Load()
	if err != nil {
		n.setState(Disconnected)
		n.mu.Unlock()
		return fmt.Errorf("sparkplugb: load bdSeq: %w", err)
	}
	n.bdSeq = (bdSeq + 1) % 256

	if err := n.cfg.BdSeqStore.Store(n.bdSeq); err != nil {
		n.setState(Disconnected)
		n.mu.Unlock()
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
		return fmt.Errorf("sparkplugb: encode NDEATH: %w", err)
	}

	n.gen++
	connGen := n.gen
	n.mu.Unlock()

	brokerURL := n.brokerURL()

	err = n.transport.Connect(ctx, mqtt.ConnectOpts{
		BrokerURL:    brokerURL,
		ClientID:     n.cfg.ClientID,
		CleanSession: true,
		WillTopic:    willTopic,
		WillPayload:  willBytes,
		WillQoS:      1,
		WillRetained: false,

		OnConnectionLost: n.makeConnLostHandler(connGen),
	})
	if err != nil {
		n.mu.Lock()
		n.setState(Disconnected)
		n.mu.Unlock()
		return fmt.Errorf("sparkplugb: connect: %w", err)
	}

	n.mu.Lock()
	n.setState(Subscribing)
	n.mu.Unlock()

	ncmdTopic := topic.ForNCMD(n.cfg.GroupID, n.cfg.EdgeNodeID)
	if err := n.transport.Subscribe(ctx, ncmdTopic, 1, n.makeHandler(connGen)); err != nil {
		return fmt.Errorf("sparkplugb: subscribe NCMD: %w", err)
	}

	dcmdTopic := topic.Topic{
		GroupID:    n.cfg.GroupID,
		Type:       topic.DCMD,
		EdgeNodeID: n.cfg.EdgeNodeID,
		DeviceID:   "+",
	}.String()
	if err := n.transport.Subscribe(ctx, dcmdTopic, 1, n.makeHandler(connGen)); err != nil {
		return fmt.Errorf("sparkplugb: subscribe DCMD: %w", err)
	}

	if n.cfg.PrimaryHostID != "" {
		// Waiting before subscribing: the retained STATE can arrive before
		// Subscribe returns, and it only counts while the node waits for it.
		n.mu.Lock()
		n.setState(WaitingForPrimaryHost)
		n.mu.Unlock()
		stateTopic := topic.ForState(n.cfg.PrimaryHostID)
		if err := n.transport.Subscribe(ctx, stateTopic, 1, n.makeStateHandler(connGen)); err != nil {
			return fmt.Errorf("sparkplugb: subscribe STATE: %w", err)
		}
	} else {
		n.mu.Lock()
		n.setState(Online)
		n.mu.Unlock()
	}

	return nil
}

func (n *Node) PublishBirth(ctx context.Context, nodeMetrics []*pb.Payload_Metric) error {
	n.mu.Lock()
	if n.state != Online {
		n.mu.Unlock()
		return ErrNotConnected
	}
	n.seq = 0
	n.born = true
	n.aliases = make(map[uint64]string)

	// Store birth metrics for auto-rebirth
	n.birthMetrics = make([]*pb.Payload_Metric, len(nodeMetrics))
	copy(n.birthMetrics, nodeMetrics)

	// Register aliases from birth metrics
	for _, m := range nodeMetrics {
		if m.Alias != nil {
			n.aliases[m.GetAlias()] = m.GetName()
		}
	}

	seq := n.seq
	bdSeq := n.bdSeq
	n.mu.Unlock()

	payload.StampMetrics(nodeMetrics)

	birthMetrics := make([]*pb.Payload_Metric, 0, len(nodeMetrics)+2)
	birthMetrics = append(birthMetrics, payload.BdSeqMetric(bdSeq))
	// Sparkplug 3.0.0 requires "Node Control/Rebirth" in every NBIRTH, so
	// that a host knows it can ask for one. Added unless the caller has it.
	if !hasMetric(nodeMetrics, rebirthMetricName) {
		birthMetrics = append(birthMetrics, payload.RebirthMetric(false))
	}
	birthMetrics = append(birthMetrics, nodeMetrics...)

	p := payload.NewPayload(seq, birthMetrics...)
	return n.publish(ctx, topic.NBIRTH, "", p)
}

func (n *Node) PublishDeviceBirth(ctx context.Context, deviceID string, metrics []*pb.Payload_Metric) error {
	n.mu.Lock()
	if !n.born {
		n.mu.Unlock()
		return ErrNoBirth
	}

	if _, exists := n.devices[deviceID]; !exists {
		n.devices[deviceID] = &Device{ID: deviceID}
	}
	n.devices[deviceID].Metrics = make([]*pb.Payload_Metric, len(metrics))
	copy(n.devices[deviceID].Metrics, metrics)
	n.devices[deviceID].Online = true

	// Register aliases from device birth metrics
	for _, m := range metrics {
		if m.Alias != nil {
			n.aliases[m.GetAlias()] = m.GetName()
		}
	}

	n.seq = (n.seq + 1) % 256
	seq := n.seq
	n.mu.Unlock()

	payload.StampMetrics(metrics)
	p := payload.NewPayload(seq, metrics...)
	return n.publish(ctx, topic.DBIRTH, deviceID, p)
}

func (n *Node) PublishNodeData(ctx context.Context, metrics []*pb.Payload_Metric) error {
	n.mu.Lock()
	if !n.born {
		n.mu.Unlock()
		return ErrNoBirth
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
	n.mu.Lock()
	if !n.born {
		n.mu.Unlock()
		return ErrNoBirth
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
// for each online device. It resets the sequence counter per the spec.
func (n *Node) Rebirth(ctx context.Context) error {
	n.mu.Lock()
	if n.state != Online {
		n.mu.Unlock()
		return ErrNotConnected
	}
	birthMetrics := n.birthMetrics
	devices := make(map[string]*Device, len(n.devices))
	for k, v := range n.devices {
		devices[k] = v
	}
	n.mu.Unlock()

	if err := n.PublishBirth(ctx, birthMetrics); err != nil {
		return fmt.Errorf("sparkplugb: rebirth NBIRTH: %w", err)
	}

	for id, dev := range devices {
		if dev.Online {
			if err := n.PublishDeviceBirth(ctx, id, dev.Metrics); err != nil {
				return fmt.Errorf("sparkplugb: rebirth DBIRTH %s: %w", id, err)
			}
		}
	}
	return nil
}

func (n *Node) Disconnect(ctx context.Context) error {
	n.mu.Lock()
	if n.state == Closed || n.state == Disconnected {
		n.mu.Unlock()
		return nil
	}

	if n.born {
		ndeath := n.buildNDEATH()
		ndeathBytes, err := payload.Encode(ndeath)
		if err == nil {
			t := topic.Topic{
				GroupID:    n.cfg.GroupID,
				Type:       topic.NDEATH,
				EdgeNodeID: n.cfg.EdgeNodeID,
			}.String()
			n.mu.Unlock()
			_ = n.transport.Publish(ctx, t, 1, false, ndeathBytes)
			n.mu.Lock()
		}
	}
	n.born = false
	n.birthMetrics = nil
	n.setState(Disconnected)
	n.mu.Unlock()

	return n.transport.Disconnect(ctx)
}

func (n *Node) Close(ctx context.Context) error {
	n.mu.Lock()
	if n.state == Closed {
		n.mu.Unlock()
		return nil
	}
	n.mu.Unlock()

	err := n.Disconnect(ctx)
	n.mu.Lock()
	n.setState(Closed)
	close(n.closeCh)
	n.mu.Unlock()
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
		defer n.mu.Unlock()
		if n.gen != connGen || n.state == Closed || n.state == Disconnected {
			return
		}
		n.cfg.Logger.Warnf("transport lost: %v", err)
		n.born = false
		n.setState(Disconnected)
	}
}

func (n *Node) setState(s State) {
	old := n.state
	n.state = s
	if old != s {
		n.cfg.Logger.Infof("state: %s -> %s", old, s)
		n.cfg.Metrics.OnStateChange(old, s)
		if n.cfg.OnState != nil {
			n.cfg.OnState(old, s)
		}
	}
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

	n.cfg.Metrics.OnMessage(t.Type.String())

	switch t.Type {
	case topic.NCMD:
		n.handleNCMD(p)
	case topic.DCMD:
		if n.cfg.OnDeviceCmd != nil {
			n.cfg.OnDeviceCmd(t.DeviceID, p.Metrics)
		}
	}
}

const rebirthMetricName = "Node Control/Rebirth"

// withCurrent returns the birth metrics with the values of the data metrics
// that name them, by name or by alias, so that a rebirth publishes current
// values as Sparkplug requires. The birth keeps its names, aliases and
// datatypes; neither slice is modified. Called with n.mu held.
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
			out[i] = c
		}
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
		n.cfg.OnNodeCmd("", p.Metrics)
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
	if sp.Online && n.state == WaitingForPrimaryHost {
		n.setState(Online)
	} else if !sp.Online && n.state == Online && n.cfg.PrimaryHostID != "" {
		n.born = false
		n.birthMetrics = nil
		n.setState(Disconnected)
		walk = len(n.cfg.ServerURLs) > 1
	}
	n.mu.Unlock()

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
	_ = n.transport.Disconnect(context.Background())

	for attempt := 0; ; attempt++ {
		n.mu.Lock()
		n.serverIdx = (n.serverIdx + 1) % len(n.cfg.ServerURLs)
		url := n.brokerURL()
		n.mu.Unlock()

		n.cfg.Logger.Infof("walking to server %s (attempt %d)", url, attempt)

		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := n.Connect(ctx)
		cancel()
		n.cfg.Metrics.OnConnect(url, time.Since(start), err)
		if err == nil {
			return
		}
		n.cfg.Logger.Warnf("walk connect failed: %v", err)

		retry, wait := n.cfg.RetryPolicy.ShouldRetry(attempt, err)
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
