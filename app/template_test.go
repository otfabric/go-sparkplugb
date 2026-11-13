// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
)

// warnLogger records warnings.
type warnLogger struct {
	mu    sync.Mutex
	warns []string
}

func (l *warnLogger) Debugf(string, ...any) {}
func (l *warnLogger) Infof(string, ...any)  {}
func (l *warnLogger) Errorf(string, ...any) {}
func (l *warnLogger) Warnf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, fmt.Sprintf(format, args...))
}

func (l *warnLogger) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	w := l.warns
	l.warns = nil
	return w
}

func encodeMsg(t *testing.T, seq uint64, metrics ...*pb.Payload_Metric) []byte {
	t.Helper()
	return encodeAt(t, 0, seq, metrics...)
}

// encodeAt encodes a payload stamped later by shift milliseconds.
func encodeAt(t *testing.T, shift, seq uint64, metrics ...*pb.Payload_Metric) []byte {
	t.Helper()
	p := payload.NewPayload(seq, metrics...)
	ts := p.GetTimestamp() + shift
	p.Timestamp = &ts
	b, err := payload.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// An instance whose definition is not in the NBIRTH, or a definition outside
// it, is reported as a warning, and the message is still delivered.
func TestTemplateInstanceUnknownDefinition(t *testing.T) {
	tr := newMockTransport()
	log := &warnLogger{}
	var births, data, dbirths, ddata int
	cfg := Config{BrokerURL: "tcp://localhost:1883"}
	cfg.Logger = log
	cfg.OnNodeBirth = func(_, _ string, _ []*pb.Payload_Metric) { births++ }
	cfg.OnNodeData = func(_, _ string, _ []*pb.Payload_Metric) { data++ }
	cfg.OnDeviceBirth = func(_, _, _ string, _ []*pb.Payload_Metric) { dbirths++ }
	cfg.OnDeviceData = func(_, _, _ string, _ []*pb.Payload_Metric) { ddata++ }
	a, err := New(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := a.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer a.Close(ctx)

	rpm, _ := payload.NewMetric("rpm", pb.DataType_Double, 0.0)
	def, _ := payload.NewTemplateDefinition("Motor", "1.0", []*pb.Payload_Metric{rpm}, nil)
	good, _ := payload.NewTemplateInstance("M1", "Motor", []*pb.Payload_Metric{rpm}, nil)
	bad, _ := payload.NewTemplateInstance("P1", "Pump", []*pb.Payload_Metric{rpm}, nil)

	const node = "spBv1.0/g1/%s/n1"
	tr.deliver(fmt.Sprintf(node, "NBIRTH"), encodeMsg(t, 0, payload.BdSeqMetric(0), def, good))
	if w := log.take(); len(w) != 0 {
		t.Fatalf("valid NBIRTH warned: %v", w)
	}

	steps := []struct {
		topic   string
		metrics []*pb.Payload_Metric
		want    string
	}{
		{fmt.Sprintf(node, "NDATA"), []*pb.Payload_Metric{good}, ""},
		{fmt.Sprintf(node, "NDATA"), []*pb.Payload_Metric{bad}, "unknown template definition"},
		{fmt.Sprintf(node, "DBIRTH") + "/d1", []*pb.Payload_Metric{bad}, "unknown template definition"},
		{fmt.Sprintf(node, "DDATA") + "/d1", []*pb.Payload_Metric{bad}, "unknown template definition"},
		{fmt.Sprintf(node, "DDATA") + "/d1", []*pb.Payload_Metric{def}, "outside the NBIRTH"},
	}
	for i, s := range steps {
		tr.deliver(s.topic, encodeMsg(t, uint64(i+1), s.metrics...))
		w := log.take()
		if s.want == "" {
			if len(w) != 0 {
				t.Errorf("%s: warned %v", s.topic, w)
			}
			continue
		}
		if len(w) != 1 || !strings.Contains(w[0], s.want) {
			t.Errorf("%s: warnings %v, want one with %q", s.topic, w, s.want)
		}
	}
	if births != 1 || data != 2 || dbirths != 1 || ddata != 2 {
		t.Fatalf("delivered NBIRTH %d, NDATA %d, DBIRTH %d, DDATA %d", births, data, dbirths, ddata)
	}

	// An NBIRTH with an instance of no definition is reported and applied.
	// It is stamped later, so that the messages before it are not taken
	// for its session's.
	tr.deliver(fmt.Sprintf(node, "NBIRTH"), encodeAt(t, 5000, 0, payload.BdSeqMetric(1), bad))
	if w := log.take(); len(w) != 1 || !strings.Contains(w[0], "unknown template definition") {
		t.Fatalf("NBIRTH warnings %v", w)
	}
	if births != 2 {
		t.Fatal("NBIRTH with an invalid template not delivered")
	}
	// Its definitions replace those of the first NBIRTH.
	tr.deliver(fmt.Sprintf(node, "NDATA"), encodeAt(t, 5001, 1, good))
	if w := log.take(); len(w) != 1 || !strings.Contains(w[0], "unknown template definition") {
		t.Fatalf("instance of a definition of the previous NBIRTH: warnings %v", w)
	}
}
