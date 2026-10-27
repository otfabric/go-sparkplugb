// SPDX-License-Identifier: MIT

package payload

import (
	"testing"

	pb "github.com/otfabric/go-sparkplugb/proto"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	m, err := NewMetric("temperature", pb.DataType_Double, float64(23.5))
	if err != nil {
		t.Fatal(err)
	}
	SetQuality(m, QualityGood)

	p := NewPayload(0, m)
	b, err := Encode(p)
	if err != nil {
		t.Fatal(err)
	}

	p2, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}

	if len(p2.Metrics) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(p2.Metrics))
	}
	if p2.GetSeq() != 0 {
		t.Errorf("seq = %d, want 0", p2.GetSeq())
	}

	val, err := GetValue(p2.Metrics[0])
	if err != nil {
		t.Fatal(err)
	}
	if val.(float64) != 23.5 {
		t.Errorf("value = %v, want 23.5", val)
	}

	q, ok := GetQuality(p2.Metrics[0])
	if !ok {
		t.Fatal("quality not found")
	}
	if q != QualityGood {
		t.Errorf("quality = %d, want %d", q, QualityGood)
	}
}

func TestBdSeqMetric(t *testing.T) {
	m := BdSeqMetric(42)
	if m.GetName() != "bdSeq" {
		t.Errorf("name = %q", m.GetName())
	}
	if pb.DataType(m.GetDatatype()) != pb.DataType_Int64 {
		t.Errorf("datatype = %d", m.GetDatatype())
	}
	if m.GetLongValue() != 42 {
		t.Errorf("value = %d, want 42", m.GetLongValue())
	}
}

func TestRebirthMetric(t *testing.T) {
	m := RebirthMetric(true)
	if m.GetName() != "Node Control/Rebirth" {
		t.Errorf("name = %q", m.GetName())
	}
	if !m.GetBooleanValue() {
		t.Error("expected true")
	}
}

func TestNewMetricWithAlias(t *testing.T) {
	m, err := NewMetricWithAlias("temp", 1, pb.DataType_Float, float32(22.0))
	if err != nil {
		t.Fatal(err)
	}
	if m.GetName() != "temp" {
		t.Errorf("name = %q", m.GetName())
	}
	if m.GetAlias() != 1 {
		t.Errorf("alias = %d", m.GetAlias())
	}
}

func TestNewAliasMetric(t *testing.T) {
	m, err := NewAliasMetric(5, pb.DataType_Boolean, true)
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != nil {
		t.Errorf("name should be nil, got %q", m.GetName())
	}
	if m.GetAlias() != 5 {
		t.Errorf("alias = %d", m.GetAlias())
	}
}

func TestSetQualityOverwrite(t *testing.T) {
	m, err := NewMetric("x", pb.DataType_Int32, int32(0))
	if err != nil {
		t.Fatal(err)
	}
	SetQuality(m, QualityBad)
	SetQuality(m, QualityGood)
	q, ok := GetQuality(m)
	if !ok || q != QualityGood {
		t.Errorf("quality = %d, ok=%v", q, ok)
	}
	if len(m.Properties.Keys) != 1 {
		t.Errorf("expected 1 property key, got %d", len(m.Properties.Keys))
	}
}

func TestStampMetrics(t *testing.T) {
	// Metric without timestamp
	name := "temperature"
	m1 := &pb.Payload_Metric{Name: &name}
	// Metric with existing timestamp (should not be overwritten)
	ts := uint64(1000)
	m2 := &pb.Payload_Metric{Name: &name, Timestamp: &ts}

	StampMetrics([]*pb.Payload_Metric{m1, m2, nil})

	if m1.Timestamp == nil {
		t.Error("StampMetrics should set timestamp on metric without one")
	}
	if *m2.Timestamp != 1000 {
		t.Errorf("StampMetrics should not overwrite existing timestamp, got %d", *m2.Timestamp)
	}
}

func TestStampMetricsEmpty(t *testing.T) {
	StampMetrics(nil)
	StampMetrics([]*pb.Payload_Metric{})
}
