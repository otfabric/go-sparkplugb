// SPDX-License-Identifier: MIT

package payload

import (
	"time"

	pb "github.com/otfabric/go-sparkplugb/proto"
	"google.golang.org/protobuf/proto"
)

const (
	QualityBad   int32 = 0
	QualityGood  int32 = 192
	QualityStale int32 = 500
)

func Encode(p *pb.Payload) ([]byte, error) {
	return proto.Marshal(p)
}

func Decode(b []byte) (*pb.Payload, error) {
	p := &pb.Payload{}
	if err := proto.Unmarshal(b, p); err != nil {
		return nil, err
	}
	return p, nil
}

func NewPayload(seq uint64, metrics ...*pb.Payload_Metric) *pb.Payload {
	ts := uint64(time.Now().UnixMilli())
	return &pb.Payload{
		Timestamp: &ts,
		Seq:       &seq,
		Metrics:   metrics,
	}
}

func NewMetric(name string, dt pb.DataType, v any) (*pb.Payload_Metric, error) {
	m := &pb.Payload_Metric{Name: &name}
	ts := uint64(time.Now().UnixMilli())
	m.Timestamp = &ts
	if err := SetValue(m, dt, v); err != nil {
		return nil, err
	}
	return m, nil
}

func NewMetricWithAlias(name string, alias uint64, dt pb.DataType, v any) (*pb.Payload_Metric, error) {
	m, err := NewMetric(name, dt, v)
	if err != nil {
		return nil, err
	}
	m.Alias = &alias
	return m, nil
}

func NewAliasMetric(alias uint64, dt pb.DataType, v any) (*pb.Payload_Metric, error) {
	m := &pb.Payload_Metric{Alias: &alias}
	ts := uint64(time.Now().UnixMilli())
	m.Timestamp = &ts
	if err := SetValue(m, dt, v); err != nil {
		return nil, err
	}
	return m, nil
}

func SetQuality(m *pb.Payload_Metric, quality int32) {
	qKey := "quality"
	qType := uint32(pb.DataType_Int32)
	qVal := uint32(quality)

	prop := &pb.Payload_PropertyValue{
		Type:  &qType,
		Value: &pb.Payload_PropertyValue_IntValue{IntValue: qVal},
	}
	if m.Properties == nil {
		m.Properties = &pb.Payload_PropertySet{
			Keys:   []string{qKey},
			Values: []*pb.Payload_PropertyValue{prop},
		}
		return
	}
	for i, k := range m.Properties.Keys {
		if k == qKey {
			m.Properties.Values[i] = prop
			return
		}
	}
	m.Properties.Keys = append(m.Properties.Keys, qKey)
	m.Properties.Values = append(m.Properties.Values, prop)
}

func GetQuality(m *pb.Payload_Metric) (int32, bool) {
	if m.Properties == nil {
		return 0, false
	}
	for i, k := range m.Properties.Keys {
		if k == "quality" {
			if i < len(m.Properties.Values) {
				return int32(m.Properties.Values[i].GetIntValue()), true
			}
		}
	}
	return 0, false
}

func BdSeqMetric(bdSeq uint64) *pb.Payload_Metric {
	name := "bdSeq"
	dt := uint32(pb.DataType_Int64)
	m := &pb.Payload_Metric{
		Name:     &name,
		Datatype: &dt,
		Value:    &pb.Payload_Metric_LongValue{LongValue: bdSeq},
	}
	return m
}

func RebirthMetric(value bool) *pb.Payload_Metric {
	name := "Node Control/Rebirth"
	dt := uint32(pb.DataType_Boolean)
	m := &pb.Payload_Metric{
		Name:     &name,
		Datatype: &dt,
		Value:    &pb.Payload_Metric_BooleanValue{BooleanValue: value},
	}
	return m
}
