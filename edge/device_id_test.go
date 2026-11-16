// SPDX-License-Identifier: MIT

package edge

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	pb "github.com/otfabric/go-sparkplugb/proto"
	"github.com/otfabric/go-sparkplugb/topic"
)

// Sparkplug 3.0.0 tck-id-topics-device-id: PublishDeviceBirth,
// PublishDeviceData and PublishDeviceDeath refuse a device ID that cannot be
// the device segment of a topic with topic.ErrInvalidID, as New does for
// the group and edge node IDs: one that is empty, not valid UTF-8, or
// contains '/', '+' or '#'. A refused call publishes nothing, takes no seq
// and does not add a device.
func TestDeviceMethodsRefuseInvalidIDs(t *testing.T) {
	for _, tc := range []struct {
		id string
		ok bool
	}{
		{"", false},
		{"d", true},
		{"d1", true},
		{"motor-01_A.b", true},
		{"d 1", true},
		{"Gerät", true},
		{"�", true}, // the replacement character is valid UTF-8
		{strings.Repeat("d", 1024), true},
		{"/", false},
		{"a/b", false},
		{"d/", false},
		{"+", false},
		{"d+", false},
		{"#", false},
		{"d#1", false},
		{"\xff", false},
		{"d\xc3", false},        // a two-byte sequence cut short
		{"\xc0\xaf", false},     // '/' in an overlong form
		{"\xed\xa0\x80", false}, // a UTF-16 surrogate
	} {
		t.Run(fmt.Sprintf("%.20q", tc.id), func(t *testing.T) {
			n, tr := connectedNode(t)
			ctx := context.Background()
			defer n.Close(ctx)
			if err := n.PublishBirth(ctx, []*pb.Payload_Metric{intMetric(t, "x", 1)}); err != nil {
				t.Fatal(err)
			}
			for _, op := range []struct {
				name string
				mt   topic.MessageType
				call func() error
			}{
				{"PublishDeviceBirth", topic.DBIRTH, func() error {
					return n.PublishDeviceBirth(ctx, tc.id, []*pb.Payload_Metric{intMetric(t, "y", 1)})
				}},
				{"PublishDeviceData", topic.DDATA, func() error {
					return n.PublishDeviceData(ctx, tc.id, []*pb.Payload_Metric{intMetric(t, "y", 2)})
				}},
				{"PublishDeviceDeath", topic.DDEATH, func() error { return n.PublishDeviceDeath(ctx, tc.id) }},
			} {
				before, seq := len(tr.getPublished()), n.Seq()
				err := op.call()
				published := tr.getPublished()[before:]
				if tc.ok {
					want := topic.Topic{GroupID: "testGroup", Type: op.mt, EdgeNodeID: "testNode", DeviceID: tc.id}.String()
					if err != nil || len(published) != 1 || published[0].Topic != want {
						t.Errorf("%s %q: %v, published %d messages, want nil and one on %q", op.name, tc.id, err, len(published), want)
					}
					continue
				}
				if !errors.Is(err, topic.ErrInvalidID) {
					t.Errorf("%s %q: %v, want topic.ErrInvalidID", op.name, tc.id, err)
				}
				if len(published) != 0 {
					t.Errorf("%s %q: published %d messages, want none", op.name, tc.id, len(published))
				}
				if s := n.Seq(); s != seq {
					t.Errorf("%s %q: seq %d, want %d: a refused message takes none", op.name, tc.id, s, seq)
				}
			}
			n.mu.Lock()
			_, added := n.devices[tc.id]
			n.mu.Unlock()
			if added != tc.ok {
				t.Errorf("device %q known to the node: %v, want %v", tc.id, added, tc.ok)
			}
		})
	}
}
