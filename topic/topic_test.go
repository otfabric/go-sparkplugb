// SPDX-License-Identifier: MIT

package topic

import (
	"errors"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Topic
		wantErr error
	}{
		{
			name:  "NBIRTH",
			input: "spBv1.0/myGroup/NBIRTH/myNode",
			want:  Topic{GroupID: "myGroup", Type: NBIRTH, EdgeNodeID: "myNode"},
		},
		{
			name:  "NDEATH",
			input: "spBv1.0/myGroup/NDEATH/myNode",
			want:  Topic{GroupID: "myGroup", Type: NDEATH, EdgeNodeID: "myNode"},
		},
		{
			name:  "DBIRTH with device",
			input: "spBv1.0/myGroup/DBIRTH/myNode/myDevice",
			want:  Topic{GroupID: "myGroup", Type: DBIRTH, EdgeNodeID: "myNode", DeviceID: "myDevice"},
		},
		{
			name:  "DDEATH with device",
			input: "spBv1.0/myGroup/DDEATH/myNode/dev1",
			want:  Topic{GroupID: "myGroup", Type: DDEATH, EdgeNodeID: "myNode", DeviceID: "dev1"},
		},
		{
			name:  "NDATA",
			input: "spBv1.0/plant/NDATA/gateway",
			want:  Topic{GroupID: "plant", Type: NDATA, EdgeNodeID: "gateway"},
		},
		{
			name:  "DDATA with device",
			input: "spBv1.0/plant/DDATA/gateway/sensor1",
			want:  Topic{GroupID: "plant", Type: DDATA, EdgeNodeID: "gateway", DeviceID: "sensor1"},
		},
		{
			name:  "NCMD",
			input: "spBv1.0/plant/NCMD/gateway",
			want:  Topic{GroupID: "plant", Type: NCMD, EdgeNodeID: "gateway"},
		},
		{
			name:  "DCMD with device",
			input: "spBv1.0/plant/DCMD/gateway/actuator",
			want:  Topic{GroupID: "plant", Type: DCMD, EdgeNodeID: "gateway", DeviceID: "actuator"},
		},
		{
			name:  "STATE",
			input: "spBv1.0/STATE/myHost",
			want:  Topic{Type: STATE, EdgeNodeID: "myHost"},
		},
		{
			name:    "wrong namespace",
			input:   "spAv1.0/myGroup/NBIRTH/myNode",
			wantErr: ErrInvalidNamespace,
		},
		{
			name:    "too few parts",
			input:   "spBv1.0/myGroup",
			wantErr: ErrInvalidTopic,
		},
		{
			name:    "unknown message type",
			input:   "spBv1.0/myGroup/XBIRTH/myNode",
			wantErr: ErrInvalidMessageType,
		},
		{
			name:    "device message without device ID",
			input:   "spBv1.0/myGroup/DBIRTH/myNode",
			wantErr: ErrInvalidTopic,
		},
		{
			name:    "node message with device ID",
			input:   "spBv1.0/myGroup/NBIRTH/myNode/extra",
			wantErr: ErrInvalidTopic,
		},
		{
			name:    "empty group ID",
			input:   "spBv1.0//NBIRTH/myNode",
			wantErr: ErrInvalidID,
		},
		{
			name:    "wildcard in node ID",
			input:   "spBv1.0/g/NBIRTH/node+1",
			wantErr: ErrInvalidID,
		},
		{
			name:    "STATE with extra parts",
			input:   "spBv1.0/STATE/host/extra",
			wantErr: ErrInvalidTopic,
		},
		{
			name:    "too many parts",
			input:   "spBv1.0/g/DBIRTH/n/d/extra",
			wantErr: ErrInvalidTopic,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.input)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("Parse(%q) error = %v, want %v", tt.input, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("Parse(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestTopicRoundTrip(t *testing.T) {
	topics := []string{
		"spBv1.0/myGroup/NBIRTH/myNode",
		"spBv1.0/plant/DBIRTH/gw/sensor1",
		"spBv1.0/STATE/hostApp",
		"spBv1.0/g/NDATA/n",
		"spBv1.0/g/DDATA/n/d",
		"spBv1.0/g/NCMD/n",
		"spBv1.0/g/DCMD/n/d",
	}
	for _, s := range topics {
		t.Run(s, func(t *testing.T) {
			parsed, err := Parse(s)
			if err != nil {
				t.Fatal(err)
			}
			got := parsed.String()
			if got != s {
				t.Errorf("round-trip: got %q, want %q", got, s)
			}
		})
	}
}

func TestMessageTypeIsDevice(t *testing.T) {
	device := []MessageType{DBIRTH, DDEATH, DDATA, DCMD}
	node := []MessageType{NBIRTH, NDEATH, NDATA, NCMD}

	for _, mt := range device {
		if !mt.IsDevice() {
			t.Errorf("%s.IsDevice() = false", mt)
		}
	}
	for _, mt := range node {
		if mt.IsDevice() {
			t.Errorf("%s.IsDevice() = true", mt)
		}
	}
}

func TestSubscriptionHelpers(t *testing.T) {
	if got := ForNCMD("g", "n"); got != "spBv1.0/g/NCMD/n" {
		t.Errorf("ForNCMD = %q", got)
	}
	if got := ForDCMD("g", "n", "d"); got != "spBv1.0/g/DCMD/n/d" {
		t.Errorf("ForDCMD = %q", got)
	}
	if got := ForState("h"); got != "spBv1.0/STATE/h" {
		t.Errorf("ForState = %q", got)
	}
	if got := ForEdge("g", "n"); got != "spBv1.0/g/+/n/#" {
		t.Errorf("ForEdge = %q", got)
	}
}
