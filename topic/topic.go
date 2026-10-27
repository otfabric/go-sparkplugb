// SPDX-License-Identifier: MIT

package topic

import (
	"errors"
	"strings"
)

const Namespace = "spBv1.0"

type MessageType uint8

const (
	NBIRTH MessageType = iota
	NDEATH
	DBIRTH
	DDEATH
	NDATA
	DDATA
	NCMD
	DCMD
	STATE
)

var messageTypeStrings = [...]string{
	"NBIRTH", "NDEATH",
	"DBIRTH", "DDEATH",
	"NDATA", "DDATA",
	"NCMD", "DCMD",
	"STATE",
}

func (m MessageType) String() string {
	if int(m) < len(messageTypeStrings) {
		return messageTypeStrings[m]
	}
	return "UNKNOWN"
}

func (m MessageType) IsDevice() bool {
	return m == DBIRTH || m == DDEATH || m == DDATA || m == DCMD
}

func (m MessageType) IsNode() bool {
	return m == NBIRTH || m == NDEATH || m == NDATA || m == NCMD
}

var messageTypeLookup = map[string]MessageType{
	"NBIRTH": NBIRTH, "NDEATH": NDEATH,
	"DBIRTH": DBIRTH, "DDEATH": DDEATH,
	"NDATA": NDATA, "DDATA": DDATA,
	"NCMD": NCMD, "DCMD": DCMD,
	"STATE": STATE,
}

func ParseMessageType(s string) (MessageType, error) {
	if mt, ok := messageTypeLookup[s]; ok {
		return mt, nil
	}
	return 0, ErrInvalidMessageType
}

var (
	ErrInvalidTopic       = errors.New("sparkplugb: invalid topic")
	ErrInvalidNamespace   = errors.New("sparkplugb: invalid namespace")
	ErrInvalidMessageType = errors.New("sparkplugb: invalid message type")
	ErrInvalidID          = errors.New("sparkplugb: invalid ID")
)

type Topic struct {
	GroupID    string
	Type       MessageType
	EdgeNodeID string
	DeviceID   string
}

func (t Topic) String() string {
	if t.Type == STATE {
		return Namespace + "/STATE/" + t.EdgeNodeID
	}
	s := Namespace + "/" + t.GroupID + "/" + t.Type.String() + "/" + t.EdgeNodeID
	if t.DeviceID != "" {
		s += "/" + t.DeviceID
	}
	return s
}

func Parse(topic string) (Topic, error) {
	parts := strings.Split(topic, "/")
	if len(parts) < 3 {
		return Topic{}, ErrInvalidTopic
	}
	if parts[0] != Namespace {
		return Topic{}, ErrInvalidNamespace
	}

	// STATE topic: spBv1.0/STATE/{host_id}
	if parts[1] == "STATE" {
		if len(parts) != 3 {
			return Topic{}, ErrInvalidTopic
		}
		if err := validateID(parts[2]); err != nil {
			return Topic{}, err
		}
		return Topic{
			Type:       STATE,
			EdgeNodeID: parts[2],
		}, nil
	}

	// Regular topic: spBv1.0/{group}/{type}/{node}[/{device}]
	if len(parts) < 4 || len(parts) > 5 {
		return Topic{}, ErrInvalidTopic
	}

	groupID := parts[1]
	if err := validateID(groupID); err != nil {
		return Topic{}, err
	}

	mt, err := ParseMessageType(parts[2])
	if err != nil {
		return Topic{}, err
	}

	nodeID := parts[3]
	if err := validateID(nodeID); err != nil {
		return Topic{}, err
	}

	var deviceID string
	if len(parts) == 5 {
		deviceID = parts[4]
		if err := validateID(deviceID); err != nil {
			return Topic{}, err
		}
		if !mt.IsDevice() {
			return Topic{}, ErrInvalidTopic
		}
	} else if mt.IsDevice() {
		return Topic{}, ErrInvalidTopic
	}

	return Topic{
		GroupID:    groupID,
		Type:       mt,
		EdgeNodeID: nodeID,
		DeviceID:   deviceID,
	}, nil
}

func validateID(id string) error {
	if id == "" {
		return ErrInvalidID
	}
	for _, c := range id {
		if c == '/' || c == '+' || c == '#' {
			return ErrInvalidID
		}
	}
	return nil
}

func ForEdge(groupID, nodeID string) string {
	return Namespace + "/" + groupID + "/+/" + nodeID + "/#"
}

func ForState(hostID string) string {
	return Namespace + "/STATE/" + hostID
}

func ForNCMD(groupID, nodeID string) string {
	return Namespace + "/" + groupID + "/NCMD/" + nodeID
}

func ForDCMD(groupID, nodeID, deviceID string) string {
	return Namespace + "/" + groupID + "/DCMD/" + nodeID + "/" + deviceID
}
