// SPDX-License-Identifier: EPL-2.0

//go:generate protoc --proto_path=. --go_out=. --go_opt=Msparkplug_b.proto=github.com/otfabric/go-sparkplugb/proto --go_opt=paths=source_relative sparkplug_b.proto

package proto
