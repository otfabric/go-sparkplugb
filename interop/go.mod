module github.com/otfabric/go-sparkplugb/interop

go 1.24.0

require github.com/otfabric/go-sparkplugb v0.0.0

require (
	github.com/eclipse/paho.mqtt.golang v1.5.1 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	golang.org/x/net v0.44.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

// The library of this repository, as it is here.
replace github.com/otfabric/go-sparkplugb => ../
