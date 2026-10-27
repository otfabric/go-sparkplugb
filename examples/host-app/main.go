// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/otfabric/go-sparkplugb/host"
	"github.com/otfabric/go-sparkplugb/mqtt"
	pb "github.com/otfabric/go-sparkplugb/proto"
)

func main() {
	brokerURL := envOr("MQTT_BROKER", "tcp://localhost:1883")

	transport := mqtt.NewPahoTransport()

	app, err := host.New(transport, host.Config{
		HostID:    "scada1",
		BrokerURL: brokerURL,
		OnState: func(old, new host.State) {
			fmt.Printf("Host state: %s -> %s\n", old, new)
		},
		OnNodeBirth: func(g, n string, metrics []*pb.Payload_Metric) {
			fmt.Printf("Node BIRTH: %s/%s (%d metrics)\n", g, n, len(metrics))
		},
		OnDeviceBirth: func(g, n, d string, metrics []*pb.Payload_Metric) {
			fmt.Printf("Device BIRTH: %s/%s/%s (%d metrics)\n", g, n, d, len(metrics))
		},
		OnNodeData: func(g, n string, metrics []*pb.Payload_Metric) {
			fmt.Printf("Node DATA: %s/%s (%d metrics)\n", g, n, len(metrics))
		},
		OnDeviceData: func(g, n, d string, metrics []*pb.Payload_Metric) {
			fmt.Printf("Device DATA: %s/%s/%s (%d metrics)\n", g, n, d, len(metrics))
		},
		OnNodeDeath: func(g, n string) {
			fmt.Printf("Node DEATH: %s/%s\n", g, n)
		},
		OnDeviceDeath: func(g, n, d string) {
			fmt.Printf("Device DEATH: %s/%s/%s\n", g, n, d)
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := app.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer app.Close(context.Background())

	fmt.Println("Host Application online — listening for Sparkplug traffic...")
	<-ctx.Done()
	fmt.Println("Shutting down...")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
