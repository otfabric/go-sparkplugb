// SPDX-License-Identifier: MIT

// Example: Edge Node with metric aliases for bandwidth-efficient DATA messages.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/otfabric/go-sparkplugb/edge"
	"github.com/otfabric/go-sparkplugb/mqtt"
	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
)

func main() {
	broker := os.Getenv("MQTT_BROKER")
	if broker == "" {
		broker = "tcp://localhost:1883"
	}

	transport := mqtt.NewPahoTransport()

	node, err := edge.New(transport, edge.Config{
		GroupID:    "factory1",
		EdgeNodeID: "plc-01",
		BrokerURL:  broker,
		OnState: func(old, new edge.State) {
			fmt.Printf("State: %s -> %s\n", old, new)
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := node.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer node.Close(context.Background())

	// NBIRTH includes full metric names + alias mappings.
	// Set Alias on each metric so the edge node registers them.
	// Subsequent NDATA messages can use aliases instead of names.
	temp, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(22.5))
	a1 := uint64(1)
	temp.Alias = &a1

	pressure, _ := payload.NewMetric("pressure", pb.DataType_Float, float32(1013.25))
	a2 := uint64(2)
	pressure.Alias = &a2

	humidity, _ := payload.NewMetric("humidity", pb.DataType_Float, float32(55.0))
	a3 := uint64(3)
	humidity.Alias = &a3

	status, _ := payload.NewMetric("status", pb.DataType_Boolean, true)
	a4 := uint64(4)
	status.Alias = &a4

	if err := node.PublishBirth(ctx, []*pb.Payload_Metric{temp, pressure, humidity, status}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("NBIRTH published with 4 metrics (aliases assigned)")

	// Resolve aliases back to names (e.g. on the host/application side)
	if name, ok := node.ResolveAlias(1); ok {
		fmt.Printf("Alias 1 -> %s\n", name)
	}
	if name, ok := node.ResolveAlias(2); ok {
		fmt.Printf("Alias 2 -> %s\n", name)
	}

	// Periodic DATA publishes — the host resolves aliases to metric names
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	i := 0
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Shutting down...")
			return
		case <-ticker.C:
			i++
			m, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(22.5)+float64(i)*0.1)
			if err := node.PublishNodeData(ctx, []*pb.Payload_Metric{m}); err != nil {
				log.Printf("publish error: %v", err)
			} else {
				fmt.Printf("Published temperature=%.1f (alias=%d)\n", 22.5+float64(i)*0.1, a1)
			}
		}
	}
}
