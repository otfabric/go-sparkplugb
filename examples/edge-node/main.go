// SPDX-License-Identifier: MIT

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
	brokerURL := envOr("MQTT_BROKER", "tcp://localhost:1883")

	transport := mqtt.NewPahoTransport()

	node, err := edge.New(transport, edge.Config{
		GroupID:    "factory1",
		EdgeNodeID: "gateway1",
		BrokerURL:  brokerURL,
		OnNodeCmd: func(deviceID string, metrics []*pb.Payload_Metric) {
			fmt.Printf("NCMD received: %d metrics\n", len(metrics))
		},
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

	tempMetric, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(22.5))
	pressMetric, _ := payload.NewMetric("pressure", pb.DataType_Float, float32(101.3))
	if err := node.PublishBirth(ctx, []*pb.Payload_Metric{tempMetric, pressMetric}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("NBIRTH published")

	sensorMetric, _ := payload.NewMetric("rpm", pb.DataType_UInt32, uint32(1500))
	if err := node.PublishDeviceBirth(ctx, "motor1", []*pb.Payload_Metric{sensorMetric}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("DBIRTH published for motor1")

	ticker := time.NewTicker(5 * time.Second)
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
				fmt.Printf("Published temperature=%.1f (seq=%d)\n", 22.5+float64(i)*0.1, node.Seq())
			}
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
