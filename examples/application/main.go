// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/otfabric/go-sparkplugb/app"
	"github.com/otfabric/go-sparkplugb/mqtt"
	pb "github.com/otfabric/go-sparkplugb/proto"
)

func main() {
	broker := os.Getenv("MQTT_BROKER")
	if broker == "" {
		broker = "tcp://localhost:1883"
	}

	transport := mqtt.NewPahoTransport()

	application, err := app.New(transport, app.Config{
		BrokerURL: broker,
		ClientID:  "sparkplug-app-example",
		OnState: func(old, new app.State) {
			fmt.Printf("State: %s -> %s\n", old, new)
		},
		OnNodeBirth: func(group, node string, metrics []*pb.Payload_Metric) {
			fmt.Printf("NBIRTH  %s/%s  metrics=%d\n", group, node, len(metrics))
			for _, m := range metrics {
				fmt.Printf("  %-20s %s\n", m.GetName(), metricValue(m))
			}
		},
		OnNodeDeath: func(group, node string) {
			fmt.Printf("NDEATH  %s/%s\n", group, node)
		},
		OnDeviceBirth: func(group, node, device string, metrics []*pb.Payload_Metric) {
			fmt.Printf("DBIRTH  %s/%s/%s  metrics=%d\n", group, node, device, len(metrics))
		},
		OnDeviceDeath: func(group, node, device string) {
			fmt.Printf("DDEATH  %s/%s/%s\n", group, node, device)
		},
		OnNodeData: func(group, node string, metrics []*pb.Payload_Metric) {
			fmt.Printf("NDATA   %s/%s  metrics=%d\n", group, node, len(metrics))
			for _, m := range metrics {
				fmt.Printf("  %-20s %s\n", m.GetName(), metricValue(m))
			}
		},
		OnDeviceData: func(group, node, device string, metrics []*pb.Payload_Metric) {
			fmt.Printf("DDATA   %s/%s/%s  metrics=%d\n", group, node, device, len(metrics))
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := application.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer application.Close(context.Background())

	fmt.Println("Application online — subscribing to spBv1.0/# (no STATE published)")
	fmt.Println("Press Ctrl+C to exit")
	<-ctx.Done()

	fmt.Printf("\nTracked %d node(s)\n", len(application.Nodes()))
}

func metricValue(m *pb.Payload_Metric) string {
	switch v := m.Value.(type) {
	case *pb.Payload_Metric_IntValue:
		return fmt.Sprintf("%d", v.IntValue)
	case *pb.Payload_Metric_LongValue:
		return fmt.Sprintf("%d", v.LongValue)
	case *pb.Payload_Metric_FloatValue:
		return fmt.Sprintf("%.2f", v.FloatValue)
	case *pb.Payload_Metric_DoubleValue:
		return fmt.Sprintf("%.2f", v.DoubleValue)
	case *pb.Payload_Metric_BooleanValue:
		return fmt.Sprintf("%t", v.BooleanValue)
	case *pb.Payload_Metric_StringValue:
		return v.StringValue
	default:
		return "<complex>"
	}
}
