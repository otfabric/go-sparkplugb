// SPDX-License-Identifier: MIT

// Example: sending NCMD and DCMD commands from a Host Application.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/otfabric/go-sparkplugb/host"
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

	var hostApp *host.App

	h, err := host.New(transport, host.Config{
		HostID:    "scada-cmd-demo",
		BrokerURL: broker,
		OnNodeBirth: func(group, node string, metrics []*pb.Payload_Metric) {
			fmt.Printf("Node online: %s/%s (%d metrics)\n", group, node, len(metrics))

			go func() {
				time.Sleep(2 * time.Second)
				ctx := context.Background()
				rebirth := payload.RebirthMetric(true)
				if err := hostApp.SendNodeCommand(ctx, group, node, []*pb.Payload_Metric{rebirth}); err != nil {
					log.Printf("NCMD failed: %v", err)
				} else {
					fmt.Printf("Sent rebirth NCMD to %s/%s\n", group, node)
				}
			}()
		},
		OnDeviceBirth: func(group, node, device string, metrics []*pb.Payload_Metric) {
			fmt.Printf("Device online: %s/%s/%s (%d metrics)\n", group, node, device, len(metrics))

			go func() {
				time.Sleep(1 * time.Second)
				ctx := context.Background()
				m, _ := payload.NewMetric("setpoint", pb.DataType_Double, float64(75.0))
				if err := hostApp.SendDeviceCommand(ctx, group, node, device, []*pb.Payload_Metric{m}); err != nil {
					log.Printf("DCMD failed: %v", err)
				} else {
					fmt.Printf("Sent setpoint DCMD to %s/%s/%s\n", group, node, device)
				}
			}()
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	hostApp = h

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := hostApp.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer hostApp.Close(context.Background())

	fmt.Println("Host online — will send commands to any node/device that appears")
	fmt.Println("Press Ctrl+C to exit")
	<-ctx.Done()
}
