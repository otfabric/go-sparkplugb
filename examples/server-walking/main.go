// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/otfabric/go-sparkplugb/edge"
	"github.com/otfabric/go-sparkplugb/mqtt"
	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
)

func main() {
	transport := mqtt.NewPahoTransport()

	node, err := edge.New(transport, edge.Config{
		GroupID:    "factory1",
		EdgeNodeID: "gateway1",
		PrimaryHostID: "scada1",
		ServerURLs: []string{
			"tcp://broker1:1883",
			"tcp://broker2:1883",
			"tcp://broker3:1883",
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
	if err := node.PublishBirth(ctx, []*pb.Payload_Metric{tempMetric}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Edge node online — walks to next broker when primary host goes offline")
	fmt.Println("Press Ctrl+C to exit")
	<-ctx.Done()
}
