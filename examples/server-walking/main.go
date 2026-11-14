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

	// The node publishes no NBIRTH by itself. A session needs one each time
	// the node comes online: after Connect, once the primary host is online,
	// and after each walk to another broker. OnState is called without the
	// node's locks held, so it publishes the birth there.
	var node *edge.Node
	node, err := edge.New(transport, edge.Config{
		GroupID:       "factory1",
		EdgeNodeID:    "gateway1",
		PrimaryHostID: "scada1",
		ServerURLs: []string{
			"tcp://broker1:1883",
			"tcp://broker2:1883",
			"tcp://broker3:1883",
		},
		OnState: func(old, new edge.State) {
			fmt.Printf("State: %s -> %s\n", old, new)
			if new != edge.Online {
				return
			}
			tempMetric, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(22.5))
			if err := node.PublishBirth(context.Background(), []*pb.Payload_Metric{tempMetric}); err != nil {
				log.Printf("NBIRTH: %v", err)
			}
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

	fmt.Println("Edge node connected: it births when the primary host is online, and walks to the next broker when it goes offline")
	fmt.Println("Press Ctrl+C to exit")
	<-ctx.Done()
}
