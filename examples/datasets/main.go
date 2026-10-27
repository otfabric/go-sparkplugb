// SPDX-License-Identifier: MIT

package main

import (
	"fmt"
	"log"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
)

func main() {
	// Create a DataSet metric with typed columns.
	columns := []string{"name", "value", "active"}
	types := []pb.DataType{pb.DataType_String, pb.DataType_Double, pb.DataType_Boolean}
	rows := [][]any{
		{"sensor1", float64(22.5), true},
		{"sensor2", float64(33.1), false},
		{"sensor3", float64(18.9), true},
	}

	m, err := payload.NewDataSet("readings", columns, types, rows)
	if err != nil {
		log.Fatal(err)
	}

	// Encode and decode through protobuf.
	p := payload.NewPayload(0, m)
	b, _ := payload.Encode(p)
	p2, _ := payload.Decode(b)

	// Extract the DataSet back.
	cols, dts, gotRows, err := payload.GetDataSetValues(p2.Metrics[0])
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("DataSet %q: %d columns, %d rows\n\n", p2.Metrics[0].GetName(), len(cols), len(gotRows))

	// Print header.
	for i, c := range cols {
		fmt.Printf("  %-10s (%s)", c, dts[i])
		if i < len(cols)-1 {
			fmt.Print("  ")
		}
	}
	fmt.Println()
	fmt.Println("  " + "---------- ----------  ---------- ----------  ---------- ----------")

	// Print rows.
	for _, row := range gotRows {
		for i, v := range row {
			fmt.Printf("  %-22v", v)
			if i < len(row)-1 {
				fmt.Print("")
			}
		}
		fmt.Println()
	}
}
