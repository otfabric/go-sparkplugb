// SPDX-License-Identifier: MIT

package main

import (
	"fmt"
	"log"

	"github.com/otfabric/go-sparkplugb/payload"
	pb "github.com/otfabric/go-sparkplugb/proto"
)

func main() {
	// Define a template with member metrics and parameters.
	tempMetric, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(0))
	unitParam, _ := payload.NewTemplateParameter("unit", pb.DataType_String, "celsius")

	def, err := payload.NewTemplateDefinition("SensorTemplate", "1.0",
		[]*pb.Payload_Metric{tempMetric},
		[]*pb.Payload_Template_Parameter{unitParam})
	if err != nil {
		log.Fatal(err)
	}

	// Create an instance that overrides the parameter.
	instMetric, _ := payload.NewMetric("temperature", pb.DataType_Double, float64(72.5))
	instParam, _ := payload.NewTemplateParameter("unit", pb.DataType_String, "fahrenheit")

	inst, err := payload.NewTemplateInstance("sensor1", "SensorTemplate",
		[]*pb.Payload_Metric{instMetric},
		[]*pb.Payload_Template_Parameter{instParam})
	if err != nil {
		log.Fatal(err)
	}

	// Encode both into a payload and round-trip through protobuf.
	p := payload.NewPayload(0, def, inst)
	b, _ := payload.Encode(p)
	p2, _ := payload.Decode(b)

	for _, m := range p2.Metrics {
		tmpl := m.GetTemplateValue()
		fmt.Printf("metric=%s  is_definition=%v  template_ref=%q  version=%q  members=%d  params=%d\n",
			m.GetName(), tmpl.GetIsDefinition(), tmpl.GetTemplateRef(),
			tmpl.GetVersion(), len(tmpl.GetMetrics()), len(tmpl.GetParameters()))

		for _, pm := range tmpl.GetParameters() {
			v, _ := payload.GetTemplateParameterValue(pm)
			fmt.Printf("  param %s = %v\n", pm.GetName(), v)
		}
	}
}
