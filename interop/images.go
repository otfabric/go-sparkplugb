// SPDX-License-Identifier: MIT

package interop

// The MQTT broker images the suite runs against. The environment variable named
// beside each overrides it, for example with a locally built image. The
// Interop workflow pins the same images (TestWorkflowPins).
const (
	// MosquittoImage is Eclipse Mosquitto 2.0.21, an open-source MQTT broker.
	// Override: SPARKPLUGB_INTEROP_MOSQUITTO_IMAGE.
	MosquittoImage = "eclipse-mosquitto:2.0.21"
	// HiveMQCEImage is HiveMQ Community Edition 2024.5, an open-source MQTT broker.
	// Override: SPARKPLUGB_INTEROP_HIVEMQ_CE_IMAGE.
	HiveMQCEImage = "hivemq/hivemq-ce:2024.5"
	// EMQXImage is EMQX 5.8.6, an open-source MQTT broker.
	// Override: SPARKPLUGB_INTEROP_EMQX_IMAGE.
	EMQXImage = "emqx/emqx:5.8.6"
	// NanoMQImage is NanoMQ 0.21, a lightweight MQTT broker.
	// Override: SPARKPLUGB_INTEROP_NANOMQ_IMAGE.
	NanoMQImage = "emqx/nanomq:0.21"
)
