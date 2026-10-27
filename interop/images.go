// SPDX-License-Identifier: MIT

package interop

// InteropRelease is the release of otfabric/sparkplugb-interop the images
// below come from. Moving to another release means changing these constants;
// the Interop workflow pins the same references (TestWorkflowPins).
const InteropRelease = "v0.1.0"

// The reference implementations, as container images that follow the
// sparkplugb-interop container contract, pinned by digest. The environment
// variable named beside each overrides it (a local build or a candidate); an
// empty value leaves the adapter out.
const (
	// TahuImage is Eclipse Tahu (Java), edge node and host application.
	// Override: SPARKPLUGB_INTEROP_TAHU_IMAGE.
	TahuImage = "ghcr.io/otfabric/sparkplugb-interop-tahu:" + InteropRelease + "@sha256:913e97106f4ea9388b7688aca748bf96a6ae344fe5308207ebbbe200d8eaebcb"
	// PysparkplugImage is pysparkplug (Python), edge node and host application.
	// Override: SPARKPLUGB_INTEROP_PYSPARKPLUG_IMAGE.
	PysparkplugImage = "ghcr.io/otfabric/sparkplugb-interop-pysparkplug:" + InteropRelease + "@sha256:7473b1ceb06e26a6a8541546fada998eb222ea9464d031ad56ab7a5badb36e44"
	// SparkplugNetImage is SparkplugNet (C#), edge node and host application.
	// Override: SPARKPLUGB_INTEROP_SPARKPLUGNET_IMAGE.
	SparkplugNetImage = "ghcr.io/otfabric/sparkplugb-interop-sparkplugnet:" + InteropRelease + "@sha256:919fce5b243a370656ddcb082ef95d0a4f001b991c7521ba3a06e64aa8a74a39"
	// SradImage is srad (Rust), edge node and host application.
	// Override: SPARKPLUGB_INTEROP_SRAD_IMAGE.
	SradImage = "ghcr.io/otfabric/sparkplugb-interop-srad:" + InteropRelease + "@sha256:dddf909f714c0b7d606871e7adb9bd4034d4cad6b0fbd3133dd00daf12662633"
	// TahuJSImage is Eclipse Tahu sparkplug-client (JavaScript), edge node.
	// Override: SPARKPLUGB_INTEROP_TAHU_JS_IMAGE.
	TahuJSImage = "ghcr.io/otfabric/sparkplugb-interop-tahu-js:" + InteropRelease + "@sha256:d278a2eebb12c34a05a29a126fc17908e9b9058666ea9f26d1f475edd0401b3e"
	// SparkplugHostImage is sparkplughost (Go), host application.
	// Override: SPARKPLUGB_INTEROP_SPARKPLUGHOST_IMAGE.
	SparkplugHostImage = "ghcr.io/otfabric/sparkplugb-interop-sparkplughost:" + InteropRelease + "@sha256:69265cbb76a542248b265db2c34ad3b5fc5add5d68a4adc85d91bdabd8a8cd60"
)

// The MQTT brokers, pinned by multi-arch digest as sparkplugb-interop
// InteropRelease pins them (its manifest.json). Override with the variable
// named beside each.
const (
	// MosquittoImage is Eclipse Mosquitto 2.1.2.
	// Override: SPARKPLUGB_INTEROP_MOSQUITTO_IMAGE.
	MosquittoImage = "docker.io/library/eclipse-mosquitto:2.1.2-alpine@sha256:860aeb5d63ce509508f6dcfb2bc8a1c1dd2bc60555b562b45ce90e52d8dabb0f"
	// HiveMQCEImage is HiveMQ Community Edition 2026.5.
	// Override: SPARKPLUGB_INTEROP_HIVEMQ_CE_IMAGE.
	HiveMQCEImage = "docker.io/hivemq/hivemq-ce:2026.5@sha256:bac6d4d1d815bffe4bc9a7fca8d06ea12b32d34f84ef2f1851ff4548560ee67b"
	// EMQXImage is EMQX 6.3.2.
	// Override: SPARKPLUGB_INTEROP_EMQX_IMAGE.
	EMQXImage = "docker.io/emqx/emqx:6.3.2@sha256:135116400e11b25224350b2e55c874f4e224d357b593fb554ca1b1f398c631ac"
	// NanoMQImage is NanoMQ 0.25.7.
	// Override: SPARKPLUGB_INTEROP_NANOMQ_IMAGE.
	NanoMQImage = "docker.io/emqx/nanomq:0.25.7@sha256:ed5a9bb892f58e59aa4e47a9932fb7e056f5ae57ba201414c793b8c961587784"
)
