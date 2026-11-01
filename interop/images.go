// SPDX-License-Identifier: MIT

package interop

// InteropRelease is the release of otfabric/sparkplugb-interop the images
// below come from. Moving to another release means changing these constants;
// the Interop workflow pins the same references (TestWorkflowPins).
const InteropRelease = "v0.3.0"

// The reference implementations, as container images that follow the
// sparkplugb-interop container contract, pinned by digest. The environment
// variable named beside each overrides it (a local build or a candidate); an
// empty value leaves the adapter out.
const (
	// TahuImage is Eclipse Tahu (Java), edge node and host application.
	// Override: SPARKPLUGB_INTEROP_TAHU_IMAGE.
	TahuImage = "ghcr.io/otfabric/sparkplugb-interop-tahu:" + InteropRelease + "@sha256:322127f8be82eb046abd28869eb47de6924b55288ab71fa6b011245d01ae5741"
	// PysparkplugImage is pysparkplug (Python), edge node and host application.
	// Override: SPARKPLUGB_INTEROP_PYSPARKPLUG_IMAGE.
	PysparkplugImage = "ghcr.io/otfabric/sparkplugb-interop-pysparkplug:" + InteropRelease + "@sha256:1405e89f83df743b9054cccff289d7c65a873626b104b45341caad3fa98c5855"
	// SparkplugNetImage is SparkplugNet (C#), edge node and host application.
	// Override: SPARKPLUGB_INTEROP_SPARKPLUGNET_IMAGE.
	SparkplugNetImage = "ghcr.io/otfabric/sparkplugb-interop-sparkplugnet:" + InteropRelease + "@sha256:1a0aee704cf01a5b3d39d86201bba40b93e5df12cc5bca94a8bf340dca20a8ee"
	// SradImage is srad (Rust), edge node and host application.
	// Override: SPARKPLUGB_INTEROP_SRAD_IMAGE.
	SradImage = "ghcr.io/otfabric/sparkplugb-interop-srad:" + InteropRelease + "@sha256:e56a9188e33845fb634bd5804ff129641b619a353c2d79c2548635d6c532df65"
	// TahuJSImage is Eclipse Tahu sparkplug-client (JavaScript), edge node.
	// Override: SPARKPLUGB_INTEROP_TAHU_JS_IMAGE.
	TahuJSImage = "ghcr.io/otfabric/sparkplugb-interop-tahu-js:" + InteropRelease + "@sha256:802bc5a2d574c981abf05bf58e077d261b18cf3237b8cfa0c2344441880dc2fb"
	// SparkplugHostImage is sparkplughost (Go), host application.
	// Override: SPARKPLUGB_INTEROP_SPARKPLUGHOST_IMAGE.
	SparkplugHostImage = "ghcr.io/otfabric/sparkplugb-interop-sparkplughost:" + InteropRelease + "@sha256:5ca0a074f7f8ef7b2480fb53f86151b673991415c10e804ba53168da5c82d807"
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
