// SPDX-License-Identifier: MIT

package interop

// InteropRelease is the release of otfabric/sparkplugb-interop the images
// below come from. Moving to another release means changing these constants;
// the Interop workflow pins the same references (TestWorkflowPins).
const InteropRelease = "v0.4.0"

// The reference implementations, as container images that follow the
// sparkplugb-interop container contract, pinned by digest. The environment
// variable named beside each overrides it (a local build or a candidate); an
// empty value leaves the adapter out.
const (
	// TahuImage is Eclipse Tahu (Java), edge node and host application.
	// Override: SPARKPLUGB_INTEROP_TAHU_IMAGE.
	TahuImage = "ghcr.io/otfabric/sparkplugb-interop-tahu:" + InteropRelease + "@sha256:540056d47e9e7e257fbb14a48d7eb80dce5e79a4752acf1a59b4d74a6c40ddfa"
	// PysparkplugImage is pysparkplug (Python), edge node and host application.
	// Override: SPARKPLUGB_INTEROP_PYSPARKPLUG_IMAGE.
	PysparkplugImage = "ghcr.io/otfabric/sparkplugb-interop-pysparkplug:" + InteropRelease + "@sha256:593281b65453b2eb4dacb57f014f6ab1789a8009927c18bfb207576e7c150993"
	// SparkplugNetImage is SparkplugNet (C#), edge node and host application.
	// Override: SPARKPLUGB_INTEROP_SPARKPLUGNET_IMAGE.
	SparkplugNetImage = "ghcr.io/otfabric/sparkplugb-interop-sparkplugnet:" + InteropRelease + "@sha256:c7b4bc620cb84ee276e93a43b60bb365c71144adfd78775b6454e219071f7dc9"
	// SradImage is srad (Rust), edge node and host application.
	// Override: SPARKPLUGB_INTEROP_SRAD_IMAGE.
	SradImage = "ghcr.io/otfabric/sparkplugb-interop-srad:" + InteropRelease + "@sha256:6617fe6493af987de17d603daf7b10bc406c53e90302b5a2f32774d446c516b0"
	// TahuJSImage is Eclipse Tahu sparkplug-client (JavaScript), edge node.
	// Override: SPARKPLUGB_INTEROP_TAHU_JS_IMAGE.
	TahuJSImage = "ghcr.io/otfabric/sparkplugb-interop-tahu-js:" + InteropRelease + "@sha256:c4a5a0c73f84e7c12433335ff8cab4656293f2e87b12866b8736a93dcd850152"
	// SparkplugHostImage is sparkplughost (Go), host application.
	// Override: SPARKPLUGB_INTEROP_SPARKPLUGHOST_IMAGE.
	SparkplugHostImage = "ghcr.io/otfabric/sparkplugb-interop-sparkplughost:" + InteropRelease + "@sha256:87a6d4834c3dc64ddc1e019491d7d798e514890d802220838c0c39a4dc788676"
)

// There is no default image of go-sparkplugb itself (sparkplugb-interop's
// otfabric adapter, ghcr.io/otfabric/sparkplugb-interop-otfabric): it runs only
// when SPARKPLUGB_INTEROP_OTFABRIC_IMAGE names one, as the Self-interop
// workflow does with :latest. A pass against it is not independent evidence.

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
