# Licensing — go-sparkplugb

This document records the provenance and licensing of third-party materials
incorporated into or referenced by this library.

---

## Library License

All original OT Fabric code in this repository is licensed under the MIT
License. See [LICENSE](../LICENSE) in the repository root.

Every original `.go` file carries the header:

```go
// SPDX-License-Identifier: MIT
```

---

## Eclipse Sparkplug Specification

| Property | Value |
|----------|-------|
| Title | Eclipse Sparkplug Specification Version 3.0.0 |
| Repository | https://github.com/eclipse-sparkplug/sparkplug |
| Commit | `e45b610` |
| License | EPL-2.0 (Eclipse Foundation Specification License) |
| Trademark | "Sparkplug" is a trademark of the Eclipse Foundation |

This library implements the Sparkplug B 3.0.0 specification. Implementing a
specification does not create a derivative work of the specification text.

---

## Eclipse Tahu Protobuf Schema

| Property | Value |
|----------|-------|
| File | `sparkplug_b/sparkplug_b.proto` |
| Repository | https://github.com/eclipse-tahu/tahu |
| Commit | `5736e404889d4b95910613040a99ba79589ffb13` |
| SHA-256 | `4432c5c483b7fb9732d0594c98a2e97dca5e517e39c5374a8b918d837f0b4a19` |
| License | EPL-2.0 |
| SPDX | `SPDX-License-Identifier: EPL-2.0` (in file header) |

### EPL-2.0 Implications

The Eclipse Public License 2.0 is a weak copyleft license. Under Section 3,
derivative works of EPL-2.0 source must be made available under the EPL-2.0
(or a compatible license).

The proto file does NOT designate a Secondary License (GPL-2.0+ compatible).

### Handling in This Repository

The `proto/` directory contains:

1. The original `sparkplug_b.proto` file (EPL-2.0)
2. Go source code generated from it by `protoc-gen-go` (EPL-2.0 derivative)

These files are licensed under EPL-2.0 and carry the header:

```go
// SPDX-License-Identifier: EPL-2.0
```

All code outside `proto/` is original OT Fabric work under the MIT License.
The two licenses are compatible for use within a single binary — MIT-licensed
code may depend on EPL-2.0 code.

---

## Trademark Notice

"Sparkplug", "Sparkplug Compatible", and the Sparkplug Logo are trademarks of
the Eclipse Foundation. This library uses these terms for identification
purposes in accordance with fair use.

This library does NOT claim "Sparkplug Compatible" status. That designation
requires:

1. Passing the official Eclipse Sparkplug TCK for the claimed profile
2. Submitting results to the Eclipse Foundation
3. Signing the compatibility agreement
4. Being listed on the official compatibility listing

---

## Runtime Dependencies

| Module | License | Purpose |
|--------|---------|---------|
| `google.golang.org/protobuf` | BSD-3-Clause | Protobuf runtime |
| `github.com/eclipse/paho.mqtt.golang` | EPL-2.0 / EDL-1.0 | MQTT 3.1.1 client (falls back to 3.1) |
| `github.com/gorilla/websocket` | BSD-2-Clause | Indirect: WebSocket transport of Paho |
| `golang.org/x/net` | BSD-3-Clause | Indirect: of Paho |
| `golang.org/x/sync` | BSD-3-Clause | Indirect: of Paho |

The Eclipse Distribution License 1.0 (EDL-1.0) is equivalent to the
BSD-3-Clause license.

---

## Verification

To verify the proto file has not been modified from upstream:

```bash
sha256sum proto/sparkplug_b.proto
# Expected: 4432c5c483b7fb9732d0594c98a2e97dca5e517e39c5374a8b918d837f0b4a19
```

To verify against the upstream commit:

```bash
git clone --depth 1 https://github.com/eclipse-tahu/tahu.git /tmp/tahu
cd /tmp/tahu
git fetch --depth 1 origin 5736e404889d4b95910613040a99ba79589ffb13
git checkout 5736e404889d4b95910613040a99ba79589ffb13
sha256sum sparkplug_b/sparkplug_b.proto
```
