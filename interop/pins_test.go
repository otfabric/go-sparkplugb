// SPDX-License-Identifier: MIT

package interop

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestWorkflowPins checks that the Interop workflow runs the images of
// images.go: the reference images by the same reference, the brokers by the
// same digest (the workflow pulls them through a mirror).
func TestWorkflowPins(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", ".github", "workflows", "interop.yml"))
	if err != nil {
		t.Skipf("interop workflow not found: %v", err)
	}
	wf := string(data)

	for name, image := range map[string]string{
		"tahu": TahuImage, "pysparkplug": PysparkplugImage, "sparkplugnet": SparkplugNetImage,
		"srad": SradImage, "tahu-js": TahuJSImage, "sparkplughost": SparkplugHostImage,
	} {
		if !strings.Contains(wf, envKey(name)+": "+image+"\n") {
			t.Errorf("workflow does not pin %s as %s", envKey(name), image)
		}
	}
	for name, image := range map[string]string{
		"mosquitto": MosquittoImage, "hivemq-ce": HiveMQCEImage, "emqx": EMQXImage, "nanomq": NanoMQImage,
	} {
		_, digest, ok := strings.Cut(image, "@")
		if !ok {
			t.Errorf("%s image %s is not pinned by digest", name, image)
			continue
		}
		if !pinsLine(wf, envKey(name), "@"+digest) {
			t.Errorf("workflow does not pin %s by %s", envKey(name), digest)
		}
	}
}

// pinsLine reports whether the workflow sets key to a value ending in suffix.
func pinsLine(wf, key, suffix string) bool {
	for _, line := range strings.Split(wf, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), key+": "); ok {
			return strings.HasSuffix(v, suffix)
		}
	}
	return false
}
