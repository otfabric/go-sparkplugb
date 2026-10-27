// SPDX-License-Identifier: MIT

package interop

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkflowPins(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", ".github", "workflows", "interop.yml"))
	if err != nil {
		t.Skipf("interop workflow not found: %v", err)
	}
	wf := string(data)

	images := []struct {
		name  string
		image string
	}{
		{"mosquitto", MosquittoImage},
		{"hivemq-ce", HiveMQCEImage},
		{"emqx", EMQXImage},
		{"nanomq", NanoMQImage},
	}
	for _, img := range images {
		if !strings.Contains(wf, img.image) {
			t.Errorf("workflow does not pin %s image %s", img.name, img.image)
		}
	}
}
