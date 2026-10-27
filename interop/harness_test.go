// SPDX-License-Identifier: MIT

package interop

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/mqtt"
)

type broker struct {
	Name      string
	Image     string
	EnvVar    string
	Port      int
	Container string
	Env       []string
	Cmd       []string
}

var brokers = []broker{
	{Name: "mosquitto", Image: MosquittoImage, EnvVar: "SPARKPLUGB_INTEROP_MOSQUITTO_IMAGE", Port: 11883, Cmd: []string{"mosquitto", "-c", "/mosquitto-no-auth.conf"}},
	{Name: "hivemq-ce", Image: HiveMQCEImage, EnvVar: "SPARKPLUGB_INTEROP_HIVEMQ_CE_IMAGE", Port: 11884},
	{Name: "emqx", Image: EMQXImage, EnvVar: "SPARKPLUGB_INTEROP_EMQX_IMAGE", Port: 11885, Env: []string{"EMQX_ALLOW_ANONYMOUS=true"}},
	{Name: "nanomq", Image: NanoMQImage, EnvVar: "SPARKPLUGB_INTEROP_NANOMQ_IMAGE", Port: 11886},
}

func resolveImage(b broker) string {
	if v := os.Getenv(b.EnvVar); v != "" {
		return v
	}
	return b.Image
}

func startBrokers(t *testing.T) {
	t.Helper()
	for i := range brokers {
		b := &brokers[i]
		image := resolveImage(*b)

		args := []string{
			"run", "-d", "--rm",
			"--name", fmt.Sprintf("sparkplugb-interop-%s", b.Name),
			"-p", fmt.Sprintf("127.0.0.1:%d:1883/tcp", b.Port),
		}
		for _, e := range b.Env {
			args = append(args, "-e", e)
		}
		args = append(args, image)
		args = append(args, b.Cmd...)

		out, err := exec.Command("docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("start %s: %v\n%s", b.Name, err, out)
		}
		b.Container = strings.TrimSpace(string(out))
		t.Cleanup(func() {
			_ = exec.Command("docker", "rm", "-f", b.Container).Run()
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	for _, b := range brokers {
		if err := waitForMQTT(ctx, b.Port); err != nil {
			t.Fatalf("broker %s not ready: %v", b.Name, err)
		}
		t.Logf("broker %s ready on port %d", b.Name, b.Port)
	}
}

// waitForMQTT polls until the broker answers an MQTT CONNECT; a bare TCP
// dial is not enough because Docker's port proxy accepts before the broker does.
func waitForMQTT(ctx context.Context, port int) error {
	url := fmt.Sprintf("tcp://127.0.0.1:%d", port)
	var lastErr error
	for {
		tr := mqtt.NewPahoTransport()
		tr.ConnectTimeout = 2 * time.Second
		lastErr = tr.Connect(ctx, mqtt.ConnectOpts{BrokerURL: url, ClientID: fmt.Sprintf("probe-%d", port), CleanSession: true})
		if lastErr == nil {
			return tr.Disconnect(ctx)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("port %d: %w (last error: %v)", port, ctx.Err(), lastErr)
		case <-time.After(500 * time.Millisecond):
		}
	}
}
