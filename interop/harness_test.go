// SPDX-License-Identifier: MIT

package interop

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

type broker struct {
	Name      string
	Image     string
	EnvVar    string
	Port      int
	Container string
	Env       []string
}

var brokers = []broker{
	{Name: "mosquitto", Image: MosquittoImage, EnvVar: "SPARKPLUGB_INTEROP_MOSQUITTO_IMAGE", Port: 11883},
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
		if err := waitForPort(ctx, b.Port); err != nil {
			t.Fatalf("broker %s not ready: %v", b.Name, err)
		}
		t.Logf("broker %s ready on port %d", b.Name, b.Port)
	}
}

func waitForPort(ctx context.Context, port int) error {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("port %d: %w", port, ctx.Err())
		case <-ticker.C:
			conn, err := net.DialTimeout("tcp", addr, time.Second)
			if err == nil {
				_ = conn.Close()
				return nil
			}
		}
	}
}

