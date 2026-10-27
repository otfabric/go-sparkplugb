// SPDX-License-Identifier: MIT

package interop

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otfabric/go-sparkplugb/mqtt"
)

// ── Reference implementations ────────────────────────────────────────────────

// adapter is one reference implementation, as a container image that follows
// the sparkplugb-interop container contract.
type adapter struct {
	name  string
	image string
}

// adapters returns the reference implementations to test against. The image
// of each can be overridden with SPARKPLUGB_INTEROP_<NAME>_IMAGE (see
// images.go); an empty value leaves it out. SPARKPLUGB_INTEROP_ADAPTERS
// restricts the run to a comma-separated subset.
func adapters(t testing.TB) []adapter {
	t.Helper()
	all := []adapter{
		{"tahu", envImage("tahu", TahuImage)},
		{"pysparkplug", envImage("pysparkplug", PysparkplugImage)},
		{"sparkplugnet", envImage("sparkplugnet", SparkplugNetImage)},
		{"srad", envImage("srad", SradImage)},
		{"tahu-js", envImage("tahu-js", TahuJSImage)},
		{"sparkplughost", envImage("sparkplughost", SparkplugHostImage)},
	}
	return selectNamed(t, "SPARKPLUGB_INTEROP_ADAPTERS", all, func(a adapter) (string, string) { return a.name, a.image })
}

func envKey(name string) string {
	return "SPARKPLUGB_INTEROP_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_IMAGE"
}

// envImage returns the override of the image, the default when there is none.
// A variable set to the empty string returns "".
func envImage(name, def string) string {
	if v, ok := os.LookupEnv(envKey(name)); ok {
		return v
	}
	return def
}

func selectNamed[T any](t testing.TB, key string, all []T, id func(T) (name, image string)) []T {
	t.Helper()
	var only []string
	if v := os.Getenv(key); v != "" {
		for _, s := range strings.Split(v, ",") {
			only = append(only, strings.TrimSpace(s))
		}
	}
	var out []T
	for _, x := range all {
		name, image := id(x)
		switch {
		case len(only) > 0 && !slices.Contains(only, name):
		case image != "":
			out = append(out, x)
		case len(only) > 0:
			t.Fatalf("%s selects %s, which has no image: set %s", key, name, envKey(name))
		}
	}
	if len(only) > 0 && len(out) == 0 {
		t.Fatalf("%s=%q selects nothing", key, os.Getenv(key))
	}
	return out
}

// capabilities is the document "print-capabilities" prints.
type capabilities struct {
	AdapterVersion string `json:"adapterVersion"`
	Upstream       struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"upstream"`
	Roles struct {
		Edge struct {
			Supported bool            `json:"supported"`
			Fixtures  []string        `json:"fixtures"`
			Calls     []string        `json:"calls"`
			Features  map[string]bool `json:"features"`
		} `json:"edge"`
		Host struct {
			Supported bool            `json:"supported"`
			Datatypes []string        `json:"datatypes"`
			Calls     []string        `json:"calls"`
			Features  map[string]bool `json:"features"`
		} `json:"host"`
	} `json:"roles"`
}

var (
	capsCache    sync.Map // image -> *capabilities
	fixtureCache sync.Map // image + "/" + name -> *fixture
)

func (a adapter) caps(t testing.TB) *capabilities {
	t.Helper()
	if c, ok := capsCache.Load(a.image); ok {
		return c.(*capabilities)
	}
	out, stderr, err := docker("run", "--rm", a.image, "print-capabilities")
	if err != nil {
		t.Fatalf("print-capabilities of %s: %v\n%s", a.image, err, stderr)
	}
	c := &capabilities{}
	if err := json.Unmarshal([]byte(out), c); err != nil {
		t.Fatalf("capabilities of %s are not JSON: %v", a.image, err)
	}
	capsCache.Store(a.image, c)
	return c
}

func (a adapter) isEdge(t testing.TB) bool { return a.caps(t).Roles.Edge.Supported }
func (a adapter) isHost(t testing.TB) bool { return a.caps(t).Roles.Host.Supported }

func (a adapter) edgeHas(t testing.TB, feature string) bool {
	return a.caps(t).Roles.Edge.Features[feature]
}

func (a adapter) hostHas(t testing.TB, feature string) bool {
	return a.caps(t).Roles.Host.Features[feature]
}

func (a adapter) edgeCalls(t testing.TB, ops ...string) bool {
	for _, op := range ops {
		if !slices.Contains(a.caps(t).Roles.Edge.Calls, op) {
			return false
		}
	}
	return true
}

func (a adapter) hostCalls(t testing.TB, ops ...string) bool {
	for _, op := range ops {
		if !slices.Contains(a.caps(t).Roles.Host.Calls, op) {
			return false
		}
	}
	return true
}

// fixture returns the named fixture baked into the image.
func (a adapter) fixture(t testing.TB, name string) *fixture {
	t.Helper()
	key := a.image + "/" + name
	if f, ok := fixtureCache.Load(key); ok {
		return f.(*fixture).clone()
	}
	out, stderr, err := docker("run", "--rm", a.image, "print-fixture", name)
	if err != nil {
		t.Fatalf("print-fixture %s from %s: %v\n%s", name, a.image, err, stderr)
	}
	f := &fixture{}
	if err := json.Unmarshal([]byte(out), f); err != nil {
		t.Fatalf("fixture %s of %s is not JSON: %v", name, a.image, err)
	}
	fixtureCache.Store(key, f)
	return f.clone()
}

// fixtures lists the fixtures a reference edge serves. A reference host is
// given every fixture whose datatypes it declares.
func (a adapter) edgeFixtures(t testing.TB) []string { return a.caps(t).Roles.Edge.Fixtures }

func (a adapter) hostAccepts(t testing.TB, f *fixture) (missing []string) {
	for _, dt := range f.datatypes() {
		if !slices.Contains(a.caps(t).Roles.Host.Datatypes, dt) {
			missing = append(missing, dt)
		}
	}
	return missing
}

// knownBug reports whether the reference has a bug that sparkplugb-interop
// lists in tests/known-bugs.tsv for its version, so that a test can expect
// it instead of failing on the reference.
func (a adapter) knownBug(t testing.TB, role, broker, fixture, step string) (string, bool) {
	t.Helper()
	c := a.caps(t)
	for _, k := range knownBugs {
		if k.impl == a.name && k.version == c.Upstream.Version && k.role == role &&
			(k.broker == "" || slices.Contains(strings.Split(k.broker, "|"), broker)) &&
			(k.fixture == "" || k.fixture == fixture) &&
			(slices.Contains(k.steps, "*") || slices.Contains(k.steps, step)) {
			return k.what, true
		}
	}
	return "", false
}

type knownBugEntry struct {
	impl, version, role string
	broker              string // "" for every broker, or names separated by "|"
	fixture             string // "" for every fixture
	steps               []string
	what                string
}

// reorderBug is the bug of the hosts that apply the messages of an edge node
// in the order they arrive: MQTT orders messages per topic only, and NanoMQ
// and HiveMQ CE deliver the messages of one node on different topics out of
// publishing order. It shows in some runs only, so the pairing is skipped.
const reorderBug = "does not reorder the messages of an edge node by seq: on this broker, which delivers messages of different topics out of publishing order, the host misses data and requests rebirths (intermittent)"

// knownBugs are the upstream bugs of the pinned references that change what
// go-sparkplugb observes, as sparkplugb-interop documents them.
var knownBugs = []knownBugEntry{
	{"pysparkplug", "0.6.1", "edge", "", "", []string{"graceful-death"},
		"EdgeNode advances bdSeq after NBIRTH: the NDEATH of disconnect() carries bdSeq+1"},
	{"pysparkplug", "0.6.1", "host", "", "scalars", []string{"births"},
		"decodes Int8 and Int16 only as width-sized two's complement: a negative value sign-extended to 32 bits, as Tahu and go-sparkplugb encode it, raises OverflowError and the whole NBIRTH is dropped"},
	{"pysparkplug", "0.6.1", "host", "nanomq|hivemq-ce", "", []string{"*"}, reorderBug},
	{"srad", "0.5.0", "host", "nanomq|hivemq-ce", "", []string{"*"}, reorderBug},
	{"sparkplughost", "v0.4.0", "host", "nanomq|hivemq-ce", "", []string{"*"}, reorderBug},
	{"sparkplugnet", "1.4.1", "host", "", "", []string{"device-death-birth", "graceful-death", "will"},
		"SparkplugApplication updates its node and device states on NDEATH and DDEATH through a lazy iterator it never enumerates: dead nodes and devices stay online"},
	{"sparkplugnet", "1.4.1", "edge", "", "baseline", []string{"writes", "device-death-birth"},
		"SparkplugNode filters the metrics of a DCMD against the node's metrics, not the device's: a device write is dropped"},
}

// ── Brokers ──────────────────────────────────────────────────────────────────

type broker struct {
	name  string
	image string
	env   []string
	cmd   []string
}

// brokers returns the brokers to run on: every pinned one, or the subset of
// SPARKPLUGB_INTEROP_BROKERS.
func brokers(t testing.TB) []broker {
	t.Helper()
	all := []broker{
		{name: "mosquitto", image: envImage("mosquitto", MosquittoImage), cmd: []string{"mosquitto", "-c", "/mosquitto-no-auth.conf"}},
		{name: "hivemq-ce", image: envImage("hivemq-ce", HiveMQCEImage)},
		{name: "emqx", image: envImage("emqx", EMQXImage), env: []string{"EMQX_ALLOW_ANONYMOUS=true"}},
		{name: "nanomq", image: envImage("nanomq", NanoMQImage)},
	}
	return selectNamed(t, "SPARKPLUGB_INTEROP_BROKERS", all, func(b broker) (string, string) { return b.name, b.image })
}

// ── Docker ───────────────────────────────────────────────────────────────────

func docker(args ...string) (stdout, stderr string, err error) {
	var out, errb bytes.Buffer
	cmd := exec.Command("docker", args...)
	cmd.Stdout, cmd.Stderr = &out, &errb
	err = cmd.Run()
	return out.String(), errb.String(), err
}

var seq atomic.Int64

func uniqueName(kind string) string {
	return fmt.Sprintf("go-sparkplugb-%s-%d-%d", kind, os.Getpid(), seq.Add(1))
}

// world is one test's private Docker network with a broker on it. The
// references reach the broker by its container name; go-sparkplugb, on the
// host, through a port published on loopback.
type world struct {
	t       testing.TB
	broker  broker
	network string
	name    string // broker container
	refURL  string // the broker as the references reach it
	url     string // the broker as go-sparkplugb reaches it
}

// worlds bounds how many worlds run at once (SPARKPLUGB_INTEROP_PARALLEL,
// default 4): every world is a broker and a network, and JVM and Erlang
// brokers starting side by side, or too many networks, exhaust a runner.
var worlds = func() chan struct{} {
	n, err := strconv.Atoi(os.Getenv("SPARKPLUGB_INTEROP_PARALLEL"))
	if err != nil || n < 1 {
		n = 4
	}
	return make(chan struct{}, n)
}()

func newWorld(t testing.TB, b broker) *world {
	t.Helper()
	worlds <- struct{}{}
	t.Cleanup(func() { <-worlds })
	w := &world{t: t, broker: b, network: uniqueName("net"), name: uniqueName(b.name)}
	if _, stderr, err := docker("network", "create", w.network); err != nil {
		t.Fatalf("create network: %v\n%s", err, stderr)
	}
	t.Cleanup(func() { _, _, _ = docker("network", "rm", w.network) })

	args := []string{"run", "-d", "--name", w.name, "--network", w.network, "-p", "127.0.0.1::1883"}
	for _, e := range b.env {
		args = append(args, "-e", e)
	}
	args = append(append(args, b.image), b.cmd...)
	if _, stderr, err := docker(args...); err != nil {
		t.Fatalf("start %s: %v\n%s", b.name, err, stderr)
	}
	t.Cleanup(func() {
		if t.Failed() {
			out, errOut, _ := docker("logs", "--tail", "40", w.name)
			t.Logf("%s log:\n%s%s", b.name, out, errOut)
		}
		_, _, _ = docker("rm", "-f", "-v", w.name)
	})
	out, _, err := docker("port", w.name, "1883/tcp")
	if err != nil {
		t.Fatalf("port of %s: %v", b.name, err)
	}
	hostPort := strings.TrimSpace(strings.Split(strings.TrimSpace(out), "\n")[0])
	if _, port, err := net.SplitHostPort(hostPort); err == nil {
		w.url = "tcp://127.0.0.1:" + port
	} else {
		t.Fatalf("port of %s: %q", b.name, out)
	}
	w.refURL = "tcp://" + w.name + ":1883"

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := waitForMQTT(ctx, w.url); err != nil {
		t.Fatalf("%s not ready: %v", b.name, err)
	}
	return w
}

// waitForMQTT polls until the broker answers an MQTT CONNECT; a bare TCP
// dial is not enough because Docker's port proxy accepts before the broker does.
func waitForMQTT(ctx context.Context, url string) error {
	var lastErr error
	for {
		tr := mqtt.NewPahoTransport()
		tr.ConnectTimeout = 2 * time.Second
		lastErr = tr.Connect(ctx, mqtt.ConnectOpts{BrokerURL: url, ClientID: uniqueName("probe"), CleanSession: true})
		if lastErr == nil {
			return tr.Disconnect(ctx)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w (last error: %v)", url, ctx.Err(), lastErr)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// ── Reference roles ──────────────────────────────────────────────────────────

// role is a running "edge" or "host" of a reference image.
type role struct {
	w       *world
	adapter adapter
	kind    string
	name    string
	args    []string
}

// connectTimeoutMs is the MQTT connection timeout of every reference role
// (the contract's default is 10 s). Eclipse Tahu on a busy runner took longer
// than that to reach a HiveMQ that had just started, and gave up. It stays
// below the 90 s a role has to become ready.
const connectTimeoutMs = "60000"

// start runs a role of the reference on the world's broker and waits until it
// is ready: an edge has published its births, a host its STATE.
func (w *world) start(a adapter, kind string, args ...string) *role {
	w.t.Helper()
	r := &role{w: w, adapter: a, kind: kind, name: uniqueName(a.name + "-" + kind),
		args: append([]string{kind, "--broker", w.refURL, "--connect-timeout-ms", connectTimeoutMs}, args...)}
	r.run()
	w.t.Cleanup(func() {
		if w.t.Failed() {
			out, errOut, _ := docker("logs", "--tail", "60", r.name)
			w.t.Logf("%s %s log:\n%s%s", a.name, kind, out, errOut)
		}
		_, _, _ = docker("rm", "-f", "-v", r.name)
	})
	r.waitReady()
	return r
}

func (r *role) run() {
	r.w.t.Helper()
	args := append([]string{"run", "-d", "--name", r.name, "--network", r.w.network, r.adapter.image}, r.args...)
	if _, stderr, err := docker(args...); err != nil {
		r.w.t.Fatalf("start %s %s: %v\n%s", r.adapter.name, r.kind, err, stderr)
	}
}

func (r *role) waitReady() {
	t := r.w.t
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		if _, _, err := docker("exec", r.name, "test", "-f", "/run/sparkplugb-interop/ready"); err == nil {
			return
		}
		if state, _, _ := docker("inspect", "--format", "{{.State.Status}}", r.name); strings.TrimSpace(state) != "running" {
			out, errOut, _ := docker("logs", "--tail", "40", r.name)
			t.Fatalf("%s %s is %q, not running:\n%s%s", r.adapter.name, r.kind, strings.TrimSpace(state), out, errOut)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s %s not ready after 90s", r.adapter.name, r.kind)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// stop sends SIGTERM: an edge publishes its NDEATH, a host its STATE offline.
func (r *role) stop() {
	r.w.t.Helper()
	if _, stderr, err := docker("stop", "-t", "10", r.name); err != nil {
		r.w.t.Fatalf("stop %s %s: %v\n%s", r.adapter.name, r.kind, err, stderr)
	}
}

// kill drops the connection without a word: the broker publishes the will.
func (r *role) kill() {
	r.w.t.Helper()
	if _, stderr, err := docker("kill", r.name); err != nil {
		r.w.t.Fatalf("kill %s %s: %v\n%s", r.adapter.name, r.kind, err, stderr)
	}
}

// restart starts the stopped container again, with a fresh MQTT session.
func (r *role) restart() {
	r.w.t.Helper()
	_, _, _ = docker("rm", "-f", "-v", r.name)
	r.run()
	r.waitReady()
}

// result is a call result document.
type result struct {
	OK        bool            `json:"ok"`
	Operation string          `json:"operation"`
	Result    json.RawMessage `json:"result"`
	Error     *struct {
		Code   string `json:"code"`
		Detail string `json:"detail"`
	} `json:"error"`
	exit int
}

// call runs one operation on the role through its control socket.
func (r *role) call(op string, payload any) *result {
	t := r.w.t
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err := docker("exec", r.name, "sparkplugb-interop", "call", op, "--payload", string(b))
	res := &result{}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
		res.exit = exitErr.ExitCode()
	default:
		t.Fatalf("call %s on %s: %v", op, r.adapter.name, err)
	}
	line := strings.TrimSpace(stdout)
	if line == "" || strings.Contains(line, "\n") {
		t.Fatalf("call %s on %s %s: want one result line, got %q (exit %d, stderr %q)", op, r.adapter.name, r.kind, stdout, res.exit, stderr)
	}
	if err := json.Unmarshal([]byte(line), res); err != nil {
		t.Fatalf("call %s on %s: result is not JSON: %v\n%s", op, r.adapter.name, err, line)
	}
	return res
}

// decodeResult decodes the "result" of a call result document into v.
func decodeResult(t testing.TB, res *result, v any) {
	t.Helper()
	if err := json.Unmarshal(res.Result, v); err != nil {
		t.Fatalf("%s result %s: %v", res.Operation, res.Result, err)
	}
}

// mustCall is call that fails the test unless the operation succeeded.
func (r *role) mustCall(op string, payload any) *result {
	r.w.t.Helper()
	res := r.call(op, payload)
	if !res.OK {
		r.w.t.Fatalf("call %s on %s %s failed: exit %d, %+v", op, r.adapter.name, r.kind, res.exit, res.Error)
	}
	return res
}

// event is one line of a role's JSON Lines output.
type event struct {
	Event    string        `json:"event"`
	Type     string        `json:"type"`
	Group    string        `json:"group"`
	EdgeNode string        `json:"edgeNode"`
	Device   *string       `json:"device"`
	Seq      *int          `json:"seq"`
	BdSeq    *int          `json:"bdSeq"`
	Metrics  []metricValue `json:"metrics"`
	HostID   string        `json:"hostId"`
	Online   *bool         `json:"online"`
	Applied  *bool         `json:"applied"`
	Reason   string        `json:"reason"`
}

func (e event) device() string {
	if e.Device == nil {
		return ""
	}
	return *e.Device
}

func (e event) String() string {
	s := e.Event
	if e.Type != "" {
		s += " " + e.Type
	}
	if d := e.device(); d != "" {
		s += "/" + d
	}
	if e.Seq != nil {
		s += fmt.Sprintf(" seq=%d", *e.Seq)
	}
	if e.BdSeq != nil {
		s += fmt.Sprintf(" bdSeq=%d", *e.BdSeq)
	}
	return s
}

func (r *role) events() []event {
	t := r.w.t
	t.Helper()
	out, _, err := docker("logs", r.name)
	if err != nil {
		t.Fatalf("logs of %s %s: %v", r.adapter.name, r.kind, err)
	}
	var evs []event
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var e event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("%s %s wrote a stdout line that is not JSON: %q", r.adapter.name, r.kind, line)
		}
		evs = append(evs, e)
	}
	return evs
}

// matching returns the events that satisfy match.
func (r *role) matching(match func(event) bool) []event {
	var out []event
	for _, e := range r.events() {
		if match(e) {
			out = append(out, e)
		}
	}
	return out
}

// waitFor waits until at least n events satisfy match and returns them all.
func (r *role) waitFor(what string, n int, match func(event) bool) []event {
	t := r.w.t
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		got := r.matching(match)
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s %s: %d of %d %s events after %s; got %v", r.adapter.name, r.kind, len(got), n, what, eventTimeout, got)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

const eventTimeout = 20 * time.Second

// eventually polls cond until it holds or the timeout passes.
func eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// logReference records which reference build a test ran against, and checks
// that a pinned image is the release it is pinned as.
func logReference(t testing.TB, a adapter) {
	t.Helper()
	c := a.caps(t)
	t.Logf("%s: sparkplugb-interop %s, %s %s (%s)", a.name, c.AdapterVersion, c.Upstream.Name, c.Upstream.Version, a.image)
	if os.Getenv(envKey(a.name)) == "" && c.AdapterVersion != InteropRelease {
		t.Errorf("%s is pinned as sparkplugb-interop %s but reports %s", a.name, InteropRelease, c.AdapterVersion)
	}
}

// TestReferences checks that the pinned images are what the documentation
// says this version is qualified against.
func TestReferences(t *testing.T) {
	for _, a := range adapters(t) {
		logReference(t, a)
		if !a.isEdge(t) && !a.isHost(t) {
			t.Errorf("%s declares neither role", a.name)
		}
	}
}
