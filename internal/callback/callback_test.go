// SPDX-License-Identifier: MIT

package callback

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	sparkplugb "github.com/otfabric/go-sparkplugb"
)

// recLogger records the entries logged at error level; the methods named in
// fail do not return normally.
type recLogger struct {
	mu      sync.Mutex
	errors  []string
	infos   int
	failing map[string]bool
}

func (l *recLogger) maybe(level string) {
	if l.failing[level] {
		panic(level + " logger")
	}
}

func (l *recLogger) Debugf(string, ...any) { l.maybe("debug") }

func (l *recLogger) Infof(string, ...any) {
	l.maybe("info")
	l.mu.Lock()
	l.infos++
	l.mu.Unlock()
}

func (l *recLogger) Warnf(string, ...any) { l.maybe("warn") }

func (l *recLogger) Errorf(format string, args ...any) {
	l.maybe("error")
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, fmt.Sprintf(format, args...))
}

func (l *recLogger) logged() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.errors)
}

func TestTry(t *testing.T) {
	if fail := Try("Config.OnState", func() {}); fail != nil {
		t.Errorf("Try of a function that returns: %v", fail)
	}
	fail := Try("Config.OnState", func() { panic("boom") })
	if fail == nil {
		t.Fatal("Try of a function that does not return normally: nil")
	}
	if fail.Callback != "Config.OnState" || fail.Value != "boom" {
		t.Errorf("Failure %+v", fail)
	}
	if got, want := fail.Error(), "Config.OnState did not return normally: boom"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if !strings.Contains(fail.stack, "goroutine ") || !strings.Contains(fail.stack, "TestTry") {
		t.Errorf("stack %q, want the goroutine's stack", fail.stack)
	}
	if fail := Try("x", func() { panic(fmt.Errorf("an error")) }); fail == nil || fail.Error() != "x did not return normally: an error" {
		t.Errorf("Failure of an error value: %v", fail)
	}
}

func TestReportAndRun(t *testing.T) {
	l := &recLogger{}
	Report(l, nil)
	if got := l.logged(); len(got) != 0 {
		t.Errorf("Report(nil) logged %q", got)
	}
	if !Run(l, "Metrics.OnMessage", func() {}) {
		t.Error("Run of a function that returns: false")
	}
	if Run(l, "Metrics.OnMessage", func() { panic(42) }) {
		t.Error("Run of a function that does not return normally: true")
	}
	got := l.logged()
	if len(got) != 1 {
		t.Fatalf("logged %d errors, want 1", len(got))
	}
	if !strings.HasPrefix(got[0], "callback did not return normally: callback=Metrics.OnMessage value=42\n") ||
		!strings.Contains(got[0], "goroutine ") {
		t.Errorf("logged %q, want the callback, the value and the stack", got[0])
	}
}

func TestLogger(t *testing.T) {
	if Logger(nil) != nil {
		t.Error("Logger(nil) is not nil")
	}
	if Logger(sparkplugb.NopLogger()) != sparkplugb.NopLogger() {
		t.Error("Logger(NopLogger()) is not NopLogger()")
	}

	l := &recLogger{failing: map[string]bool{"debug": true, "info": true, "warn": true}}
	g := Logger(l)
	g.Debugf("d")
	g.Infof("i")
	g.Warnf("w")
	g.Errorf("e %d", 1)
	got := l.logged()
	if len(got) != 4 {
		t.Fatalf("logged %d errors, want 4: %q", len(got), got)
	}
	for i, level := range []string{"debug", "info", "warn"} {
		if !strings.HasPrefix(got[i], "callback did not return normally: callback=Logger value="+level+" logger\n") {
			t.Errorf("entry %d: %q", i, got[i])
		}
	}
	if got[3] != "e 1" {
		t.Errorf("entry 3: %q, want the entry itself", got[3])
	}

	// A Logger of which every method fails loses the entries, and the
	// caller goes on.
	all := Logger(&recLogger{failing: map[string]bool{"debug": true, "info": true, "warn": true, "error": true}})
	all.Debugf("d")
	all.Infof("i")
	all.Warnf("w")
	all.Errorf("e")
}

func TestQueueDeliversInOrder(t *testing.T) {
	var q Queue[int]
	var got []int
	for i := range 3 {
		q.Push(i)
	}
	q.Deliver(func(v int) {
		got = append(got, v)
		if v == 1 { // pushed and delivered from f: after f returns
			q.Push(10)
			q.Deliver(func(int) { t.Error("a Deliver during another one delivered") })
			q.Push(11)
		}
	})
	if want := []int{0, 1, 2, 10, 11}; !slices.Equal(got, want) {
		t.Errorf("delivered %v, want %v", got, want)
	}
	q.Deliver(func(v int) { t.Errorf("delivered %d twice", v) })
}

// Values pushed from several goroutines are each delivered once, one at a
// time.
func TestQueueConcurrent(t *testing.T) {
	var q Queue[int]
	var mu sync.Mutex
	seen := map[int]int{}
	var running atomic.Int32
	deliver := func(v int) {
		if running.Add(1) != 1 {
			t.Error("two deliveries at once")
		}
		mu.Lock()
		seen[v]++
		mu.Unlock()
		running.Add(-1)
	}
	const goroutines, each = 8, 200
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range each {
				q.Push(g*each + i)
				q.Deliver(deliver)
			}
		})
	}
	wg.Wait()
	q.Deliver(deliver)
	if len(seen) != goroutines*each {
		t.Errorf("delivered %d values, want %d", len(seen), goroutines*each)
	}
	for v, n := range seen {
		if n != 1 {
			t.Errorf("value %d delivered %d times", v, n)
		}
	}
}
