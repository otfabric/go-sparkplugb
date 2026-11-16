// SPDX-License-Identifier: MIT

package routine

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// waited runs Wait and reports on the channel when it returned.
func waited(g *Group) <-chan struct{} {
	done := make(chan struct{})
	go func() { g.Wait(); close(done) }()
	return done
}

func returned(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// Wait returns once the goroutines of the group have returned.
func TestWaitForGoroutines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var g Group
		g.Wait() // nothing to wait for
		release := make(chan struct{})
		var ended atomic.Int32
		for range 3 {
			g.Go(func() { <-release; ended.Add(1) })
		}
		done := waited(&g)
		synctest.Wait()
		if returned(done) {
			t.Fatal("Wait returned while the goroutines of the group run")
		}
		close(release)
		synctest.Wait()
		if !returned(done) {
			t.Fatal("Wait did not return once the goroutines of the group returned")
		}
		if n := ended.Load(); n != 3 {
			t.Errorf("%d goroutines ended when Wait returned, want 3", n)
		}
		if n := g.Len(); n != 0 {
			t.Errorf("Len %d after Wait, want 0", n)
		}
	})
}

// A goroutine of the group that starts another before it returns: Wait
// waits for both.
func TestWaitForGoroutinesStartedMeanwhile(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var g Group
		release, second := make(chan struct{}), make(chan struct{})
		g.Go(func() {
			<-release
			g.Go(func() { <-second })
		})
		done := waited(&g)
		close(release)
		synctest.Wait()
		if returned(done) {
			t.Fatal("Wait returned while the goroutine started meanwhile runs")
		}
		close(second)
		synctest.Wait()
		if !returned(done) {
			t.Fatal("Wait did not return")
		}
	})
}

// Wait called on a goroutine of the group does not wait for that goroutine,
// and still waits for the others.
func TestWaitOnAGoroutineOfTheGroup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var g Group
		other := make(chan struct{})
		g.Go(func() { <-other })
		waitedInside := make(chan struct{})
		g.Go(func() {
			g.Wait()
			close(waitedInside)
		})
		synctest.Wait()
		if returned(waitedInside) {
			t.Fatal("Wait on a goroutine of the group returned while another runs")
		}
		close(other)
		synctest.Wait()
		if !returned(waitedInside) {
			t.Fatal("Wait on a goroutine of the group waits for itself")
		}
		g.Wait()
	})
}

// A timer is waited for until it is stopped, or until its function returned.
func TestWaitForTimers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var g Group
		var called atomic.Int32
		stopped := g.AfterFunc(time.Second, func() { called.Add(1) })
		release := make(chan struct{})
		fired := g.AfterFunc(time.Second, func() { <-release; called.Add(1) })
		if n := g.Len(); n != 2 {
			t.Fatalf("Len %d with two timers, want 2", n)
		}
		if !stopped.Stop() {
			t.Fatal("Stop of a pending timer returned false")
		}
		if stopped.Stop() {
			t.Error("second Stop returned true")
		}
		done := waited(&g)
		time.Sleep(2 * time.Second) // the clock of the bubble
		synctest.Wait()
		if returned(done) {
			t.Fatal("Wait returned while the function of a timer runs")
		}
		if fired.Stop() {
			t.Error("Stop of a timer whose function was called returned true")
		}
		close(release)
		synctest.Wait()
		if !returned(done) {
			t.Fatal("Wait did not return once the function of the timer returned")
		}
		if n := called.Load(); n != 1 {
			t.Errorf("%d timer functions called, want 1 (the other was stopped)", n)
		}
	})
}

func TestGoroutineID(t *testing.T) {
	id := goroutineID()
	if id == 0 {
		t.Fatal("goroutineID() = 0 on the test goroutine")
	}
	if again := goroutineID(); again != id {
		t.Errorf("goroutineID() = %d, then %d", id, again)
	}
	other := make(chan uint64)
	go func() { other <- goroutineID() }()
	if o := <-other; o == 0 || o == id {
		t.Errorf("goroutineID() of another goroutine = %d, want neither 0 nor %d", o, id)
	}
	for _, tc := range []struct {
		stack string
		want  uint64
	}{
		{"goroutine 12 [running]:\n", 12},
		{"goroutine 18446744073709551615 [select]:\n", 18446744073709551615},
		{"goroutine 18446744073709551616 [select]:\n", 0},
		{"goroutine x [running]", 0},
		{"goroutine 12", 0},
		{"thread 12 [running]", 0},
		{"", 0},
	} {
		if got := parseID([]byte(tc.stack)); got != tc.want {
			t.Errorf("parseID(%q) = %d, want %d", tc.stack, got, tc.want)
		}
	}
}
