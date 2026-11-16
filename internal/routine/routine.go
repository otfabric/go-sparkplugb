// SPDX-License-Identifier: MIT

// Package routine keeps the goroutines a runtime of this module starts, the
// functions of its timers included, so that its Close can wait for them.
//
// Close may be called by the application's code that one of these goroutines
// runs (an edge node's StateHandler during a walk to another server, say).
// Wait does not wait for the goroutine it is called on: that goroutine goes
// on once the application's code returns. To tell the goroutines apart, Wait
// reads the identifier of the calling goroutine from the header of its stack,
// once per call; so does each goroutine of the group when it starts.
package routine

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// Group is the goroutines and timers of a runtime. Its zero value is empty
// and ready to use.
type Group struct {
	mu   sync.Mutex
	runs map[*run]struct{}
}

// run is a goroutine of the group, or the function of a timer that has not
// fired yet.
type run struct {
	id   uint64        // the goroutine, once it runs; 0 before
	done chan struct{} // closed when it has returned, or its timer was stopped
}

func (g *Group) add() *run {
	r := &run{done: make(chan struct{})}
	g.mu.Lock()
	if g.runs == nil {
		g.runs = make(map[*run]struct{})
	}
	g.runs[r] = struct{}{}
	g.mu.Unlock()
	return r
}

func (g *Group) remove(r *run) {
	g.mu.Lock()
	delete(g.runs, r)
	g.mu.Unlock()
	close(r.done)
}

// call runs f as r, on the calling goroutine.
func (g *Group) call(r *run, f func()) {
	id := goroutineID()
	g.mu.Lock()
	r.id = id
	g.mu.Unlock()
	defer g.remove(r)
	f()
}

// Go calls f on a new goroutine of the group.
func (g *Group) Go(f func()) {
	r := g.add()
	go g.call(r, f)
}

// Timer is a timer of a Group.
type Timer struct {
	g *Group
	r *run
	t *time.Timer
}

// AfterFunc calls f on a goroutine of the group once d has passed, unless
// the timer is stopped first. Wait waits for the timer until then.
func (g *Group) AfterFunc(d time.Duration, f func()) *Timer {
	r := g.add()
	return &Timer{g: g, r: r, t: time.AfterFunc(d, func() { g.call(r, f) })}
}

// Stop stops the timer, and reports whether it did: false when its function
// has been called already, or the timer was stopped before.
func (t *Timer) Stop() bool {
	if !t.t.Stop() {
		return false
	}
	t.g.remove(t.r)
	return true
}

// Wait returns once every goroutine of the group has returned, and every
// timer has been stopped or has called its function and that has returned,
// those that they start meanwhile included. It does not wait for the
// goroutine it is called on.
//
// A timer that is neither stopped nor fired is waited for: stop the timers
// first. A goroutine started after Wait returned is not waited for.
func (g *Group) Wait() {
	self := goroutineID()
	for {
		var next chan struct{}
		g.mu.Lock()
		for r := range g.runs {
			if self == 0 || r.id != self {
				next = r.done
				break
			}
		}
		g.mu.Unlock()
		if next == nil {
			return
		}
		<-next
	}
}

// Len returns the number of goroutines and pending timers of the group.
func (g *Group) Len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.runs)
}

// goroutineID returns the identifier of the calling goroutine, from the
// first line of its stack ("goroutine 12 [running]:"), 0 if that cannot be
// read.
func goroutineID() uint64 {
	var buf [64]byte
	return parseID(buf[:runtime.Stack(buf[:], false)])
}

func parseID(stack []byte) uint64 {
	rest, ok := bytes.CutPrefix(stack, []byte("goroutine "))
	if !ok {
		return 0
	}
	n := bytes.IndexByte(rest, ' ')
	if n < 0 {
		return 0
	}
	id, err := strconv.ParseUint(string(rest[:n]), 10, 64)
	if err != nil {
		return 0
	}
	return id
}
