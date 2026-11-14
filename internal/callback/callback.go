// SPDX-License-Identifier: MIT

// Package callback is how the runtimes of this module call the
// application's code: the callbacks of their Config, Metrics, the Logger, a
// BdSeqStore and a RetryPolicy. A call that does not return normally is
// recovered where it is made and logged at error level with its stack once
// the runtime holds none of its locks, and the runtime goes on as after an
// error of that code, or as if the call had returned.
//
// Queue delivers the state transitions of a runtime after its lock is
// released, in the order they were made.
package callback

import (
	"fmt"
	"runtime/debug"
	"sync"

	sparkplugb "github.com/otfabric/go-sparkplugb"
)

// Failure is a call of the application's code that did not return normally.
type Failure struct {
	// Callback names what was called, for example "Config.OnNodeData".
	Callback string
	// Value is what the call ended with.
	Value any
	stack string
}

// Error describes the call, for example
// "BdSeqStore.Load did not return normally: no file".
func (f *Failure) Error() string {
	return fmt.Sprintf("%s did not return normally: %v", f.Callback, f.Value)
}

// Try calls f, the application's code that callback names, and returns nil,
// or the Failure when f does not return normally. It does not log: a caller
// that holds a lock gives the Failure to Report once it has released it.
func Try(callback string, f func()) (fail *Failure) {
	defer func() {
		if r := recover(); r != nil {
			fail = &Failure{Callback: callback, Value: r, stack: string(debug.Stack())}
		}
	}()
	f()
	return nil
}

// Report logs fail through l at error level, with the value and the stack;
// a nil fail logs nothing.
func Report(l sparkplugb.Logger, fail *Failure) {
	if fail != nil {
		l.Errorf("callback did not return normally: callback=%s value=%v\n%s", fail.Callback, fail.Value, fail.stack)
	}
}

// Run is Try followed by Report, for a call made while the runtime holds
// none of its locks. It reports whether f returned normally.
func Run(l sparkplugb.Logger, callback string, f func()) bool {
	fail := Try(callback, f)
	Report(l, fail)
	return fail == nil
}

// Logger returns l with every method guarded: a call that does not return
// normally loses its entry and is reported once through l's Errorf, and
// dropped when that does not return normally either. NopLogger is returned
// as it is.
func Logger(l sparkplugb.Logger) sparkplugb.Logger {
	if l == nil || l == sparkplugb.NopLogger() {
		return l
	}
	return logger{l: l}
}

type logger struct{ l sparkplugb.Logger }

func (g logger) Debugf(format string, args ...any) {
	defer g.rescue()
	g.l.Debugf(format, args...)
}

func (g logger) Infof(format string, args ...any) {
	defer g.rescue()
	g.l.Infof(format, args...)
}

func (g logger) Warnf(format string, args ...any) {
	defer g.rescue()
	g.l.Warnf(format, args...)
}

func (g logger) Errorf(format string, args ...any) {
	defer g.rescue()
	g.l.Errorf(format, args...)
}

func (g logger) rescue() {
	if r := recover(); r != nil {
		g.failed(r)
	}
}

func (g logger) failed(r any) {
	defer func() { _ = recover() }()
	g.l.Errorf("callback did not return normally: callback=Logger value=%v\n%s", r, debug.Stack())
}

// Queue holds values, the state transitions of a runtime, and hands them to
// a function one at a time, in the order of Push. Push is called with the
// runtime's lock held, Deliver once it is released. A Deliver that finds
// another one at work returns at once: that one also delivers what was
// pushed meanwhile. So the function may call the runtime, which pushes and
// delivers again, without waiting for itself; what that call pushes is
// delivered after the function returns.
type Queue[T any] struct {
	mu   sync.Mutex
	vals []T
	busy bool
}

// Push appends v.
func (q *Queue[T]) Push(v T) {
	q.mu.Lock()
	q.vals = append(q.vals, v)
	q.mu.Unlock()
}

// Deliver calls f with every value pushed and not yet delivered, in order,
// unless another Deliver is at work. f must return normally: guard the
// application's code it calls with Run.
func (q *Queue[T]) Deliver(f func(T)) {
	q.mu.Lock()
	if q.busy {
		q.mu.Unlock()
		return
	}
	q.busy = true
	for len(q.vals) > 0 {
		v := q.vals[0]
		var zero T
		q.vals[0] = zero
		q.vals = q.vals[1:]
		q.mu.Unlock()
		f(v)
		q.mu.Lock()
	}
	q.vals = nil
	q.busy = false
	q.mu.Unlock()
}
