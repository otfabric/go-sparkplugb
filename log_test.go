// SPDX-License-Identifier: MIT

package sparkplugb

import (
	"bytes"
	"log"
	"log/slog"
	"strings"
	"testing"
)

func TestNopLogger(t *testing.T) {
	l := NopLogger()
	l.Debugf("test %d", 1)
	l.Infof("test %d", 2)
	l.Warnf("test %d", 3)
	l.Errorf("test %d", 4)
}

func TestSlogLogger(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	l := NewSlogLogger(h)

	l.Debugf("debug %d", 1)
	l.Infof("info %d", 2)
	l.Warnf("warn %d", 3)
	l.Errorf("error %d", 4)

	out := buf.String()
	for _, want := range []string{"debug 1", "info 2", "warn 3", "error 4"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestSlogLoggerWith(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	l := NewSlogLogger(h).With("key", "val")

	l.Infof("hello")

	out := buf.String()
	if !strings.Contains(out, "key=val") {
		t.Errorf("With field missing in output:\n%s", out)
	}
}

func TestStdLogger(t *testing.T) {
	var buf bytes.Buffer
	l := NewStdLogger(log.New(&buf, "", 0), true)

	l.Debugf("debug %d", 1)
	l.Infof("info %d", 2)
	l.Warnf("warn %d", 3)
	l.Errorf("error %d", 4)

	out := buf.String()
	for _, want := range []string{"DEBUG debug 1", "INFO info 2", "WARN warn 3", "ERROR error 4"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestStdLoggerNoDebug(t *testing.T) {
	var buf bytes.Buffer
	l := NewStdLogger(log.New(&buf, "", 0), false)

	l.Debugf("hidden")
	l.Infof("visible")

	out := buf.String()
	if strings.Contains(out, "hidden") {
		t.Errorf("debug should be hidden:\n%s", out)
	}
	if !strings.Contains(out, "visible") {
		t.Errorf("info should be visible:\n%s", out)
	}
}

func TestStdLoggerWith(t *testing.T) {
	var buf bytes.Buffer
	l := NewStdLogger(log.New(&buf, "", 0), false).With("k", "v")

	l.Infof("test")

	if !strings.Contains(buf.String(), "k=v") {
		t.Errorf("With field missing:\n%s", buf.String())
	}
}

func TestStdLoggerNil(t *testing.T) {
	l := NewStdLogger(nil, false)
	l.Infof("should not panic")
}

func TestComponentLoggerNil(t *testing.T) {
	l := ComponentLogger(nil, "test")
	l.Infof("nop logger, should not panic")
}

func TestComponentLoggerPlain(t *testing.T) {
	var buf bytes.Buffer
	plain := NewStdLogger(log.New(&buf, "", 0), false)
	// StdLogger is a FieldLogger, so it should add the component.
	l := ComponentLogger(plain, "mycomp")
	l.Infof("hello")
	if !strings.Contains(buf.String(), "component=mycomp") {
		t.Errorf("component field missing:\n%s", buf.String())
	}
}
