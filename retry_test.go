// SPDX-License-Identifier: MIT

package sparkplugb

import (
	"errors"
	"testing"
	"time"
)

func TestNoRetry(t *testing.T) {
	p := NoRetry()
	retry, _ := p.ShouldRetry(0, errors.New("fail"))
	if retry {
		t.Error("NoRetry should never retry")
	}
}

func TestExponentialBackoffDefaults(t *testing.T) {
	b := ExponentialBackoff{NoJitter: true}

	for attempt := 0; attempt < 5; attempt++ {
		retry, wait := b.ShouldRetry(attempt, errors.New("fail"))
		if !retry {
			t.Errorf("attempt %d: should retry", attempt)
		}
		if wait <= 0 {
			t.Errorf("attempt %d: wait should be > 0, got %v", attempt, wait)
		}
	}

	retry, _ := b.ShouldRetry(5, errors.New("fail"))
	if retry {
		t.Error("should stop after 5 attempts")
	}
}

func TestExponentialBackoffDelayDoubles(t *testing.T) {
	b := ExponentialBackoff{
		Initial:  100 * time.Millisecond,
		Max:      10 * time.Second,
		NoJitter: true,
	}

	d0 := b.Delay(0)
	d1 := b.Delay(1)
	d2 := b.Delay(2)

	if d0 != 100*time.Millisecond {
		t.Errorf("delay(0) = %v, want 100ms", d0)
	}
	if d1 != 200*time.Millisecond {
		t.Errorf("delay(1) = %v, want 200ms", d1)
	}
	if d2 != 400*time.Millisecond {
		t.Errorf("delay(2) = %v, want 400ms", d2)
	}
}

func TestExponentialBackoffCapsAtMax(t *testing.T) {
	b := ExponentialBackoff{
		Initial:  1 * time.Second,
		Max:      2 * time.Second,
		NoJitter: true,
	}

	d := b.Delay(10)
	if d != 2*time.Second {
		t.Errorf("delay(10) = %v, want 2s cap", d)
	}
}

func TestExponentialBackoffUnbounded(t *testing.T) {
	b := ExponentialBackoff{Attempts: -1, NoJitter: true}

	retry, _ := b.ShouldRetry(100, errors.New("fail"))
	if !retry {
		t.Error("negative Attempts should be unbounded")
	}
}

func TestExponentialBackoffJitter(t *testing.T) {
	b := ExponentialBackoff{Initial: 1 * time.Second}

	seen := make(map[time.Duration]bool)
	for i := 0; i < 20; i++ {
		d := b.Delay(0)
		seen[d] = true
		if d < 500*time.Millisecond || d > 1*time.Second {
			t.Errorf("delay with jitter out of range: %v", d)
		}
	}
	if len(seen) < 2 {
		t.Error("jitter should produce varying delays")
	}
}

// A delay near the largest Duration stays at Max, and never wraps around
// to a negative or zero wait.
func TestExponentialBackoffLargeDelays(t *testing.T) {
	const most = time.Duration(1<<63 - 1)
	for _, b := range []ExponentialBackoff{
		{Initial: most/2 + 1, Max: most, NoJitter: true},
		{Initial: most - 1, Max: most, NoJitter: true},
		{Initial: 3 * time.Second, Max: most, NoJitter: true},
		{Initial: time.Hour, Max: time.Minute, NoJitter: true},
	} {
		for _, attempt := range []int{0, 1, 2, 62, 63, 64, 1000} {
			d := b.Delay(attempt)
			if d <= 0 || d > b.Max {
				t.Errorf("Initial %v, Max %v: Delay(%d) = %v, want in (0, Max]", b.Initial, b.Max, attempt, d)
			}
		}
	}
	if d := (ExponentialBackoff{Initial: most/2 + 1, Max: most, NoJitter: true}).Delay(1); d != most {
		t.Errorf("Delay(1) = %v, want Max", d)
	}
}
