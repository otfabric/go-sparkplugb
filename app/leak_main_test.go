// SPDX-License-Identifier: MIT

package app

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the test binary if a goroutine is still running after the
// tests finished (a Close that did not stop everything).
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
