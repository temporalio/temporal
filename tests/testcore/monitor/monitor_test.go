package monitor_test

import (
	testmonitor "go.temporal.io/server/tests/testcore/monitor"
	"go.temporal.io/server/tests/umpire2"
	"go.temporal.io/server/tests/umpirev1"
)

var (
	_ testmonitor.Monitor = (*umpirev1.Monitor)(nil)
	_ testmonitor.Monitor = (*umpire2.Monitor)(nil)
)
