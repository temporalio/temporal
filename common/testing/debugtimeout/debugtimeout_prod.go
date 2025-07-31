//go:build !test_dep && !integration

package debugtimeout

import (
	"time"
)

const Multiplier time.Duration = 1
