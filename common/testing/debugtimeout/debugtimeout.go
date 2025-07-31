//go:build test_dep || integration

package debugtimeout

import (
	"os"
	"strconv"
	"time"
)

var Multiplier = getMultiplier()

func getMultiplier() time.Duration {
	v, ok := os.LookupEnv("TEMPORAL_DEBUG_TIMEOUT")
	if !ok {
		return time.Duration(1)
	}
	if v == "" {
		return time.Duration(10)
	}
	parsed, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		panic("TEMPORAL_DEBUG_TIMEOUT should be empty or a valid integer, got: " + v)
	}
	return time.Duration(parsed)
}
