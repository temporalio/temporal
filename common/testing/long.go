package testing

import (
	"os"
	"strconv"
	"testing"
)

// LongTest calls Skip() on the testing.T or suite unless the environment variable
// TEMPORAL_TEST_LONG is set to true.
func LongTest(t any) {
	if long, _ := strconv.ParseBool(os.Getenv("TEMPORAL_TEST_LONG")); long {
		return
	}

	if s, ok := t.(interface{ T() *testing.T }); ok {
		t = s.T()
	}
	if s, ok := t.(interface{ Skip(...any) }); ok {
		s.Skip("skipping long test, use TEMPORAL_TEST_LONG=1 to run")
	}
	panic("skipping long test, use TEMPORAL_TEST_LONG=1 to run")
}
