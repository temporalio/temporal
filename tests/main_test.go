package tests

import (
	"testing"

	"go.temporal.io/server/tests/testcore"
)

func TestMain(m *testing.M) {
	testcore.RunTests(m.Run)
}
