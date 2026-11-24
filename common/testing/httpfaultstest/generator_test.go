package httpfaultstest_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/common/testing/httpfaultstest"
)

// TestNewGenerator_Enabled checks that only an enabled source yields a generator, so a disabled
// one installs no fault interceptor.
func TestNewGenerator_Enabled(t *testing.T) {
	t.Parallel()

	require.Nil(t, httpfaultstest.NewGenerator(failpoints.NewSource(failpoints.Decision{})))
	require.Nil(t, httpfaultstest.NewGenerator(nil))
	require.NotNil(t, httpfaultstest.NewGenerator(failpoints.NewEnabledSource()))
}
