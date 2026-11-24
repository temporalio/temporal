package grpcfaultstest_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/common/testing/grpcfaultstest"
	"go.temporal.io/testx/failpoint/failpointpb"
)

// TestNewGenerator_Enabled checks that only an enabled source yields a generator, so a disabled
// one installs no fault interceptor.
func TestNewGenerator_Enabled(t *testing.T) {
	t.Parallel()

	require.Nil(t, grpcfaultstest.NewGenerator(failpoints.NewSource(failpoints.Decision{}), nil, primitives.HistoryService))
	require.Nil(t, grpcfaultstest.NewGenerator(nil, nil, primitives.HistoryService))
	require.NotNil(t, grpcfaultstest.NewGenerator(failpoints.NewEnabledSource(), nil, primitives.HistoryService))
}

func TestFailpointServicePrefix(t *testing.T) {
	t.Parallel()

	require.Equal(t, grpcfaultstest.FailpointServicePrefix, "/"+failpointpb.FailpointService_ServiceDesc.ServiceName+"/")
}
