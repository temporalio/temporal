package rpc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/rpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRequestIssues(t *testing.T) {
	t.Parallel()
	var issues rpc.RequestIssues
	err := issues.GetError()
	require.NoError(t, err)
	issues.Append("issue 1")
	issues.Appendf("issue %d", 2)
	issues.Append("issue 3")
	err = issues.GetError()
	require.Error(t, err)
	assert.ErrorContains(t, err, "issue 1, issue 2, issue 3")
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
