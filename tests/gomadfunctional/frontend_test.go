package gomadfunctional

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore"
)

func TestFrontendSystemInfo(t *testing.T) {
	env := testcore.NewEnv(t, testcore.WithInMemorySQLitePersistence())
	response, err := env.FrontendClient().GetSystemInfo(t.Context(), &workflowservice.GetSystemInfoRequest{})
	require.NoError(t, err)
	require.NotNil(t, response)
}
