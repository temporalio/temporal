package testcore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testlogger"
)

func TestTestEnvStartLogCapture(t *testing.T) {
	testLogger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	var capture *testlogger.Capture
	t.Run("capture", func(t *testing.T) {
		env := &TestEnv{
			FunctionalTestBase: &FunctionalTestBase{externalNamespace: namespace.Name("external")},
			Logger:             testLogger,
			nsName:             namespace.Name("primary"),
			nsID:               namespace.ID("primary-id"),
			t:                  t,
		}
		capture = env.StartLogCapture()

		testLogger.Info("primary name", tag.WorkflowNamespace("primary"))
		testLogger.Info("primary ID", tag.WorkflowNamespaceID("primary-id"))
		testLogger.Info("external name", tag.WorkflowNamespace("external"))
		testLogger.Info("unrelated name", tag.WorkflowNamespace("unrelated"))
		testLogger.Info("unrelated ID", tag.WorkflowNamespaceID("unrelated-id"))
		testLogger.Info("target only", tag.NexusEndpointTargetNamespaceID("primary-id"))
		testLogger.Info("unscoped")
	})
	testLogger.Info("after cleanup", tag.WorkflowNamespace("primary"))

	records := capture.Snapshot()
	require.Len(t, records, 3)
	require.Equal(t, "primary name", records[0].Message)
	require.Equal(t, "primary ID", records[1].Message)
	require.Equal(t, "external name", records[2].Message)
}
