package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPersistenceFaultInjectionValidate(t *testing.T) {
	t.Parallel()
	var cfg *FaultInjection
	require.NoError(t, cfg.Validate())
	require.NoError(t, DefaultFaultInjection().Validate())
	cfg = (&FaultInjection{}).
		WithError(ExecutionStoreName, "CreateWorkflowExecution", "Timeout", 0.3).
		WithError(ExecutionStoreName, "CreateWorkflowExecution", "ExecuteAndTimeout", 0.2)
	ds := DataStore{SQL: &SQL{}, FaultInjection: cfg}
	require.NoError(t, ds.Validate())
	cfg.WithError(ExecutionStoreName, "CreateWorkflowExecution", "ExecuteAndTimeout", 0.8)
	require.ErrorContains(t, ds.Validate(), "faultInjection: targets.dataStores.ExecutionStore.methods.CreateWorkflowExecution")
}
