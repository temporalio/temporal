package faultinjection

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	"go.uber.org/mock/gomock"
)

func TestExecutionPolicy(t *testing.T) {
	t.Parallel()
	realError := errors.New("datastore failure")
	hookError := errors.New("test hook")
	for _, tc := range []struct {
		name         string
		fault        string
		operationErr error
		hookErr      error
		wantCalls    int
	}{
		{name: "disabled", wantCalls: 1},
		{name: "timeout skips execution", fault: "Timeout"},
		{name: "execute and timeout preserves response", fault: "ExecuteAndTimeout", wantCalls: 1},
		{name: "execute and timeout preserves real failure", fault: "ExecuteAndTimeout", operationErr: realError, wantCalls: 1},
		{name: "test hook wins over execute and timeout", fault: "ExecuteAndTimeout", hookErr: hookError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			baseFactory := mock.NewMockDataStoreFactory(ctrl)
			baseStore := mock.NewMockExecutionStore(ctrl)
			baseFactory.EXPECT().NewExecutionStore().Return(baseStore, nil)
			request := &persistence.InternalCreateWorkflowExecutionRequest{}
			response := &persistence.InternalCreateWorkflowExecutionResponse{}
			baseStore.EXPECT().CreateWorkflowExecution(gomock.Any(), request).Return(response, tc.operationErr).Times(tc.wantCalls)
			cfg := &config.FaultInjection{}
			if tc.fault != "" {
				cfg.WithError(config.ExecutionStoreName, "CreateWorkflowExecution", tc.fault, 1)
			}
			if tc.hookErr != nil {
				cfg.Injector = func(target config.FaultInjectionTarget) error {
					require.Equal(t, config.FaultInjectionTarget{Store: config.ExecutionStoreName, Method: "CreateWorkflowExecution", Request: request}, target)
					return tc.hookErr
				}
			}
			store, err := NewFaultInjectionDatastoreFactory(cfg, baseFactory).NewExecutionStore()
			require.NoError(t, err)
			got, err := store.CreateWorkflowExecution(t.Context(), request)
			if tc.wantCalls == 1 {
				require.Same(t, response, got)
			} else {
				require.Nil(t, got)
			}
			switch {
			case tc.hookErr != nil:
				require.ErrorIs(t, err, hookError)
			case tc.operationErr != nil:
				require.ErrorIs(t, err, realError)
			case tc.fault != "":
				require.ErrorAs(t, err, new(*persistence.TimeoutError))
			default:
				require.NoError(t, err)
			}
		})
	}
}

func TestInvalidProbabilitiesDoNotOpenStore(t *testing.T) {
	t.Parallel()
	baseFactory := mock.NewMockDataStoreFactory(gomock.NewController(t))
	cfg := (&config.FaultInjection{}).WithError(config.ExecutionStoreName, "CreateWorkflowExecution", "Timeout", 2)
	_, err := NewFaultInjectionDatastoreFactory(cfg, baseFactory).NewExecutionStore()
	require.ErrorContains(t, err, "probability must be between 0 and 1")
}
