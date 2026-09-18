package history

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/nexusoperations"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	currentRunNexusOperationID        = "operation-id"
	currentRunNexusOperationRequestID = "request-id"
)

func TestAccessCurrentNexusOperationRejectsInvalidLookup(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		runID     string
		requestID string
	}{
		{
			name:      "run ID is set",
			runID:     "run-id",
			requestID: currentRunNexusOperationRequestID,
		},
		{
			name: "request ID is empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := stateMachineEnvironment{}
			accessorCalled := false
			err := env.accessCurrentNexusOperation(
				context.Background(),
				hsm.Ref{WorkflowKey: definition.NewWorkflowKey("namespace-id", "workflow-id", tc.runID)},
				tc.requestID,
				func(*hsm.Node) error {
					accessorCalled = true
					return nil
				},
			)

			var notFound *serviceerror.NotFound
			require.ErrorAs(t, err, &notFound)
			require.False(t, accessorCalled)
		})
	}
}

func TestAccessCurrentNexusOperation(t *testing.T) {
	testCases := []struct {
		name                string
		requestID           string
		workflowState       enumsspb.WorkflowExecutionState
		mutateRef           func(*hsm.Ref)
		accessor            func(*hsm.Node) error
		wantAccessorCalled  bool
		wantWorkflowUpdates int
		wantWorkflowLoads   int
		assertError         func(*testing.T, error)
	}{
		{
			name:                "matching operation",
			requestID:           currentRunNexusOperationRequestID,
			workflowState:       enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			accessor:            func(*hsm.Node) error { return nil },
			wantAccessorCalled:  true,
			wantWorkflowUpdates: 1,
			wantWorkflowLoads:   1,
			assertError:         func(t *testing.T, err error) { require.NoError(t, err) },
		},
		{
			name:              "request ID mismatch",
			requestID:         "other-request-id",
			workflowState:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			accessor:          func(*hsm.Node) error { return nil },
			wantWorkflowLoads: 1,
			assertError: func(t *testing.T, err error) {
				var notFound *serviceerror.NotFound
				require.ErrorAs(t, err, &notFound)
			},
		},
		{
			name:          "missing operation after reload",
			requestID:     currentRunNexusOperationRequestID,
			workflowState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			mutateRef: func(ref *hsm.Ref) {
				ref.StateMachineRef.Path[0].Id = "missing-operation-id"
			},
			accessor:          func(*hsm.Node) error { return nil },
			wantWorkflowLoads: 2,
			assertError: func(t *testing.T, err error) {
				var notFound *serviceerror.NotFound
				require.ErrorAs(t, err, &notFound)
			},
		},
		{
			name:              "zombie workflow",
			requestID:         currentRunNexusOperationRequestID,
			workflowState:     enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			accessor:          func(*hsm.Node) error { return nil },
			wantWorkflowLoads: 1,
			assertError: func(t *testing.T, err error) {
				require.ErrorIs(t, err, consts.ErrWorkflowZombie)
			},
		},
		{
			name:                "accessor error",
			requestID:           currentRunNexusOperationRequestID,
			workflowState:       enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			accessor:            func(*hsm.Node) error { return errors.New("accessor failed") },
			wantAccessorCalled:  true,
			wantWorkflowLoads:   1,
			wantWorkflowUpdates: 0,
			assertError: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "accessor failed")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := newStateMachineEnvTestContext(t, true)
			t.Cleanup(s.TearDown)
			require.NoError(t, nexusoperations.RegisterStateMachines(s.mockShard.StateMachineRegistry()))
			require.NoError(t, nexusoperations.RegisterTaskSerializers(s.mockShard.StateMachineRegistry()))

			mutableState := s.prepareMutableStateWithReadyNexusCompletionCallback()
			mutableState.GetExecutionState().State = tc.workflowState
			_, err := nexusoperations.AddChild(
				mutableState.HSM(),
				currentRunNexusOperationID,
				&historypb.HistoryEvent{
					EventId:   1,
					EventTime: timestamppb.New(time.Now()),
					EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
					Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
						NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
							EndpointId: "endpoint-id",
							Endpoint:   "endpoint",
							Service:    "service",
							Operation:  "operation",
							RequestId:  currentRunNexusOperationRequestID,
						},
					},
				},
				[]byte("event-token"),
			)
			require.NoError(t, err)
			_, _, err = mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
			require.NoError(t, err)

			persistenceMutableState := workflow.TestCloneToProto(context.Background(), mutableState)
			executionManager := s.mockShard.GetExecutionManager().(*persistence.MockExecutionManager)
			currentExecutionLoads := 1
			if tc.workflowState != enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING {
				currentExecutionLoads = 2
			}
			executionManager.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetCurrentExecutionResponse{
				RunID: mutableState.GetWorkflowKey().RunID,
			}, nil).Times(currentExecutionLoads)
			executionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(
				&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState},
				nil,
			).Times(tc.wantWorkflowLoads)
			executionManager.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
				tests.UpdateWorkflowExecutionResponse,
				nil,
			).Times(tc.wantWorkflowUpdates)

			env := stateMachineEnvironment{
				shardContext:   s.mockShard,
				cache:          s.workflowCache,
				metricsHandler: s.mockShard.GetMetricsHandler(),
				logger:         s.mockShard.GetLogger(),
			}
			ref := hsm.Ref{
				WorkflowKey: definition.NewWorkflowKey(
					s.namespaceID.String(),
					mutableState.GetWorkflowKey().WorkflowID,
					"",
				),
				StateMachineRef: &persistencespb.StateMachineRef{
					Path: []*persistencespb.StateMachineKey{{
						Type: nexusoperations.OperationMachineType,
						Id:   currentRunNexusOperationID,
					}},
					MutableStateVersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: 999,
						TransitionCount:          999,
					},
				},
			}
			if tc.mutateRef != nil {
				tc.mutateRef(&ref)
			}

			accessorCalled := false
			err = env.accessCurrentNexusOperation(context.Background(), ref, tc.requestID, func(node *hsm.Node) error {
				accessorCalled = true
				operation, err := hsm.MachineData[nexusoperations.Operation](node)
				require.NoError(t, err)
				require.Equal(t, currentRunNexusOperationRequestID, operation.RequestId)
				return tc.accessor(node)
			})

			tc.assertError(t, err)
			require.Equal(t, tc.wantAccessorCalled, accessorCalled)
		})
	}
}
