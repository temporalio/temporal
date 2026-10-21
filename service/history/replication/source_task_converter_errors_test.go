package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestSourceTaskConverterBuildErrorClassification(t *testing.T) {
	t.Parallel()
	_, unknownTaskErr := (&ackMgrImpl{}).ConvertTask(t.Context(), &tasks.WorkflowTask{})
	for _, tc := range []struct {
		name      string
		engineErr error
		makeTask  func(definition.WorkflowKey) tasks.Task
		wantErr   error
		wantText  string
		wantSkip  bool
	}{
		{name: "unknown_conversion_task", engineErr: unknownTaskErr, wantErr: errUnknownReplicationTask, wantSkip: true},
		{
			name: "unknown_serialization_task",
			makeTask: func(key definition.WorkflowKey) tasks.Task {
				return &tasks.WorkflowTask{WorkflowKey: key}
			},
			wantText: "Unknown repication task type:",
			wantSkip: true,
		},
		{
			name: "unknown_serialization_equivalent",
			makeTask: func(key definition.WorkflowKey) tasks.Task {
				return &tasks.SyncVersionedTransitionTask{
					WorkflowKey:     key,
					TaskEquivalents: []tasks.Task{&tasks.WorkflowTask{WorkflowKey: key}},
				}
			},
			wantText: "Unknown repication task type:",
			wantSkip: true,
		},
		{name: "busy_workflow", engineErr: consts.ErrResourceExhaustedBusyWorkflow, wantErr: consts.ErrResourceExhaustedBusyWorkflow},
		{
			name:      "unidentified_internal",
			engineErr: serviceerror.NewInternal("version histories does not contains given item"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tv := testvars.New(t)
			key := definition.NewWorkflowKey(tv.NamespaceID().String(), tv.WorkflowID(), tv.RunID())
			var task tasks.Task = &tasks.SyncActivityTask{WorkflowKey: key}
			if tc.makeTask != nil {
				task = tc.makeTask(key)
			}
			ctrl := gomock.NewController(t)
			registry := namespace.NewMockRegistry(ctrl)
			registry.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil)
			engine := historyi.NewMockEngine(ctrl)
			engine.EXPECT().ConvertReplicationTask(gomock.Any(), task, int32(3)).Return(&replicationspb.ReplicationTask{}, tc.engineErr)
			serializer := serialization.NewSerializer()
			converter := NewSourceTaskConverter(engine, registry, serializer, serializer, tests.NewDynamicConfig())

			result, err := converter.Convert(task, 3, enumsspb.TASK_PRIORITY_HIGH, locks.PriorityLow)
			require.Nil(t, result)
			wantErr := tc.wantErr
			if wantErr == nil {
				wantErr = tc.engineErr
			}
			if wantErr != nil {
				require.ErrorIs(t, err, wantErr)
			} else {
				require.ErrorContains(t, err, tc.wantText)
			}
			require.Equal(t, err.Error(), serviceerror.ToStatus(err).Message())
			require.Equal(t, tc.wantSkip, isSkippable(&convertError{err: err}))
		})
	}
}
