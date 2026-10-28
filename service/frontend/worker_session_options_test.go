package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sessionpb "go.temporal.io/api/session/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
)

func TestValidateWorkerSessionOptions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		kind           enumspb.TaskQueueKind
		options        *sessionpb.WorkerSessionOptions
		singleSession  bool
		stableIdentity string
		invalid        bool
	}{
		{name: "regular poll", kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		{name: "ordinary sticky poll", kind: enumspb.TASK_QUEUE_KIND_STICKY},
		{name: "empty options on sticky poll", kind: enumspb.TASK_QUEUE_KIND_STICKY, options: &sessionpb.WorkerSessionOptions{}},
		{name: "options on normal queue", kind: enumspb.TASK_QUEUE_KIND_NORMAL, options: &sessionpb.WorkerSessionOptions{}, invalid: true},
		{name: "valid single session", kind: enumspb.TASK_QUEUE_KIND_STICKY, singleSession: true, stableIdentity: "queue"},
		{name: "empty stable identity", kind: enumspb.TASK_QUEUE_KIND_STICKY, singleSession: true, invalid: true},
		{name: "sticky queue differs from identity", kind: enumspb.TASK_QUEUE_KIND_STICKY, singleSession: true, stableIdentity: "session", invalid: true},
		{name: "single session on normal queue", kind: enumspb.TASK_QUEUE_KIND_NORMAL, singleSession: true, stableIdentity: "queue", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			options := tc.options
			if tc.singleSession {
				options = &sessionpb.WorkerSessionOptions{
					Admission: &sessionpb.WorkerSessionOptions_SingleSession{SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: tc.stableIdentity}},
				}
			}
			err := validateWorkerSessionOptions(options, &taskqueuepb.TaskQueue{Name: "queue", Kind: tc.kind})
			if tc.invalid {
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			} else {
				require.NoError(t, err)
			}
		})
	}
}
