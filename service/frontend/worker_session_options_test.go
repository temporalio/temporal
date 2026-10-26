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
		name        string
		kind        enumspb.TaskQueueKind
		options     *sessionpb.WorkerSessionOptions
		invalid     bool
		unsupported bool
	}{
		{name: "regular poll", kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		{name: "ordinary sticky poll", kind: enumspb.TASK_QUEUE_KIND_STICKY},
		{name: "empty options on sticky poll", kind: enumspb.TASK_QUEUE_KIND_STICKY, options: &sessionpb.WorkerSessionOptions{}},
		{name: "options on normal queue", kind: enumspb.TASK_QUEUE_KIND_NORMAL, options: &sessionpb.WorkerSessionOptions{}, invalid: true},
		{name: "unsupported isolation", kind: enumspb.TASK_QUEUE_KIND_STICKY, options: &sessionpb.WorkerSessionOptions{Isolation: &sessionpb.WorkerSessionOptions_SingleSession{SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: "session"}}}, unsupported: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateWorkerSessionOptions(tc.options, &taskqueuepb.TaskQueue{Name: "queue", Kind: tc.kind})
			switch {
			case tc.invalid:
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			case tc.unsupported:
				require.ErrorAs(t, err, new(*serviceerror.Unimplemented))
			default:
				require.NoError(t, err)
			}
		})
	}
}
