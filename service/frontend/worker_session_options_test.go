package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sessionpb "go.temporal.io/api/session/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
)

func TestValidateWorkerSessionOptions(t *testing.T) {
	t.Parallel()
	singleSession := func(stableIdentity string) *sessionpb.WorkerSessionOptions {
		return &sessionpb.WorkerSessionOptions{
			Admission: &sessionpb.WorkerSessionOptions_SingleSession{SingleSession: &sessionpb.SingleSessionOptions{StableIdentity: stableIdentity}},
		}
	}
	for _, tc := range []struct {
		name        string
		sticky      string // name of the polled sticky queue; "" polls the normal queue "normal"
		options     *sessionpb.WorkerSessionOptions
		instanceKey string
		buildIDs    bool // the worker polls with old build-ID versioning
		invalid     bool
	}{
		{name: "regular poll"},
		{name: "ordinary sticky poll", sticky: "queue"},
		{name: "empty options on sticky poll", sticky: "queue", options: &sessionpb.WorkerSessionOptions{}},
		{name: "options on normal queue", options: &sessionpb.WorkerSessionOptions{}, invalid: true},
		{name: "single session", sticky: "S", options: singleSession("S"), instanceKey: "key"},
		{name: "single session on normal queue", options: singleSession("S"), instanceKey: "key", invalid: true},
		{name: "empty stable identity", sticky: "S", options: singleSession(""), instanceKey: "key", invalid: true},
		{name: "sticky queue differs from stable identity", sticky: "other", options: singleSession("S"), instanceKey: "key", invalid: true},
		{name: "stable identity is the normal queue", sticky: "normal", options: singleSession("normal"), instanceKey: "key", invalid: true},
		{name: "single session without instance key", sticky: "S", options: singleSession("S"), invalid: true},
		{name: "single session on a build-ID versioned worker", sticky: "S", options: singleSession("S"), instanceKey: "key", buildIDs: true, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			taskQueue := &taskqueuepb.TaskQueue{Name: "normal", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
			if tc.sticky != "" {
				taskQueue = &taskqueuepb.TaskQueue{Name: tc.sticky, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: "normal"}
			}
			var versionCapabilities *commonpb.WorkerVersionCapabilities
			if tc.buildIDs {
				versionCapabilities = &commonpb.WorkerVersionCapabilities{BuildId: "build", UseVersioning: true}
			}
			err := validateWorkerSessionOptions(tc.options, taskQueue, tc.instanceKey, versionCapabilities)
			if tc.invalid {
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			} else {
				require.NoError(t, err)
			}
		})
	}
}
