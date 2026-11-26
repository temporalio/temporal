package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sessionpb "go.temporal.io/api/session/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
)

func TestValidateWorkerSessionOptions(t *testing.T) {
	t.Parallel()
	multiSession := &sessionpb.WorkerSessionOptions{
		Admission: &sessionpb.WorkerSessionOptions_MultiSession{MultiSession: &sessionpb.MultiSessionOptions{}},
	}
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
		versioned   bool // the worker polls with worker versioning
		buildIDs    bool // the worker polls with old build-ID versioning
		invalid     bool
	}{
		{name: "regular poll"},
		{name: "ordinary sticky poll", sticky: "queue"},
		{name: "multi session on normal queue", options: multiSession, instanceKey: "key"},
		{name: "multi session on its sticky queue", sticky: "key", options: multiSession, instanceKey: "key"},
		{name: "multi session sticky queue differs from instance key", sticky: "other", options: multiSession, instanceKey: "key", invalid: true},
		{name: "multi session instance key is the normal queue", options: multiSession, instanceKey: "normal", invalid: true},
		{name: "multi session without instance key", options: multiSession, invalid: true},
		{name: "single session", sticky: "S", options: singleSession("S"), instanceKey: "key"},
		{name: "single session on normal queue", options: singleSession("S"), instanceKey: "key", invalid: true},
		{name: "empty stable identity", sticky: "S", options: singleSession(""), instanceKey: "key", invalid: true},
		{name: "sticky queue differs from stable identity", sticky: "other", options: singleSession("S"), instanceKey: "key", invalid: true},
		{name: "stable identity is the normal queue", sticky: "normal", options: singleSession("normal"), instanceKey: "key", invalid: true},
		{name: "single session without instance key", sticky: "S", options: singleSession("S"), invalid: true},
		{name: "options without an admission mode", sticky: "queue", options: &sessionpb.WorkerSessionOptions{}, instanceKey: "key", invalid: true},
		{name: "multi session on a versioned worker", options: multiSession, instanceKey: "key", versioned: true, invalid: true},
		{name: "single session on a versioned worker", sticky: "S", options: singleSession("S"), instanceKey: "key", versioned: true},
		{name: "multi session on a build-ID versioned worker", options: multiSession, instanceKey: "key", buildIDs: true, invalid: true},
		{name: "single session on a build-ID versioned worker", sticky: "S", options: singleSession("S"), instanceKey: "key", buildIDs: true, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			taskQueue := &taskqueuepb.TaskQueue{Name: "normal", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
			if tc.sticky != "" {
				taskQueue = &taskqueuepb.TaskQueue{Name: tc.sticky, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: "normal"}
			}
			var deploymentOptions *deploymentpb.WorkerDeploymentOptions
			if tc.versioned {
				deploymentOptions = &deploymentpb.WorkerDeploymentOptions{
					DeploymentName: "deployment", BuildId: "build", WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
				}
			}
			var versionCapabilities *commonpb.WorkerVersionCapabilities
			if tc.buildIDs {
				versionCapabilities = &commonpb.WorkerVersionCapabilities{BuildId: "build", UseVersioning: true}
			}
			err := validateWorkerSessionOptions(tc.options, taskQueue, tc.instanceKey, deploymentOptions, versionCapabilities)
			if tc.invalid {
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			} else {
				require.NoError(t, err)
			}
		})
	}
}
