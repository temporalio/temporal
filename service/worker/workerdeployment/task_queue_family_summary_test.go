package workerdeployment

import (
	"testing"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/worker_versioning"
)

func TestBuildTaskQueueFamilySummary(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name              string
		taskQueueFamilies map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData
		wantCount         int32
	}{
		{
			name: "families",
			taskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
				"queue-a": {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
						int32(enumspb.TASK_QUEUE_TYPE_NEXUS):    {},
					},
				},
				"queue-b": {
					TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
						int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY): {},
					},
				},
			},
			wantCount: 2,
		},
		{
			name: "no families",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			summary := buildTaskQueueFamilySummary(tc.taskQueueFamilies)
			require.Equal(t, tc.wantCount, summary.GetCount())
			if tc.wantCount == 0 {
				require.Zero(t, summary.GetBloomFilterSize())
				require.Zero(t, summary.GetBloomFilterHashCount())
				require.Empty(t, summary.GetBloomFilterWords())
				return
			}

			require.NotZero(t, summary.GetBloomFilterSize())
			require.NotZero(t, summary.GetBloomFilterHashCount())
			require.NotEmpty(t, summary.GetBloomFilterWords())
			words := make([]uint64, len(summary.GetBloomFilterWords()))
			for index, word := range summary.GetBloomFilterWords() {
				words[index] = uint64(word)
			}
			filter := bloom.FromWithM(words, uint(summary.GetBloomFilterSize()), uint(summary.GetBloomFilterHashCount()))
			require.True(t, filter.TestString("queue-a"))
			require.True(t, filter.TestString("queue-b"))
			require.False(t, filter.TestString("queue-c"))
		})
	}
}

func TestVersionStateToSummaryTaskQueueFamilySummary(t *testing.T) {
	t.Parallel()

	state := &deploymentspb.VersionLocalState{
		Version: &deploymentspb.WorkerDeploymentVersion{
			DeploymentName: "deployment",
			BuildId:        "build-id",
		},
		TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
			"queue": {},
		},
	}

	testCases := []struct {
		name            string
		workflowVersion DeploymentWorkflowVersion
		wantSummary     bool
	}{
		{
			name:            "v2 omits summary",
			workflowVersion: VersionDataRevisionNumber,
		},
		{
			name:            "v3 includes summary",
			workflowVersion: TaskQueueFamilySummary,
			wantSummary:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			summary := versionStateToSummary(
				state,
				tc.workflowVersion >= TaskQueueFamilySummary,
			).GetTaskQueueFamilySummary()
			if !tc.wantSummary {
				require.Nil(t, summary)
				return
			}
			require.Equal(t, int32(1), summary.GetCount())
		})
	}
}

func TestValidateRegisterWorkerTaskQueueFamilySummary(t *testing.T) {
	t.Parallel()

	version := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: "deployment",
		BuildId:        "build-id",
	}
	versionString := worker_versioning.WorkerDeploymentVersionToStringV31(version)
	completeSummary := buildTaskQueueFamilySummary(map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
		"existing-queue": {},
	})

	testCases := []struct {
		name            string
		workflowVersion DeploymentWorkflowVersion
		summary         *deploymentspb.TaskQueueFamilySummary
		taskQueueName   string
		maxTaskQueues   int32
		wantLimitError  bool
		wantBloomPass   bool
	}{
		{
			name:            "definite miss at limit",
			workflowVersion: TaskQueueFamilySummary,
			summary:         completeSummary,
			taskQueueName:   "new-queue",
			maxTaskQueues:   1,
			wantLimitError:  true,
		},
		{
			name:            "existing family at limit",
			workflowVersion: TaskQueueFamilySummary,
			summary:         completeSummary,
			taskQueueName:   "existing-queue",
			maxTaskQueues:   1,
			wantBloomPass:   true,
		},
		{
			name:            "below limit",
			workflowVersion: TaskQueueFamilySummary,
			summary:         completeSummary,
			taskQueueName:   "new-queue",
			maxTaskQueues:   2,
		},
		{
			name:            "missing summary fails open",
			workflowVersion: TaskQueueFamilySummary,
			taskQueueName:   "new-queue",
			maxTaskQueues:   1,
		},
		{
			name:            "old workflow version fails open",
			workflowVersion: VersionDataRevisionNumber,
			summary:         completeSummary,
			taskQueueName:   "new-queue",
			maxTaskQueues:   1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runner := &WorkflowRunner{
				WorkerDeploymentWorkflowArgs: &deploymentspb.WorkerDeploymentWorkflowArgs{
					State: &deploymentspb.WorkerDeploymentLocalState{
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
							versionString: {TaskQueueFamilySummary: tc.summary},
						},
					},
				},
				metrics:         sdkclient.MetricsNopHandler,
				workflowVersion: tc.workflowVersion,
			}
			bloomFilterPassed, err := runner.validateRegisterWorkerWithBloomFilterResult(&deploymentspb.RegisterWorkerInWorkerDeploymentArgs{
				TaskQueueName: tc.taskQueueName,
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_NEXUS,
				MaxTaskQueues: tc.maxTaskQueues,
				Version:       version,
			})
			require.Equal(t, tc.wantBloomPass, bloomFilterPassed)

			if !tc.wantLimitError {
				require.NoError(t, err)
				return
			}
			var applicationError *temporal.ApplicationError
			require.ErrorAs(t, err, &applicationError)
			require.Equal(t, errMaxTaskQueuesInVersionType, applicationError.Type())
		})
	}
}
