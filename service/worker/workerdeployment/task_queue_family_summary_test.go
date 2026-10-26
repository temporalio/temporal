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

func TestMaxTaskQueuesInVersionErrorIncludesTaskQueueFamilySummary(t *testing.T) {
	t.Parallel()

	runner := &VersionWorkflowRunner{
		WorkerDeploymentVersionWorkflowArgs: &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
			VersionState: &deploymentspb.VersionLocalState{
				TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
					"existing-queue": {},
				},
			},
		},
	}

	err := runner.validateRegisterWorker(&deploymentspb.RegisterWorkerInVersionArgs{
		TaskQueueName: "new-queue",
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		MaxTaskQueues: 1,
	})

	var applicationError *temporal.ApplicationError
	require.ErrorAs(t, err, &applicationError)
	require.Equal(t, errMaxTaskQueuesInVersionType, applicationError.Type())
	var details *deploymentspb.MaxTaskQueuesInVersionFailureDetails
	require.NoError(t, applicationError.Details(&details))
	summary := details.GetTaskQueueFamilySummary()
	require.Equal(t, int32(1), summary.GetCount())
	require.True(t, taskQueueFamilyMayExist(summary, "existing-queue"))
}

func TestCacheTaskQueueFamilySummaryFromError(t *testing.T) {
	t.Parallel()

	version := "deployment.build-id"
	summary := buildTaskQueueFamilySummary(map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
		"existing-queue": {},
	})
	runner := &WorkflowRunner{
		WorkerDeploymentWorkflowArgs: &deploymentspb.WorkerDeploymentWorkflowArgs{
			State: &deploymentspb.WorkerDeploymentLocalState{
				Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
					version: {Version: version},
				},
			},
		},
	}
	err := temporal.NewApplicationError(
		"task queue limit reached",
		errMaxTaskQueuesInVersionType,
		&deploymentspb.MaxTaskQueuesInVersionFailureDetails{TaskQueueFamilySummary: summary},
	)

	runner.cacheTaskQueueFamilySummaryFromError(version, err)

	require.Same(t, summary, runner.State.Versions[version].GetTaskQueueFamilySummary())
}

func TestUpdateVersionSummaryPreservesTaskQueueFamilySummary(t *testing.T) {
	t.Parallel()

	version := "deployment.build-id"
	summary := buildTaskQueueFamilySummary(map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
		"existing-queue": {},
	})
	runner := &WorkflowRunner{
		WorkerDeploymentWorkflowArgs: &deploymentspb.WorkerDeploymentWorkflowArgs{
			State: &deploymentspb.WorkerDeploymentLocalState{
				Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
					version: {
						Version:                version,
						TaskQueueFamilySummary: summary,
					},
				},
			},
		},
	}

	runner.updateVersionSummary(&deploymentspb.WorkerDeploymentVersionSummary{Version: version})

	require.Same(t, summary, runner.State.Versions[version].GetTaskQueueFamilySummary())
}

func TestInvalidateTaskQueueFamilySummaryBelowLimit(t *testing.T) {
	t.Parallel()

	version := "deployment.build-id"
	summary := buildTaskQueueFamilySummary(map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
		"existing-queue": {},
	})
	testCases := []struct {
		name          string
		maxTaskQueues int32
		wantSummary   bool
	}{
		{
			name:          "same limit preserves summary",
			maxTaskQueues: 1,
			wantSummary:   true,
		},
		{
			name:          "increased limit invalidates summary",
			maxTaskQueues: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runner := &WorkflowRunner{
				WorkerDeploymentWorkflowArgs: &deploymentspb.WorkerDeploymentWorkflowArgs{
					State: &deploymentspb.WorkerDeploymentLocalState{
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionSummary{
							version: {
								Version:                version,
								TaskQueueFamilySummary: summary,
							},
						},
					},
				},
			}

			runner.invalidateTaskQueueFamilySummaryBelowLimit(version, tc.maxTaskQueues)

			require.Equal(t, tc.wantSummary, runner.State.Versions[version].GetTaskQueueFamilySummary() != nil)
		})
	}
}

func TestVersionStateToSummaryOmitsTaskQueueFamilySummary(t *testing.T) {
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

	require.Nil(t, versionStateToSummary(state).GetTaskQueueFamilySummary())
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
		name                   string
		summary                *deploymentspb.TaskQueueFamilySummary
		taskQueueName          string
		maxTaskQueues          int32
		wantLimitError         bool
		wantBloomFilterMatched bool
	}{
		{
			name:           "definite miss at limit",
			summary:        completeSummary,
			taskQueueName:  "new-queue",
			maxTaskQueues:  1,
			wantLimitError: true,
		},
		{
			name:                   "existing family at limit",
			summary:                completeSummary,
			taskQueueName:          "existing-queue",
			maxTaskQueues:          1,
			wantBloomFilterMatched: true,
		},
		{
			name:          "below limit",
			summary:       completeSummary,
			taskQueueName: "new-queue",
			maxTaskQueues: 2,
		},
		{
			name:          "missing summary fails open",
			taskQueueName: "new-queue",
			maxTaskQueues: 1,
		},
		{
			name: "zero bloom filter size fails open",
			summary: &deploymentspb.TaskQueueFamilySummary{
				Count:                1,
				BloomFilterHashCount: 1,
				BloomFilterWords:     []int64{1},
			},
			taskQueueName: "new-queue",
			maxTaskQueues: 1,
		},
		{
			name: "zero bloom filter hash count fails open",
			summary: &deploymentspb.TaskQueueFamilySummary{
				Count:            1,
				BloomFilterSize:  64,
				BloomFilterWords: []int64{1},
			},
			taskQueueName: "new-queue",
			maxTaskQueues: 1,
		},
		{
			name: "missing bloom filter words fails open",
			summary: &deploymentspb.TaskQueueFamilySummary{
				Count:                1,
				BloomFilterSize:      64,
				BloomFilterHashCount: 1,
			},
			taskQueueName: "new-queue",
			maxTaskQueues: 1,
		},
		{
			name: "inconsistent bloom filter metadata fails open",
			summary: &deploymentspb.TaskQueueFamilySummary{
				Count:                completeSummary.GetCount(),
				BloomFilterSize:      completeSummary.GetBloomFilterSize() + 1,
				BloomFilterHashCount: completeSummary.GetBloomFilterHashCount(),
				BloomFilterWords:     completeSummary.GetBloomFilterWords(),
			},
			taskQueueName: "new-queue",
			maxTaskQueues: 1,
		},
		{
			name: "inconsistent bloom filter hash count fails open",
			summary: &deploymentspb.TaskQueueFamilySummary{
				Count:                completeSummary.GetCount(),
				BloomFilterSize:      completeSummary.GetBloomFilterSize(),
				BloomFilterHashCount: completeSummary.GetBloomFilterHashCount() + 1,
				BloomFilterWords:     completeSummary.GetBloomFilterWords(),
			},
			taskQueueName: "new-queue",
			maxTaskQueues: 1,
		},
		{
			name: "negative bloom filter size fails open",
			summary: &deploymentspb.TaskQueueFamilySummary{
				Count:                1,
				BloomFilterSize:      -1,
				BloomFilterHashCount: 1,
				BloomFilterWords:     []int64{1},
			},
			taskQueueName: "new-queue",
			maxTaskQueues: 1,
		},
		{
			name: "negative bloom filter hash count fails open",
			summary: &deploymentspb.TaskQueueFamilySummary{
				Count:                1,
				BloomFilterSize:      64,
				BloomFilterHashCount: -1,
				BloomFilterWords:     []int64{0},
			},
			taskQueueName: "new-queue",
			maxTaskQueues: 1,
		},
		{
			name: "inconsistent bloom filter word count fails open",
			summary: &deploymentspb.TaskQueueFamilySummary{
				Count:                completeSummary.GetCount(),
				BloomFilterSize:      completeSummary.GetBloomFilterSize(),
				BloomFilterHashCount: completeSummary.GetBloomFilterHashCount(),
				BloomFilterWords:     []int64{0, 0},
			},
			taskQueueName: "new-queue",
			maxTaskQueues: 1,
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
				metrics: sdkclient.MetricsNopHandler,
			}
			bloomFilterMatched, err := runner.validateRegisterWorkerWithBloomFilterResult(&deploymentspb.RegisterWorkerInWorkerDeploymentArgs{
				TaskQueueName: tc.taskQueueName,
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_NEXUS,
				MaxTaskQueues: tc.maxTaskQueues,
				Version:       version,
			})
			require.Equal(t, tc.wantBloomFilterMatched, bloomFilterMatched)

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
