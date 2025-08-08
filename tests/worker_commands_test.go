package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/dgryski/go-farm"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/frontend"
	"go.temporal.io/server/tests/testcore"
)

type WorkerCommandsSuite struct {
	NexusTestBaseSuite
}

func TestWorkerCommandsSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkerCommandsSuite))
}

func (s *WorkerCommandsSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.WorkerHeartbeatsEnabled.Key(): true,
		dynamicconfig.WorkerCommandsEnabled.Key():   true,
		dynamicconfig.ListWorkersEnabled.Key():      true,
	}
	s.NexusTestBaseSuite.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func stringHash(str string) string {
	return fmt.Sprintf("%08x", farm.Fingerprint32([]byte(str)))
}

func (s *WorkerCommandsSuite) prepareWorker(ctx context.Context, workerInstanceKey, processKey string) {
	wh := &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: s.Namespace().String(),
		Identity:  "test-identity",
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{
				WorkerInstanceKey: workerInstanceKey,
				Status:            enumspb.WORKER_STATUS_RUNNING,
				HostInfo: &workerpb.WorkerHostInfo{
					ProcessKey: processKey,
				},
			},
		},
	}

	// Record the Worker Heartbeat
	_, err := s.GetTestCluster().FrontendClient().RecordWorkerHeartbeat(ctx, wh)
	s.NoError(err)

	// Verify that the Worker Heartbeat is recorded
	response, err := s.GetTestCluster().FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
		Namespace: s.Namespace().String(),
		Query:     fmt.Sprintf("WorkerInstanceKey='%s'", workerInstanceKey),
	})

	s.NoError(err)
	s.Len(response.GetWorkersInfo(), 1, "Expected one worker heartbeat to be recorded")
	s.Equal(response.GetWorkersInfo()[0].WorkerHeartbeat.WorkerInstanceKey, workerInstanceKey, "WorkerInstanceKey should match")
}

func prepareWorkerConfigEntry(workerInstanceKey string) *workerpb.WorkerConfigEntry {
	workflowConfig := &workerpb.WorkerConfigEntry{
		WorkerInstanceKey: workerInstanceKey,
		WorkerConfig: &sdkpb.WorkerConfig{
			WorkflowCacheSize: 1,
			PollerBehavior: &sdkpb.WorkerConfig_SimplePollerBehavior_{
				SimplePollerBehavior: &sdkpb.WorkerConfig_SimplePollerBehavior{
					MaxPollers: 1,
				},
			},
		},
	}
	return workflowConfig
}

func (s *WorkerCommandsSuite) TestFetchWorkerConfig() {
	ctx := testcore.NewContext()
	processKey := fmt.Sprintf("process-%s-%s", s.T().Name(), stringHash(uuid.New()))
	workerInstanceKey := fmt.Sprintf("worker-%s-%s", s.T().Name(), stringHash(uuid.New()))
	workerTaskQueue := frontend.GetWorkerSysTaskQueue(s.Namespace().String(), processKey)

	/*
		1. Record Worker Heartbeat.
		2. Verify that the Worker Heartbeat is recorded.
		3. Start nexus polling task.
		4. Fetch Worker Config.
		5. Verify the Worker Config.
	*/

	s.prepareWorker(ctx, workerInstanceKey, processKey)

	// prepare Worker Config
	workflowConfig := prepareWorkerConfigEntry(workerInstanceKey)
	responsePayloads, err := sdk.PreferProtoDataConverter.ToPayloads(&workerpb.FetchWorkerConfigResponsePayload{
		WorkerConfigs: []*workerpb.WorkerConfigEntry{workflowConfig},
	})
	s.NoError(err)

	// Start nexus polling task
	go s.nexusTaskPoller(ctx, workerTaskQueue, func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
		return &nexuspb.Response{
			Variant: &nexuspb.Response_StartOperation{
				StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_SyncSuccess{
						SyncSuccess: &nexuspb.StartOperationResponse_Sync{
							Payload: responsePayloads.Payloads[0],
						},
					},
				},
			},
		}, nil
	})

	// Fetch Worker Config
	wcResponse, err := s.GetTestCluster().FrontendClient().FetchWorkerConfig(ctx, &workflowservice.FetchWorkerConfigRequest{
		Namespace: s.Namespace().String(),
		Selector: &commonpb.WorkerSelector{
			Selector: &commonpb.WorkerSelector_WorkerInstanceKey{
				WorkerInstanceKey: workerInstanceKey,
			},
		},
	})

	s.NoError(err)
	s.NotNil(wcResponse)
	s.NotNil(wcResponse.WorkerConfig)
	s.Equal(int32(1), wcResponse.WorkerConfig.WorkflowCacheSize)
}

func (s *WorkerCommandsSuite) TestUpdateWorkerConfig() {
	ctx := testcore.NewContext()
	processKey := fmt.Sprintf("process-%s-%s", s.T().Name(), stringHash(uuid.New()))
	workerInstanceKey := fmt.Sprintf("worker-%s-%s", s.T().Name(), stringHash(uuid.New()))
	workerTaskQueue := frontend.GetWorkerSysTaskQueue(s.Namespace().String(), processKey)

	/*
		1. Record Worker Heartbeat.
		2. Verify that the Worker Heartbeat is recorded.
		3. Start nexus polling task.
		4. Update Worker Config.
		5. Verify the updated Worker Config.
	*/
	s.prepareWorker(ctx, workerInstanceKey, processKey)

	// prepare Worker Config
	workflowConfig := prepareWorkerConfigEntry(workerInstanceKey)
	responsePayloads, err := sdk.PreferProtoDataConverter.ToPayloads(&workerpb.UpdateWorkerConfigResponsePayload{
		WorkerConfigs: []*workerpb.WorkerConfigEntry{workflowConfig},
	})
	s.NoError(err)

	// Start nexus polling task
	go s.nexusTaskPoller(ctx, workerTaskQueue, func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
		return &nexuspb.Response{
			Variant: &nexuspb.Response_StartOperation{
				StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_SyncSuccess{
						SyncSuccess: &nexuspb.StartOperationResponse_Sync{
							Payload: responsePayloads.Payloads[0],
						},
					},
				},
			},
		}, nil
	})

	// Update Worker Config
	wcResponse, err := s.GetTestCluster().FrontendClient().UpdateWorkerConfig(ctx, &workflowservice.UpdateWorkerConfigRequest{
		Namespace: s.Namespace().String(),
		WorkerConfig: &sdkpb.WorkerConfig{
			WorkflowCacheSize: 10,
		},
		Selector: &commonpb.WorkerSelector{
			Selector: &commonpb.WorkerSelector_WorkerInstanceKey{
				WorkerInstanceKey: workerInstanceKey,
			},
		},
	})

	// Verify the result
	s.NoError(err)
	s.NotNil(wcResponse)
	s.NotNil(wcResponse.GetWorkerConfig())
	s.Equal(int32(1), wcResponse.GetWorkerConfig().WorkflowCacheSize)
}
