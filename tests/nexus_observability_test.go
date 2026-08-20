package tests

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/tests/testcore"
)

const historyStartFailureLog = "Nexus StartOperation request failed"

type nexusHandlerRequest struct {
	service   string
	operation string
	requestID string
}

type NexusObservabilitySuite struct {
	parallelsuite.Suite[*NexusObservabilitySuite]
}

func TestNexusObservabilitySuiteHSM(t *testing.T) {
	parallelsuite.Run(t, &NexusObservabilitySuite{}, false)
}

func TestNexusObservabilitySuiteCHASM(t *testing.T) {
	parallelsuite.Run(t, &NexusObservabilitySuite{}, true)
}

func (s *NexusObservabilitySuite) newTestEnv(chasmEnabled bool) *NexusTestEnv {
	rolloutPercent := 0
	if chasmEnabled {
		rolloutPercent = 100
	}
	return newNexusTestEnv(s.T(), true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, chasmEnabled),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, chasmEnabled),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, chasmEnabled),
		testcore.WithDynamicConfig(chasmnexus.ChasmWorkflowOperationsRolloutPercent, rolloutPercent),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSignalBacklinks, chasmEnabled),
	)
}

func (s *NexusObservabilitySuite) TestStartFailureEmitsCorrelatableSignals(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled)

	tv := env.Tv()
	handlerRequests := make(chan nexusHandlerRequest, 1)
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexustest.Handler{
		OnStartOperation: func(handlerCtx context.Context, service, operation string, _ *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			select {
			case handlerRequests <- nexusHandlerRequest{service: service, operation: operation, requestID: options.RequestID}:
			case <-handlerCtx.Done():
				return nil, handlerCtx.Err()
			}
			return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional failure")
		},
	})

	callerWorkflow := func(ctx workflow.Context) error {
		nexusClient := workflow.NewNexusClient(endpointName, tv.Service())
		return nexusClient.ExecuteOperation(ctx, tv.Operation(), nil, workflow.NexusOperationOptions{
			ScheduleToCloseTimeout: 3 * time.Second,
		}).Get(ctx, nil)
	}

	callerWorker := worker.New(env.SdkClient(), tv.TaskQueue().GetName(), worker.Options{})
	callerWorker.RegisterWorkflowWithOptions(callerWorkflow, workflow.RegisterOptions{Name: tv.WorkflowType().GetName()})
	s.NoError(callerWorker.Start())
	s.T().Cleanup(callerWorker.Stop)

	logCapture := env.StartLogCapture()
	metricCapture := env.StartNamespaceMetricCapture()
	_, err := env.SdkClient().ExecuteWorkflow(s.Context(), client.StartWorkflowOptions{
		ID:        tv.WorkflowID(),
		TaskQueue: tv.TaskQueue().GetName(),
	}, tv.WorkflowType().GetName())
	s.NoError(err)

	var handlerRequest nexusHandlerRequest
	select {
	case handlerRequest = <-handlerRequests:
	case <-s.Context().Done():
		s.FailNow("timed out waiting for the Nexus handler request")
	}
	s.Require().Equal(tv.Service(), handlerRequest.service)
	s.Require().Equal(tv.Operation(), handlerRequest.operation)
	s.Require().NotEmpty(handlerRequest.requestID)

	var failureLog *testlogger.CapturedLog
	s.Await(func(s *NexusObservabilitySuite) {
		records := logCapture.Snapshot()
		for i := range records {
			requestID, ok := records[i].TagValue(tag.RequestID("").Key())
			if records[i].Level == testlogger.Error &&
				records[i].Message == historyStartFailureLog &&
				ok && requestID == handlerRequest.requestID {
				failureLog = &records[i]
				return
			}
		}
		s.Require().Fail("Nexus StartOperation failure log not found")
	}, 10*time.Second, 100*time.Millisecond)
	namespace, ok := failureLog.TagValue(tag.WorkflowNamespace("").Key())
	s.Require().True(ok)
	s.Require().Equal(env.Namespace().String(), namespace)
	s.Require().NotEmpty(metricCapture.Metric(chasmnexus.OutboundRequestCounter.Name()))
	s.Require().NotEmpty(metricCapture.Metric(chasmnexus.OutboundRequestLatency.Name()))
}
