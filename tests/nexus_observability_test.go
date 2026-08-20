package tests

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
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

func (s *NexusObservabilitySuite) newTestEnv(chasmEnabled bool, logger log.Logger) *NexusTestEnv {
	rolloutPercent := 0
	if chasmEnabled {
		rolloutPercent = 100
	}
	return newNexusTestEnv(s.T(), true,
		testcore.WithLogger(logger),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, chasmEnabled),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, chasmEnabled),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, chasmEnabled),
		testcore.WithDynamicConfig(chasmnexus.ChasmWorkflowOperationsRolloutPercent, rolloutPercent),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSignalBacklinks, chasmEnabled),
	)
}

func (s *NexusObservabilitySuite) TestStartFailureEmitsCorrelatableSignals(chasmEnabled bool) {
	testLogger := testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	testlogger.DontFailOnError(testLogger)
	env := s.newTestEnv(chasmEnabled, testLogger)

	ctx := s.Context()
	tv := env.Tv()
	callerTaskQueue := tv.TaskQueue().GetName()
	serviceName := tv.Service()
	operationName := tv.Operation()
	handlerRequests := make(chan nexusHandlerRequest, 1)
	handlerRelease := make(chan struct{}, 1)
	s.T().Cleanup(func() {
		select {
		case handlerRelease <- struct{}{}:
		default:
		}
	})
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), nexustest.Handler{
		OnStartOperation: func(handlerCtx context.Context, service, operation string, _ *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			select {
			case handlerRequests <- nexusHandlerRequest{service: service, operation: operation, requestID: options.RequestID}:
			case <-handlerCtx.Done():
				return nil, handlerCtx.Err()
			}
			select {
			case <-handlerRelease:
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional failure")
			case <-handlerCtx.Done():
				return nil, handlerCtx.Err()
			}
		},
	})

	callerWorkflow := func(ctx workflow.Context) error {
		nexusClient := workflow.NewNexusClient(endpointName, serviceName)
		return nexusClient.ExecuteOperation(ctx, operationName, nil, workflow.NexusOperationOptions{
			ScheduleToCloseTimeout: 3 * time.Second,
		}).Get(ctx, nil)
	}

	callerWorker := worker.New(env.SdkClient(), callerTaskQueue, worker.Options{})
	callerWorker.RegisterWorkflowWithOptions(callerWorkflow, workflow.RegisterOptions{Name: tv.WorkflowType().GetName()})
	s.NoError(callerWorker.Start())
	s.T().Cleanup(callerWorker.Stop)

	metricCapture := env.StartNamespaceMetricCapture()
	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        tv.WorkflowID(),
		TaskQueue: callerTaskQueue,
	}, tv.WorkflowType().GetName())
	s.NoError(err)

	var handlerRequest nexusHandlerRequest
	select {
	case handlerRequest = <-handlerRequests:
	case <-ctx.Done():
		s.FailNow("timed out waiting for the Nexus handler request")
	}
	s.Require().Equal(serviceName, handlerRequest.service)
	s.Require().Equal(operationName, handlerRequest.operation)
	s.Require().NotEmpty(handlerRequest.requestID)

	expectation := testLogger.Expect(testlogger.Error, "^"+regexp.QuoteMeta(historyStartFailureLog)+"$",
		tag.WorkflowNamespace("^"+regexp.QuoteMeta(env.Namespace().String())+"$"),
		tag.RequestID("^"+regexp.QuoteMeta(handlerRequest.requestID)+"$"),
	)
	select {
	case handlerRelease <- struct{}{}:
	case <-ctx.Done():
		s.FailNow("timed out releasing the Nexus handler")
	}

	s.Await(func(s *NexusObservabilitySuite) {
		s.Require().True(expectation.Matched())
	}, 10*time.Second, 100*time.Millisecond)
	s.Require().NotEmpty(metricCapture.Metric(chasmnexus.OutboundRequestCounter.Name()))
	s.Require().NotEmpty(metricCapture.Metric(chasmnexus.OutboundRequestLatency.Name()))

	s.NoError(env.SdkClient().TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "observability contract verified"))
}
