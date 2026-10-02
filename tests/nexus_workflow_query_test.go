package tests

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	apitemporalnexus "go.temporal.io/api/temporalnexus"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
)

func (s *NexusWorkflowTestSuite) TestNexusOperationBackedByQuery(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled)
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())

	queryType := "status"
	signalName := "done"
	handlerWorkflowID := testcore.RandomizeStr(s.T().Name() + "-handler")
	handlerWf := func(ctx workflow.Context) error {
		_ = workflow.SetQueryHandler(ctx, queryType, func() (string, error) {
			return "handler-status", nil
		})
		workflow.GetSignalChannel(ctx, signalName).Receive(ctx, nil)
		return nil
	}

	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			resp, err := env.FrontendClient().QueryWorkflow(ctx, &workflowservice.QueryWorkflowRequest{
				Namespace: env.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: handlerWorkflowID},
				Query:     &querypb.WorkflowQuery{QueryType: queryType},
			})
			if err != nil {
				return nil, err
			}
			// simulate the stitching that will be done by the SDKs eventually
			nexus.AddHandlerLinks(ctx, apitemporalnexus.ConvertLinkWorkflowToNexusLink(resp.GetLink().GetWorkflow()))

			var result string
			if err := payloads.Decode(resp.GetQueryResult(), &result); err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultSync[any]{Value: result}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	callerWF := func(ctx workflow.Context) (string, error) {
		c := workflow.NewNexusClient(endpointName, "service")
		fut := c.ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(callerWF)
	w.RegisterWorkflow(handlerWf)
	s.NoError(w.Start())
	defer w.Stop()

	handlerRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        handlerWorkflowID,
		TaskQueue: taskQueue,
	}, handlerWf)
	s.NoError(err)

	callerKinds := []string{"Workflow"}
	if chasmEnabled {
		callerKinds = append(callerKinds, "SANO")
	}
	for _, callerKind := range callerKinds {
		s.RunSequential(callerKind+": Queries on running workflows complete", func(s *NexusWorkflowTestSuite) {
			var result string
			var links []*commonpb.Link
			if callerKind == "SANO" {
				operationID := testcore.RandomizeStr(s.T().Name() + "-sano")
				startResp, err := env.startNexusOperation(
					ctx,
					&workflowservice.StartNexusOperationExecutionRequest{
						OperationId: operationID,
						Endpoint:    endpointName,
						Service:     "service",
						Operation:   "operation",
						Input:       testcore.MustToPayload(s.T(), "input"),
					},
				)
				s.NoError(err)
				s.True(startResp.GetStarted())
				s.Await(func(s *NexusWorkflowTestSuite) {
					descResp := env.describeNexusOperation(ctx, s.T(), operationID)
					s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
					s.NoError(payloads.Decode(&commonpb.Payloads{Payloads: []*commonpb.Payload{descResp.GetResult()}}, &result))
					links = descResp.GetInfo().GetLinks()
				}, 5*time.Second, 100*time.Millisecond)
			} else {
				callerRun, err := env.SdkClient().ExecuteWorkflow(
					ctx,
					client.StartWorkflowOptions{
						TaskQueue: taskQueue,
					},
					callerWF,
				)
				s.NoError(err)
				s.NoError(callerRun.Get(ctx, &result))

				hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: callerRun.GetID()})
				links = s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED).GetLinks()
			}
			s.Equal("handler-status", result)

			s.Require().Len(links, 1)
			workflowLink := links[0].GetWorkflow()
			s.NotNil(workflowLink, "caller must carry a link of type workflow")
			s.Equal(env.Namespace().String(), workflowLink.GetNamespace())
			s.Equal(handlerWorkflowID, workflowLink.GetWorkflowId())
			s.Equal(handlerRun.GetRunID(), workflowLink.GetRunId())
			s.Equal("Query processed", workflowLink.GetReason())
		})
	}

	s.RunSequential("Handler finishes cleanly", func(s *NexusWorkflowTestSuite) {
		s.NoError(env.SdkClient().SignalWorkflow(ctx, handlerWorkflowID, "", signalName, nil))
		s.NoError(handlerRun.Get(ctx, nil))
	})
}
