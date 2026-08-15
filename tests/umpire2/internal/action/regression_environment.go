package action

import (
	"context"
	"errors"
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/nexus/nexustest"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
)

func (p *regressionPath) SetupResource(ctx context.Context, resource coreregress.CompletedResource) (coreregress.Cleanup, error) {
	switch resource.Realization {
	case RegressionResourceNamespace, RegressionResourceTaskQueue:
		return nil, nil
	case RegressionResourceWorker:
		return p.setupWorker()
	case RegressionResourceNexusEndpoint:
		return p.setupEndpoint(ctx)
	case RegressionResourceFaultInjector:
		if p.environment.GetFaultInjector() == nil {
			return nil, errors.New("fault injector is unavailable")
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported regression resource realization %q", resource.Realization)
	}
}

func (p *regressionPath) setupEndpoint(ctx context.Context) (coreregress.Cleanup, error) {
	sequence := regressionEndpointSequence.Add(1)
	endpointName := fmt.Sprintf("umpire-regress-%d-%d", p.index, sequence)
	var target *nexuspb.EndpointTarget
	if p.usesHandlerWorkflow() {
		target = &nexuspb.EndpointTarget{
			Variant: &nexuspb.EndpointTarget_Worker_{
				Worker: &nexuspb.EndpointTarget_Worker{
					Namespace: p.environment.Namespace().String(),
					TaskQueue: p.taskQueue,
				},
			},
		}
	} else {
		listenAddress := nexustest.AllocListenAddress()
		p.environment.StartNexusServer(listenAddress, p.policy.Handler())
		target = &nexuspb.EndpointTarget{
			Variant: &nexuspb.EndpointTarget_External_{
				External: &nexuspb.EndpointTarget_External{Url: "http://" + listenAddress},
			},
		}
	}
	response, err := p.environment.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name:   endpointName,
			Target: target,
		},
	})
	if err != nil {
		return nil, err
	}
	p.context.Endpoint = endpointName
	return func(cleanupContext context.Context) error {
		_, cleanupErr := p.environment.OperatorClient().DeleteNexusEndpoint(cleanupContext, &operatorservice.DeleteNexusEndpointRequest{
			Id:      response.Endpoint.Id,
			Version: response.Endpoint.Version,
		})
		return cleanupErr
	}, nil
}

func (p *regressionPath) setupWorker() (coreregress.Cleanup, error) {
	if !p.usesHandlerWorkflow() && !p.usesEmbeddedCaller() {
		return nil, nil
	}
	p.worker = sdkworker.New(p.environment.SdkClient(), p.taskQueue, sdkworker.Options{})
	if p.usesHandlerWorkflow() {
		service := nexus.NewService("service")
		operation := temporalnexus.NewWorkflowRunOperation(
			"operation",
			regressionHandlerWorkflow,
			func(context.Context, nexus.NoValue, nexus.StartOperationOptions) (sdkclient.StartWorkflowOptions, error) {
				return sdkclient.StartWorkflowOptions{
					ID:                       p.handlerID,
					WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
				}, nil
			},
		)
		if err := service.Register(operation); err != nil {
			return nil, err
		}
		p.worker.RegisterNexusService(service)
		p.worker.RegisterWorkflow(regressionHandlerWorkflow)
		p.worker.RegisterWorkflow(regressionSharedCallerWorkflow)
	}
	if p.usesEmbeddedCaller() {
		p.worker.RegisterWorkflow(regressionCallerWorkflow)
		p.worker.RegisterWorkflow(regressionTimeoutCallerWorkflow)
	}
	if err := p.worker.Start(); err != nil {
		return nil, err
	}
	return func(context.Context) error {
		p.worker.Stop()
		return nil
	}, nil
}

func regressionHandlerWorkflow(ctx workflow.Context, _ nexus.NoValue) (string, error) {
	workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
	return "ok", nil
}

func regressionCallerWorkflow(ctx workflow.Context, endpoint string) error {
	operation := workflow.NewNexusClient(endpoint, "service").ExecuteOperation(ctx, "operation", nil, workflow.NexusOperationOptions{})
	return operation.GetNexusOperationExecution().Get(ctx, nil)
}

func regressionTimeoutCallerWorkflow(ctx workflow.Context, input regressionTimeoutCallerInput) error {
	operation := workflow.NewNexusClient(input.Endpoint, "service").ExecuteOperation(ctx, "operation", nil, input.Options)
	return operation.Get(ctx, nil)
}

type regressionTimeoutCallerInput struct {
	Endpoint string
	Options  workflow.NexusOperationOptions
}

func regressionSharedCallerWorkflow(ctx workflow.Context, endpoint string) error {
	operation := workflow.NewNexusClient(endpoint, "service").ExecuteOperation(ctx, "operation", nil, workflow.NexusOperationOptions{})
	return operation.Get(ctx, nil)
}

func (p *regressionPath) usesHandlerWorkflow() bool {
	return pathHasRealization(p.path, RegressionNexusStartNewHandler, RegressionNexusStartAttachHandler)
}

func (p *regressionPath) usesEmbeddedCaller() bool {
	return pathHasRealization(p.path, RegressionNexusScheduleEmbedded, RegressionNexusSchedule)
}

func pathHasRealization(path coreregress.CompletedPath, realizations ...string) bool {
	for _, action := range path.Actions {
		for _, realization := range realizations {
			if action.Realization == realization {
				return true
			}
		}
	}
	return false
}
