package action

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payloads"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	regressnexus "go.temporal.io/server/tests/umpire2/regress/nexus"
	"google.golang.org/protobuf/types/known/durationpb"
)

// RegressionEnvironment adds endpoint administration to the existing live action environment.
type RegressionEnvironment interface {
	Environment
	AdminClient() adminservice.AdminServiceClient
	OperatorClient() operatorservice.OperatorServiceClient
	StartNexusServer(string, nexus.Handler)
}

// RegressionEnvironmentFactory creates the isolated environment used by one completed path.
type RegressionEnvironmentFactory func(context.Context, int) (RegressionEnvironment, coreregress.Cleanup, error)

// RegressionHarness realizes compiled sparse paths against Temporal functional-test environments.
type RegressionHarness struct {
	factory            RegressionEnvironmentFactory
	sink               coreregress.ArtifactSink
	environmentProfile umpirefw.EnvironmentProfile
	modelVersion       string
}

func NewRegressionHarness(factory RegressionEnvironmentFactory, sink coreregress.ArtifactSink) *RegressionHarness {
	return &RegressionHarness{factory: factory, sink: sink}
}

func (h *RegressionHarness) ArtifactSink() coreregress.ArtifactSink { return h.sink }

func (h *RegressionHarness) NewPath(ctx context.Context, index int, path coreregress.CompletedPath) (coreregress.PathHarness, error) {
	if h.factory == nil {
		return nil, errors.New("regression environment factory is nil")
	}
	environment, cleanup, err := h.factory(ctx, index)
	if err != nil {
		return nil, errors.Join(err, cleanupRegressionEnvironment(ctx, cleanup))
	}
	if environment == nil {
		return nil, errors.Join(errors.New("regression environment factory returned nil"), cleanupRegressionEnvironment(ctx, cleanup))
	}
	policy := NewResponsePolicy()
	sequence := regressionEndpointSequence.Add(1)
	return &regressionPath{
		index:              index,
		path:               path,
		environment:        environment,
		context:            NewCtx(environment, "", policy, index),
		policy:             policy,
		cleanup:            cleanup,
		localFacts:         map[string]bool{},
		activityRuns:       map[string]string{},
		activityOps:        map[string]string{},
		environmentProfile: h.environmentProfile,
		modelVersion:       h.modelVersion,
		taskQueue:          fmt.Sprintf("umpire-regress-%d-%d", index, sequence),
		handlerID:          fmt.Sprintf("umpire-regress-handler-%d-%d", index, sequence),
	}, nil
}

func cleanupRegressionEnvironment(ctx context.Context, cleanup coreregress.Cleanup) error {
	if cleanup == nil {
		return nil
	}
	if err := cleanup(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("cleanup partially allocated regression environment: %w", err)
	}
	return nil
}

type regressionPath struct {
	index              int
	path               coreregress.CompletedPath
	environment        RegressionEnvironment
	context            *Ctx
	policy             *ResponsePolicy
	cleanup            coreregress.Cleanup
	localFacts         map[string]bool
	taskQueue          string
	handlerID          string
	callerID           string
	worker             sdkworker.Worker
	activityRuns       map[string]string
	activityOps        map[string]string
	mu                 sync.RWMutex
	environmentProfile umpirefw.EnvironmentProfile
	modelVersion       string
}

func (p *regressionPath) ExecutionObserver() umpirefw.ExecutionObserver { return p }

func (p *regressionPath) ObserveExecution(ctx context.Context, observed umpirefw.ExecutionObservation) error {
	observer, ok := p.environment.GetMonitor().(umpirefw.ExecutionObserver)
	if !ok {
		return nil
	}
	observed.Scope = p.environment.NamespaceID().String()
	return observer.ObserveExecution(ctx, observed)
}

var regressionEndpointSequence atomic.Uint64

func (p *regressionPath) InstallAction(_ context.Context, step coreregress.CompletedStep, _ coreregress.Bindings) (coreregress.Cleanup, error) {
	switch step.Action.Realization {
	case RegressionNexusRespondStartScheduledAsync:
		p.policy.setStart(&nexus.HandlerStartOperationResultAsync{OperationToken: "umpire-regress-token"}, nil)
	case RegressionNexusRespondStartCompletionPendingAsync:
		p.policy.setDeferredStart(&nexus.HandlerStartOperationResultAsync{OperationToken: "umpire-regress-token"}, nil)
	case RegressionNexusRespondStartScheduledSync:
		p.policy.setStart(&nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil)
		p.policy.setHandlerLinks(commonnexus.ConvertLinkWorkflowEventToNexusLink(&commonpb.Link_WorkflowEvent{
			Namespace:  "umpire-regression",
			WorkflowId: "handler",
			RunId:      "handler-run",
			Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
				RequestId: "handler-start",
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			}},
		}))
	case RegressionNexusScheduleDefault, RegressionNexusScheduleEmbedded, RegressionNexusSchedule,
		RegressionNexusCompleteScheduled, RegressionNexusCompleteStarted, RegressionNexusCompleteCallbackFailed,
		RegressionNexusCancel, RegressionNexusCancelWithRetry, RegressionNexusTimeout, RegressionNexusStartNewHandler, RegressionNexusStartAttachHandler,
		RegressionNexusCompleteFromHandler, RegressionWorkflowComplete, RegressionWorkflowObserveRunID, RegressionObserve:
		// Proactive realizations perform their work in Fire.
		// Observation realizations perform their work in Reconcile.
	case RegressionNexusStartActivity:
		p.policy.setStartHook(func(ctx context.Context, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			return p.startActivityFromHandler(ctx, step.Action, options)
		})
	default:
		return nil, fmt.Errorf("unsupported regression action realization %q", step.Action.Realization)
	}
	return nil, nil
}

func (p *regressionPath) ArmPolicy(_ context.Context, policy coreregress.CompletedPolicy, _ coreregress.Bindings) (coreregress.Cleanup, error) {
	if policy.Realization != RegressionPolicyNexusDrop && policy.Realization != RegressionPolicyNexusFailNext {
		return nil, fmt.Errorf("unsupported regression policy realization %q", policy.Realization)
	}
	if len(policy.Arguments) != 1 || !policy.Arguments[0].Literal {
		return nil, fmt.Errorf("policy %s requires one RPC literal", policy.Name)
	}
	method := fmt.Sprint(policy.Arguments[0].Value)
	if policy.Realization == RegressionPolicyNexusFailNext && method == string(regressnexus.CancelNexusOperation) {
		p.policy.setNextCancelError(nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "umpire regression: injected cancellation rejection"))
		return func(context.Context) error {
			p.policy.setNextCancelError(nil)
			return nil
		}, nil
	}
	transient := policy.Realization == RegressionPolicyNexusFailNext
	cleanup, err := armRegressionFault(p.context, method, transient)
	if err != nil {
		return nil, err
	}
	return func(context.Context) error {
		cleanup()
		return nil
	}, nil
}

func (p *regressionPath) Await(ctx context.Context, atoms []coreregress.CompletedAtom, bindings coreregress.Bindings) error {
	return p.awaitAtoms(ctx, atoms, bindings, false)
}

func (p *regressionPath) Fire(ctx context.Context, step coreregress.CompletedStep, bindings coreregress.Bindings) error {
	switch step.Action.Realization {
	case RegressionNexusScheduleDefault, RegressionNexusScheduleEmbedded, RegressionNexusSchedule:
		return p.startStandalone(ctx, step, bindings)
	case RegressionNexusRespondStartCompletionPendingAsync:
		p.policy.releaseDeferredStart()
		return nil
	case RegressionNexusCompleteScheduled:
		if err := (completion{}).Fire(ctx, p.context, umpirefw.Action{}); err != nil {
			return err
		}
		p.localFacts[p.semanticFactKey("nexus.state", step.Action.Arguments[0], regressnexus.CompletionPending)] = true
		return nil
	case RegressionNexusCompleteStarted, RegressionNexusCompleteCallbackFailed:
		if p.usesHandlerWorkflow() && step.Action.Realization == RegressionNexusCompleteStarted {
			return nil
		}
		err := (completion{}).Fire(ctx, p.context, umpirefw.Action{})
		if step.Action.Realization == RegressionNexusCompleteCallbackFailed && err != nil {
			p.localFacts[p.semanticFactKey("nexus.state", step.Action.Arguments[0], regressnexus.CallbackFailed)] = true
			return nil
		}
		return err
	case RegressionNexusTimeout, RegressionNexusCompleteFromHandler, RegressionNexusStartActivity:
		return nil
	case RegressionNexusCancel, RegressionNexusCancelWithRetry:
		return p.cancelOperation(ctx, step.Action, bindings)
	case RegressionNexusStartNewHandler, RegressionNexusStartAttachHandler:
		return p.startHandlerOperation(ctx, step.Action, bindings)
	case RegressionWorkflowComplete:
		return p.completeWorkflow(ctx, step.Action, bindings)
	default:
		return fmt.Errorf("unsupported proactive regression action realization %q", step.Action.Realization)
	}
}

func (p *regressionPath) startActivityFromHandler(ctx context.Context, action coreregress.CompletedAction, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	if len(action.Arguments) != 2 {
		return nil, errors.New("start activity requires operation and activity symbols")
	}
	operationID := fmt.Sprintf("umpire-regress-%d-%s", p.index, sanitizeIdentity(action.Arguments[0].SymbolName))
	describe, err := p.environment.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
		Namespace:   p.environment.Namespace().String(),
		OperationId: operationID,
	})
	if err != nil {
		return nil, err
	}
	activitySymbol := action.Arguments[1].SymbolName
	activityID := fmt.Sprintf("umpire-regress-%d-%s", p.index, sanitizeIdentity(activitySymbol))
	response, err := p.environment.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:           p.environment.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        &commonpb.ActivityType{Name: "umpire-regression-activity"},
		Identity:            "umpire-regression",
		Input:               payloads.EncodeString("input"),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: p.taskQueue},
		StartToCloseTimeout: durationpb.New(time.Minute),
		RequestId:           uuid.NewString(),
		Links: []*commonpb.Link{{Variant: &commonpb.Link_NexusOperation_{NexusOperation: &commonpb.Link_NexusOperation{
			Namespace:   p.environment.Namespace().String(),
			OperationId: operationID,
			RunId:       describe.GetRunId(),
		}}}},
	})
	if err != nil {
		return nil, err
	}
	poll, err := p.environment.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: p.environment.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: p.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "umpire-regression",
	})
	if err != nil {
		return nil, err
	}
	_, err = p.environment.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: p.environment.Namespace().String(),
		TaskToken: poll.GetTaskToken(),
		Result:    payloads.EncodeString("result"),
		Identity:  "umpire-regression",
	})
	if err != nil {
		return nil, err
	}
	activityLink := response.GetLink().GetActivity()
	if activityLink == nil {
		return nil, errors.New("standalone activity start returned no Activity link")
	}
	nexus.AddHandlerLinks(ctx, commonnexus.ConvertLinkActivityToNexusLink(activityLink))
	p.context.Bind(activitySymbol, activityID)
	p.mu.Lock()
	p.activityRuns[activityID] = response.GetRunId()
	p.activityOps[activityID] = operationID
	p.mu.Unlock()
	return &nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil
}

func (p *regressionPath) startHandlerOperation(ctx context.Context, action coreregress.CompletedAction, bindings coreregress.Bindings) error {
	if len(action.Arguments) != 2 {
		return errors.New("handler operation action requires operation and handler symbols")
	}
	operationSymbol := action.Arguments[0].SymbolName
	handlerSymbol := action.Arguments[1].SymbolName
	operationID := fmt.Sprintf("umpire-regress-%d-%s", p.index, sanitizeIdentity(operationSymbol))
	run, err := p.environment.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        operationID,
		TaskQueue: p.taskQueue,
	}, regressionSharedCallerWorkflow, p.context.Endpoint)
	if err != nil {
		return err
	}
	p.context.RunID = run.GetRunID()
	p.context.Bind(operationSymbol, operationID)
	p.context.Bind(handlerSymbol, p.handlerID)
	bindings[operationSymbol] = operationID
	bindings[handlerSymbol] = p.handlerID
	return nil
}

func (p *regressionPath) completeWorkflow(ctx context.Context, action coreregress.CompletedAction, bindings coreregress.Bindings) error {
	if len(action.Arguments) != 1 {
		return errors.New("workflow completion requires one workflow symbol")
	}
	workflowID, ok := bindingString(bindings, action.Arguments[0].SymbolName)
	if !ok {
		return errors.New("workflow completion target is not grounded")
	}
	if workflowID == p.handlerID {
		return p.environment.SdkClient().SignalWorkflow(ctx, workflowID, "", "complete", nil)
	}
	return nil
}

func (p *regressionPath) cancelOperation(ctx context.Context, action coreregress.CompletedAction, bindings coreregress.Bindings) error {
	if len(action.Arguments) == 0 {
		return errors.New("cancel action has no operation symbol")
	}
	operationID, ok := bindingString(bindings, action.Arguments[0].SymbolName)
	if !ok {
		return errors.New("cancel operation is not grounded")
	}
	_, err := p.environment.FrontendClient().RequestCancelNexusOperationExecution(ctx, &workflowservice.RequestCancelNexusOperationExecutionRequest{
		Namespace:   p.environment.Namespace().String(),
		OperationId: operationID,
		RunId:       p.context.RunID,
		RequestId:   operationID + "-cancel",
		Reason:      "umpire sparse regression",
	})
	if err != nil {
		return err
	}
	select {
	case <-p.policy.cancelObserved:
	case <-ctx.Done():
		return ctx.Err()
	}
	if action.Realization == RegressionNexusCancelWithRetry {
		if err := p.awaitStandaloneCancellationState(ctx, operationID, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED); err != nil {
			return err
		}
	}
	return (completion{opErr: nexus.NewOperationCanceledErrorf("umpire sparse regression cancellation")}).Fire(ctx, p.context, umpirefw.Action{})
}

func (p *regressionPath) awaitStandaloneCancellationState(
	ctx context.Context,
	operationID string,
	want enumspb.NexusOperationCancellationState,
) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		response, err := p.environment.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   p.environment.Namespace().String(),
			OperationId: operationID,
			RunId:       p.context.RunID,
		})
		if err == nil && response.GetInfo().GetCancellationInfo().GetState() == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w waiting for cancellation state %s", ctx.Err(), want)
		case <-ticker.C:
		}
	}
}

func (p *regressionPath) startStandalone(ctx context.Context, step coreregress.CompletedStep, bindings coreregress.Bindings) error {
	action := step.Action
	if len(action.Arguments) == 0 || action.Arguments[0].SymbolName == "" {
		return errors.New("schedule action has no operation symbol")
	}
	options, err := nexusOperationOptions(action)
	if err != nil {
		return err
	}
	symbol := action.Arguments[0].SymbolName
	operationID := fmt.Sprintf("umpire-regress-%d-%s", p.index, sanitizeIdentity(symbol))
	if action.Realization == RegressionNexusScheduleEmbedded || action.Realization == RegressionNexusSchedule {
		return p.startEmbedded(ctx, step, bindings, operationID, options)
	}
	response, err := p.environment.FrontendClient().StartNexusOperationExecution(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:              p.environment.Namespace().String(),
		OperationId:            operationID,
		Endpoint:               p.context.Endpoint,
		Service:                "service",
		Operation:              "operation",
		RequestId:              operationID,
		ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
	})
	if err != nil {
		return err
	}
	p.context.RunID = response.GetRunId()
	p.context.Bind(symbol, operationID)
	bindings[symbol] = operationID
	for _, effect := range actionEffectsForRealization(action) {
		if effect.Predicate == "nexus.start_to_close" {
			p.localFacts[semanticAtomKey(effect)] = true
		}
	}
	return nil
}

func nexusOperationOptions(action coreregress.CompletedAction) (workflow.NexusOperationOptions, error) {
	if action.Realization != RegressionNexusSchedule {
		return workflow.NexusOperationOptions{}, nil
	}
	if len(action.Arguments) != 2 || !action.Arguments[1].Literal {
		return workflow.NexusOperationOptions{}, errors.New("schedule start-to-close requires one duration literal")
	}
	configured, ok := action.Arguments[1].Value.(time.Duration)
	if !ok {
		return workflow.NexusOperationOptions{}, fmt.Errorf("schedule start-to-close value has type %T", action.Arguments[1].Value)
	}
	return workflow.NexusOperationOptions{StartToCloseTimeout: configured}, nil
}

func (p *regressionPath) startEmbedded(
	ctx context.Context,
	step coreregress.CompletedStep,
	bindings coreregress.Bindings,
	operationID string,
	options workflow.NexusOperationOptions,
) error {
	p.callerID = operationID
	workflowFn := any(regressionCallerWorkflow)
	workflowArg := any(p.context.Endpoint)
	if options.StartToCloseTimeout > 0 {
		workflowFn = regressionTimeoutCallerWorkflow
		workflowArg = regressionTimeoutCallerInput{Endpoint: p.context.Endpoint, Options: options}
	}
	run, err := p.environment.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        p.callerID,
		TaskQueue: p.taskQueue,
	}, workflowFn, workflowArg)
	if err != nil {
		return err
	}
	p.context.RunID = run.GetRunID()
	operationSymbol := step.Action.Arguments[0].SymbolName
	p.context.Bind(operationSymbol, p.callerID)
	bindings[operationSymbol] = p.callerID
	for _, effect := range step.Effects {
		if effect.Predicate == "nexus.start_to_close" {
			p.localFacts[semanticAtomKey(effect)] = true
		}
		if effect.Predicate == "nexus.child_of" {
			p.localFacts[semanticAtomKey(effect)] = true
		}
		if effect.Predicate == "workflow.state" || effect.Predicate == "nexus.child_of" {
			for _, argument := range effect.Arguments {
				if argument.SymbolName != "" && argument.SymbolName != operationSymbol {
					p.context.Bind(argument.SymbolName, p.callerID)
					bindings[argument.SymbolName] = p.callerID
				}
			}
		}
	}
	return nil
}

func actionEffectsForRealization(action coreregress.CompletedAction) []coreregress.CompletedAtom {
	switch action.Realization {
	case RegressionNexusSchedule:
		if len(action.Arguments) < 2 {
			return nil
		}
		return []coreregress.CompletedAtom{{
			Predicate: "nexus.start_to_close",
			Arguments: []coreregress.Argument{action.Arguments[0], action.Arguments[1]},
		}}
	default:
		return nil
	}
}

func sanitizeIdentity(value string) string {
	return strings.NewReplacer("/", "-", " ", "-", "_", "-").Replace(value)
}

func armRegressionFault(actionContext *Ctx, method string, transient bool) (func(), error) {
	generator := actionContext.Env.GetFaultInjector()
	if generator == nil {
		return nil, errors.New("fault injector is nil (build with -tags test_dep)")
	}
	namespaceID := actionContext.Env.NamespaceID().String()
	namespaceName := actionContext.Env.Namespace().String()
	var seen atomic.Int32
	cleanup := generator.RegisterCallback(func(_ context.Context, fullMethod string, request, _ any, _ error) (bool, any, error) {
		if !methodMatches(fullMethod, method) || !namespaceMatches(request, namespaceID, namespaceName) {
			return false, nil, nil
		}
		if transient && seen.Add(1) > 1 {
			return false, nil, nil
		}
		return true, nil, fmt.Errorf("umpire regression: injected failure of %s", method)
	})
	return cleanup, nil
}
