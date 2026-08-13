package protocol

import (
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/tests/umpire2/action"
	"go.temporal.io/server/tests/umpire2/regress/activity"
	"go.temporal.io/server/tests/umpire2/regress/capability"
	"go.temporal.io/server/tests/umpire2/regress/nexus"
	"go.temporal.io/server/tests/umpire2/regress/workflow"
)

func defaultRegressionDomain() *coreregress.Domain {
	domain := coreregress.NewDomain("umpire2/v1")
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: nexus.StateSchema(), ExclusiveBy: []int{0}})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: workflow.StateSchema(), ExclusiveBy: []int{0}})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: workflow.RunIDSchema(), ExclusiveBy: []int{0}})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: activity.StateSchema(), ExclusiveBy: []int{0}})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: nexus.LinkedToActivitySchema()})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: activity.LinkedToNexusOperationSchema()})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: nexus.HandlerWorkflowSchema(), ExclusiveBy: []int{0}})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: nexus.ChildOfSchema(), ExclusiveBy: []int{0}})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: nexus.StartToCloseSchema(), ExclusiveBy: []int{0}})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: nexus.CancelRequestFailedSchema(), ExclusiveBy: []int{0}})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: nexus.CallbackOperationSchema(), ExclusiveBy: []int{0}})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: workflow.CallbackHandlerRunSchema(), ExclusiveBy: []int{0}})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: nexus.ResultDigestSchema(), ExclusiveBy: []int{0}})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: nexus.LinkEndpointSchema(), ExclusiveBy: []int{0}})
	mustAddPredicate(domain, coreregress.PredicateCapability{Schema: workflow.NexusStorageAbsentSchema(), ExclusiveBy: []int{0, 1}})

	mustAddResource(domain, coreregress.ResourceCapability{Name: "namespace", Realization: action.RegressionResourceNamespace})
	mustAddResource(domain, coreregress.ResourceCapability{Name: "task-queue", DependsOn: []string{"namespace"}, Realization: action.RegressionResourceTaskQueue})
	mustAddResource(domain, coreregress.ResourceCapability{Name: "worker", DependsOn: []string{"task-queue"}, Realization: action.RegressionResourceWorker})
	mustAddResource(domain, coreregress.ResourceCapability{Name: "nexus-endpoint", DependsOn: []string{"worker"}, Realization: action.RegressionResourceNexusEndpoint})
	mustAddResource(domain, coreregress.ResourceCapability{Name: "fault-injector", DependsOn: []string{"namespace"}, Requires: []string{capability.Faults.Name}, Realization: action.RegressionResourceFaultInjector})

	mustAddPolicy(domain, coreregress.PolicyCapability{
		Schema:      nexus.DropSchema(),
		Resources:   []string{"fault-injector"},
		Requires:    []string{capability.Faults.Name},
		Realization: action.RegressionPolicyNexusDrop,
	})
	mustAddPolicy(domain, coreregress.PolicyCapability{
		Schema:      nexus.FailNextSchema(),
		Resources:   []string{"fault-injector"},
		Requires:    []string{capability.Faults.Name},
		Realization: action.RegressionPolicyNexusFailNext,
	})

	registerNexusActions(domain)
	registerWorkflowActions(domain)
	registerObservationActions(domain)
	return domain
}

// DefaultRegressionDomain returns the sparse catalog compiled with the canonical Temporal protocol.
func DefaultRegressionDomain() (*coreregress.Domain, error) {
	compiled, err := Default()
	if err != nil {
		return nil, err
	}
	return compiled.regression.Clone(), nil
}

func registerNexusActions(domain *coreregress.Domain) {
	op := coreregress.Variable{Name: "operation", Type: nexus.OperationType}
	freshOp := op
	freshOp.Binding = coreregress.FreshBinding
	caller := coreregress.Variable{Name: "caller", Type: workflow.WorkflowType}
	freshCaller := caller
	freshCaller.Binding = coreregress.FreshBinding
	handler := coreregress.Variable{Name: "handler", Type: workflow.WorkflowType}
	freshHandler := handler
	freshHandler.Binding = coreregress.FreshBinding
	outcome := coreregress.Variable{Name: "outcome", Type: nexus.OutcomeType}
	response := coreregress.Variable{Name: "response", Type: nexus.ResponseType}
	timeout := coreregress.Variable{Name: "start_to_close", Type: nexus.DurationType}
	activityVar := coreregress.Variable{Name: "activity", Type: activity.ActivityType, Binding: coreregress.FreshBinding}

	scheduleDefault := coreregress.ActionSchema(
		"nexus.schedule_default",
		coreregress.SymbolParameter("operation", nexus.OperationType),
	)
	mustAddAction(domain, coreregress.ActionCapability{
		Schema:      scheduleDefault,
		Variables:   []coreregress.Variable{freshOp},
		Effects:     []coreregress.AtomTemplate{nexusState("operation", nexus.Scheduled)},
		Resources:   []string{"nexus-endpoint"},
		Realization: action.RegressionNexusScheduleDefault,
	})
	mustAddAction(domain, coreregress.ActionCapability{
		Schema:    coreregress.ActionSchema("nexus.schedule_embedded", coreregress.SymbolParameter("operation", nexus.OperationType)),
		Variables: []coreregress.Variable{freshOp, freshCaller},
		Effects: []coreregress.AtomTemplate{
			nexusState("operation", nexus.Scheduled),
			workflowState("caller", workflow.Started),
			coreregress.Atom("nexus.child_of", coreregress.TemplateVar("operation"), coreregress.TemplateVar("caller")),
		},
		Resources:   []string{"nexus-endpoint"},
		Realization: action.RegressionNexusScheduleEmbedded,
	})
	mustAddAction(domain, coreregress.ActionCapability{
		Schema:    nexus.ScheduleEmbeddedSchema(),
		Variables: []coreregress.Variable{freshOp, freshCaller},
		Effects: []coreregress.AtomTemplate{
			nexusState("operation", nexus.Scheduled),
			workflowState("caller", workflow.Started),
			coreregress.Atom("nexus.child_of", coreregress.TemplateVar("operation"), coreregress.TemplateVar("caller")),
		},
		Resources:   []string{"nexus-endpoint"},
		Realization: action.RegressionNexusScheduleEmbedded,
	})
	mustAddAction(domain, coreregress.ActionCapability{
		Schema:      nexus.ScheduleSchema(),
		Variables:   []coreregress.Variable{freshOp, timeout},
		Effects:     []coreregress.AtomTemplate{nexusState("operation", nexus.Scheduled), coreregress.Atom("nexus.start_to_close", coreregress.TemplateVar("operation"), coreregress.TemplateVar("start_to_close"))},
		Resources:   []string{"nexus-endpoint"},
		Realization: action.RegressionNexusSchedule,
	})

	for _, start := range []struct {
		from, to nexus.StateValue
		response nexus.StartResponseValue
	}{
		{from: nexus.Scheduled, to: nexus.Started, response: nexus.Async},
		{from: nexus.CompletionPending, to: nexus.Completed, response: nexus.Async},
		{from: nexus.Scheduled, to: nexus.Completed, response: nexus.Sync},
	} {
		mustAddAction(domain, coreregress.ActionCapability{
			Schema:        nexus.RespondStartSchema(),
			Mode:          coreregress.ReactiveAction,
			Variables:     []coreregress.Variable{op, response},
			Preconditions: []coreregress.AtomTemplate{nexusState("operation", start.from)},
			Effects:       []coreregress.AtomTemplate{nexusState("operation", start.to)},
			Fixed:         map[string]any{"response": start.response},
			Realization:   action.RegressionNexusRespondStart(string(start.from), string(start.response)),
		})
	}
	for _, completion := range []struct {
		from, to nexus.StateValue
	}{
		{from: nexus.Scheduled, to: nexus.CompletionPending},
		{from: nexus.Started, to: nexus.Completed},
	} {
		mustAddAction(domain, coreregress.ActionCapability{
			Schema:        nexus.CompleteSchema(),
			Variables:     []coreregress.Variable{op, outcome},
			Preconditions: []coreregress.AtomTemplate{nexusState("operation", completion.from)},
			Effects:       []coreregress.AtomTemplate{nexusState("operation", completion.to)},
			Fixed:         map[string]any{"outcome": nexus.Succeeded},
			Realization:   action.RegressionNexusComplete(string(completion.from)),
		})
	}
	mustAddAction(domain, coreregress.ActionCapability{
		Schema:    nexus.CompleteSchema(),
		Variables: []coreregress.Variable{op, outcome, caller},
		Preconditions: []coreregress.AtomTemplate{
			nexusState("operation", nexus.Started),
			workflowState("caller", workflow.Completed),
			coreregress.Atom("nexus.child_of", coreregress.TemplateVar("operation"), coreregress.TemplateVar("caller")),
		},
		Effects:     []coreregress.AtomTemplate{nexusState("operation", nexus.CallbackFailed)},
		Fixed:       map[string]any{"outcome": nexus.Succeeded},
		Realization: action.RegressionNexusCompleteCallbackFailed,
	})
	mustAddAction(domain, coreregress.ActionCapability{
		Schema:        nexus.CancelSchema(),
		Variables:     []coreregress.Variable{op},
		Preconditions: []coreregress.AtomTemplate{nexusState("operation", nexus.Started)},
		Effects:       []coreregress.AtomTemplate{nexusState("operation", nexus.Canceled)},
		Realization:   action.RegressionNexusCancel,
	})
	mustAddAction(domain, coreregress.ActionCapability{
		Schema:        nexus.CancelWithRetrySchema(),
		Variables:     []coreregress.Variable{op},
		Preconditions: []coreregress.AtomTemplate{nexusState("operation", nexus.Started)},
		Effects: []coreregress.AtomTemplate{
			coreregress.Atom("nexus.cancel_request_failed", coreregress.TemplateVar("operation")),
			nexusState("operation", nexus.Canceled),
		},
		Realization: action.RegressionNexusCancelWithRetry,
	})
	mustAddAction(domain, coreregress.ActionCapability{
		Schema:    coreregress.ActionSchema("nexus.timeout", coreregress.SymbolParameter("operation", nexus.OperationType)),
		Variables: []coreregress.Variable{op, timeout},
		Preconditions: []coreregress.AtomTemplate{
			nexusState("operation", nexus.Started),
			coreregress.Atom("nexus.start_to_close", coreregress.TemplateVar("operation"), coreregress.TemplateVar("start_to_close")),
		},
		Effects:     []coreregress.AtomTemplate{nexusState("operation", nexus.TimedOut)},
		Realization: action.RegressionNexusTimeout,
	})

	mustAddAction(domain, coreregress.ActionCapability{
		Schema:    nexus.StartSchema(),
		Variables: []coreregress.Variable{freshOp, freshHandler},
		Effects: []coreregress.AtomTemplate{
			nexusState("operation", nexus.Started),
			workflowState("handler", workflow.Started),
			coreregress.Atom("nexus.handler_workflow", coreregress.TemplateVar("operation"), coreregress.TemplateVar("handler")),
		},
		Resources:   []string{"nexus-endpoint"},
		Realization: action.RegressionNexusStartNewHandler,
	})
	mustAddAction(domain, coreregress.ActionCapability{
		Schema:        nexus.StartSchema(),
		Variables:     []coreregress.Variable{freshOp, handler},
		Preconditions: []coreregress.AtomTemplate{workflowState("handler", workflow.Started)},
		Effects: []coreregress.AtomTemplate{
			nexusState("operation", nexus.Started),
			coreregress.Atom("nexus.handler_workflow", coreregress.TemplateVar("operation"), coreregress.TemplateVar("handler")),
		},
		Resources:   []string{"nexus-endpoint"},
		Realization: action.RegressionNexusStartAttachHandler,
	})
	mustAddAction(domain, coreregress.ActionCapability{
		Schema:    coreregress.ActionSchema("nexus.complete_from_handler", coreregress.SymbolParameter("operation", nexus.OperationType)),
		Variables: []coreregress.Variable{op, handler},
		Preconditions: []coreregress.AtomTemplate{
			workflowState("handler", workflow.Completed),
			coreregress.Atom("nexus.handler_workflow", coreregress.TemplateVar("operation"), coreregress.TemplateVar("handler")),
		},
		Effects:     []coreregress.AtomTemplate{nexusState("operation", nexus.Completed)},
		Realization: action.RegressionNexusCompleteFromHandler,
	})

	mustAddAction(domain, coreregress.ActionCapability{
		Schema:        nexus.StartActivitySchema(),
		Mode:          coreregress.ReactiveAction,
		Variables:     []coreregress.Variable{op, activityVar},
		Preconditions: []coreregress.AtomTemplate{nexusState("operation", nexus.Scheduled)},
		Effects: []coreregress.AtomTemplate{
			nexusState("operation", nexus.Completed),
			activityState("activity", activity.Completed),
			coreregress.Atom("nexus.linked_to_activity", coreregress.TemplateVar("operation"), coreregress.TemplateVar("activity")),
			coreregress.Atom("activity.linked_to_nexus_operation", coreregress.TemplateVar("activity"), coreregress.TemplateVar("operation")),
		},
		Requires:    []string{capability.ActivityCallbacks.Name},
		Resources:   []string{"worker"},
		Realization: action.RegressionNexusStartActivity,
	})
}

func registerWorkflowActions(domain *coreregress.Domain) {
	workflowVar := coreregress.Variable{Name: "workflow", Type: workflow.WorkflowType}
	runID := coreregress.Variable{Name: "run_id", Type: workflow.RunIDType, Binding: coreregress.ObservedBinding}
	mustAddAction(domain, coreregress.ActionCapability{
		Schema:        coreregress.ActionSchema("workflow.complete", coreregress.SymbolParameter("workflow", workflow.WorkflowType)),
		Variables:     []coreregress.Variable{workflowVar},
		Preconditions: []coreregress.AtomTemplate{workflowState("workflow", workflow.Started)},
		Effects:       []coreregress.AtomTemplate{workflowState("workflow", workflow.Completed)},
		Realization:   action.RegressionWorkflowComplete,
	})
	mustAddAction(domain, coreregress.ActionCapability{
		Schema:        coreregress.ActionSchema("workflow.observe_run_id", coreregress.SymbolParameter("workflow", workflow.WorkflowType)),
		Mode:          coreregress.ObservationAction,
		Variables:     []coreregress.Variable{workflowVar, runID},
		Preconditions: []coreregress.AtomTemplate{workflowState("workflow", workflow.Started)},
		Effects:       []coreregress.AtomTemplate{coreregress.Atom("workflow.run_id", coreregress.TemplateVar("workflow"), coreregress.TemplateVar("run_id"))},
		Realization:   action.RegressionWorkflowObserveRunID,
	})
}

func registerObservationActions(domain *coreregress.Domain) {
	operation := coreregress.Variable{Name: "operation", Type: nexus.OperationType}
	digest := coreregress.Variable{Name: "digest", Type: nexus.DigestType}
	endpoint := coreregress.Variable{Name: "endpoint", Type: nexus.LinkType}
	workflowVar := coreregress.Variable{Name: "workflow", Type: workflow.WorkflowType}

	mustAddAction(domain, coreregress.ActionCapability{
		Schema: coreregress.ActionSchema(
			"nexus.observe_result_digest",
			coreregress.SymbolParameter("operation", nexus.OperationType),
			coreregress.LiteralParameter("digest", nexus.DigestType),
		),
		Mode:      coreregress.ObservationAction,
		Variables: []coreregress.Variable{operation, digest},
		Effects: []coreregress.AtomTemplate{
			coreregress.Atom("nexus.result_digest", coreregress.TemplateVar("operation"), coreregress.TemplateVar("digest")),
		},
		Realization: action.RegressionObserve,
	})
	mustAddAction(domain, coreregress.ActionCapability{
		Schema: coreregress.ActionSchema(
			"nexus.observe_link_endpoint",
			coreregress.SymbolParameter("operation", nexus.OperationType),
			coreregress.LiteralParameter("endpoint", nexus.LinkType),
		),
		Mode:      coreregress.ObservationAction,
		Variables: []coreregress.Variable{operation, endpoint},
		Effects: []coreregress.AtomTemplate{
			coreregress.Atom("nexus.link_endpoint", coreregress.TemplateVar("operation"), coreregress.TemplateVar("endpoint")),
		},
		Realization: action.RegressionObserve,
	})
	mustAddAction(domain, coreregress.ActionCapability{
		Schema: coreregress.ActionSchema(
			"workflow.observe_nexus_storage_absent",
			coreregress.SymbolParameter("workflow", workflow.WorkflowType),
			coreregress.SymbolParameter("operation", nexus.OperationType),
		),
		Mode:      coreregress.ObservationAction,
		Variables: []coreregress.Variable{workflowVar, operation},
		Effects: []coreregress.AtomTemplate{
			coreregress.Atom("workflow.nexus_storage_absent", coreregress.TemplateVar("workflow"), coreregress.TemplateVar("operation")),
		},
		Realization: action.RegressionObserve,
	})
}

func nexusState(variable string, state nexus.StateValue) coreregress.AtomTemplate {
	return coreregress.Atom("nexus.state", coreregress.TemplateVar(variable), coreregress.TemplateLiteral(state))
}

func workflowState(variable string, state workflow.StateValue) coreregress.AtomTemplate {
	return coreregress.Atom("workflow.state", coreregress.TemplateVar(variable), coreregress.TemplateLiteral(state))
}

func activityState(variable string, state activity.StateValue) coreregress.AtomTemplate {
	return coreregress.Atom("activity.state", coreregress.TemplateVar(variable), coreregress.TemplateLiteral(state))
}

func mustAddPredicate(domain *coreregress.Domain, predicate coreregress.PredicateCapability) {
	if err := domain.AddPredicate(predicate); err != nil {
		panic(err)
	}
}

func mustAddAction(domain *coreregress.Domain, registeredAction coreregress.ActionCapability) {
	if err := domain.AddAction(registeredAction); err != nil {
		panic(err)
	}
}

func mustAddResource(domain *coreregress.Domain, resource coreregress.ResourceCapability) {
	if err := domain.AddResource(resource); err != nil {
		panic(err)
	}
}

func mustAddPolicy(domain *coreregress.Domain, policy coreregress.PolicyCapability) {
	if err := domain.AddPolicy(policy); err != nil {
		panic(err)
	}
}
