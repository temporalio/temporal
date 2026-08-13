// Package nexus provides typed Nexus instructions for sparse regression plans.
package nexus

import (
	"time"

	"go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/tests/umpire2/regress/rpc"
)

var (
	OperationType = regress.EntityType("NexusOperation")
	StateType     = regress.ValueType("NexusState")
	OutcomeType   = regress.ValueType("NexusOutcome")
	ResponseType  = regress.ValueType("NexusStartResponse")
	DurationType  = regress.ValueType("Duration")
	RPCType       = regress.ValueType("RPC")
	ActivityType  = regress.EntityType("Activity")
	WorkflowType  = regress.EntityType("WorkflowRun")
	CallbackType  = regress.EntityType("Callback")
	DigestType    = regress.ValueType("Digest")
	LinkType      = regress.ValueType("LinkEndpoint")
)

type StateValue string

const (
	Scheduled         StateValue = "scheduled"
	CompletionPending StateValue = "completion_pending"
	Started           StateValue = "started"
	Completed         StateValue = "completed"
	Canceled          StateValue = "canceled"
	TimedOut          StateValue = "timed_out"
	CallbackFailed    StateValue = "callback_failed"
)

type OutcomeValue string

const (
	Succeeded OutcomeValue = "succeeded"
	Failed    OutcomeValue = "failed"
)

type StartResponseValue string

const (
	Async StartResponseValue = "async"
	Sync  StartResponseValue = "sync"
)

var (
	stateSchema = regress.OutcomeSchema(
		"nexus.state",
		regress.SymbolParameter("operation", OperationType),
		regress.LiteralParameter("state", StateType),
	)
	completeSchema = regress.ActionSchema(
		"nexus.complete",
		regress.SymbolParameter("operation", OperationType),
		regress.LiteralParameter("outcome", OutcomeType),
	)
	respondStartSchema = regress.ActionSchema(
		"nexus.respond_start",
		regress.SymbolParameter("operation", OperationType),
		regress.LiteralParameter("response", ResponseType),
	)
	cancelSchema = regress.ActionSchema(
		"nexus.cancel",
		regress.SymbolParameter("operation", OperationType),
	)
	cancelWithRetrySchema = regress.ActionSchema(
		"nexus.cancel_with_retry",
		regress.SymbolParameter("operation", OperationType),
	)
	cancelRequestFailedSchema = regress.OutcomeSchema(
		"nexus.cancel_request_failed",
		regress.SymbolParameter("operation", OperationType),
	)
	scheduleSchema = regress.ActionSchema(
		"nexus.schedule",
		regress.SymbolParameter("operation", OperationType),
		regress.LiteralParameter("start_to_close", DurationType),
	)
	startSchema = regress.ActionSchema(
		"nexus.start",
		regress.SymbolParameter("operation", OperationType),
		regress.SymbolParameter("handler", WorkflowType),
	)
	scheduleEmbeddedSchema = regress.ActionSchema(
		"nexus.schedule_embedded_with_caller",
		regress.SymbolParameter("operation", OperationType),
		regress.SymbolParameter("caller", WorkflowType),
	)
	startActivitySchema = regress.ActionSchema(
		"nexus.start_activity",
		regress.SymbolParameter("operation", OperationType),
		regress.SymbolParameter("activity", ActivityType),
	)
	linkedToActivitySchema = regress.RelationSchema(
		"nexus.linked_to_activity",
		regress.SymbolParameter("operation", OperationType),
		regress.SymbolParameter("activity", ActivityType),
	)
	handlerWorkflowSchema = regress.RelationSchema(
		"nexus.handler_workflow",
		regress.SymbolParameter("operation", OperationType),
		regress.SymbolParameter("handler", WorkflowType),
	)
	childOfSchema = regress.RelationSchema(
		"nexus.child_of",
		regress.SymbolParameter("operation", OperationType),
		regress.SymbolParameter("caller", WorkflowType),
	)
	startToCloseSchema = regress.OutcomeSchema(
		"nexus.start_to_close",
		regress.SymbolParameter("operation", OperationType),
		regress.LiteralParameter("duration", DurationType),
	)
	callbackOperationSchema = regress.RelationSchema(
		"nexus.callback_operation",
		regress.SymbolParameter("callback", CallbackType),
		regress.SymbolParameter("operation", OperationType),
	)
	resultDigestSchema = regress.OutcomeSchema(
		"nexus.result_digest",
		regress.SymbolParameter("operation", OperationType),
		regress.LiteralParameter("digest", DigestType),
	)
	linkEndpointSchema = regress.OutcomeSchema(
		"nexus.link_endpoint",
		regress.SymbolParameter("operation", OperationType),
		regress.LiteralParameter("endpoint", LinkType),
	)
	dropSchema = regress.PolicySchema(
		"nexus.drop",
		regress.LiteralParameter("rpc", RPCType),
	)
	failNextSchema = regress.PolicySchema(
		"nexus.fail_next",
		regress.LiteralParameter("rpc", RPCType),
	)
)

func State(operation string, state StateValue) regress.Instruction {
	return regress.Outcome(stateSchema, regress.Symbol(operation), regress.Literal(state))
}

func Complete(operation string, outcome OutcomeValue) regress.Instruction {
	return regress.Action(completeSchema, regress.Symbol(operation), regress.Literal(outcome))
}

func RespondStart(operation string, response StartResponseValue) regress.Instruction {
	return regress.Action(respondStartSchema, regress.Symbol(operation), regress.Literal(response))
}

func Cancel(operation string) regress.Instruction {
	return regress.Action(cancelSchema, regress.Symbol(operation))
}

func CancelWithRetry(operation string) regress.Instruction {
	return regress.Action(cancelWithRetrySchema, regress.Symbol(operation))
}

func CancelRequestFailed(operation string) regress.Instruction {
	return regress.Outcome(cancelRequestFailedSchema, regress.Symbol(operation))
}

type ScheduleOption struct {
	startToClose time.Duration
}

func StartToClose(timeout time.Duration) ScheduleOption {
	return ScheduleOption{startToClose: timeout}
}

func Schedule(operation string, option ScheduleOption) regress.Instruction {
	return regress.Action(scheduleSchema, regress.Symbol(operation), regress.Literal(option.startToClose))
}

func ScheduleEmbedded(operation, caller string) regress.Instruction {
	return regress.Action(scheduleEmbeddedSchema, regress.Symbol(operation), regress.Symbol(caller))
}

type StartTarget struct {
	handler string
}

func HandlerWorkflow(handler string) StartTarget { return StartTarget{handler: handler} }

func Start(operation string, target StartTarget) regress.Instruction {
	return regress.Action(startSchema, regress.Symbol(operation), regress.Symbol(target.handler))
}

func StartActivity(operation, activity string) regress.Instruction {
	return regress.Action(startActivitySchema, regress.Symbol(operation), regress.Symbol(activity))
}

func LinkedToActivity(operation, activity string) regress.Instruction {
	return regress.Relation(linkedToActivitySchema, regress.Symbol(operation), regress.Symbol(activity))
}

func ChildOf(operation, caller string) regress.Instruction {
	return regress.Relation(childOfSchema, regress.Symbol(operation), regress.Symbol(caller))
}

func CallbackOperation(callback, operation string) regress.Instruction {
	return regress.Relation(callbackOperationSchema, regress.Symbol(callback), regress.Symbol(operation))
}

func ResultDigest(operation, digest string) regress.Instruction {
	return regress.Outcome(resultDigestSchema, regress.Symbol(operation), regress.Literal(digest))
}

func LinkEndpoint(operation, endpoint string) regress.Instruction {
	return regress.Outcome(linkEndpointSchema, regress.Symbol(operation), regress.Literal(endpoint))
}

func Drop(name rpc.Name) regress.Instruction {
	return regress.Policy(dropSchema, regress.Literal(name))
}

func FailNext(name rpc.Name) regress.Instruction {
	return regress.Policy(failNextSchema, regress.Literal(name))
}

func StateSchema() regress.Schema           { return stateSchema }
func CompleteSchema() regress.Schema        { return completeSchema }
func RespondStartSchema() regress.Schema    { return respondStartSchema }
func CancelSchema() regress.Schema          { return cancelSchema }
func CancelWithRetrySchema() regress.Schema { return cancelWithRetrySchema }
func CancelRequestFailedSchema() regress.Schema {
	return cancelRequestFailedSchema
}
func ScheduleSchema() regress.Schema          { return scheduleSchema }
func ScheduleEmbeddedSchema() regress.Schema  { return scheduleEmbeddedSchema }
func StartSchema() regress.Schema             { return startSchema }
func StartActivitySchema() regress.Schema     { return startActivitySchema }
func LinkedToActivitySchema() regress.Schema  { return linkedToActivitySchema }
func HandlerWorkflowSchema() regress.Schema   { return handlerWorkflowSchema }
func ChildOfSchema() regress.Schema           { return childOfSchema }
func StartToCloseSchema() regress.Schema      { return startToCloseSchema }
func CallbackOperationSchema() regress.Schema { return callbackOperationSchema }
func ResultDigestSchema() regress.Schema      { return resultDigestSchema }
func LinkEndpointSchema() regress.Schema      { return linkEndpointSchema }
func DropSchema() regress.Schema              { return dropSchema }
func FailNextSchema() regress.Schema          { return failNextSchema }
