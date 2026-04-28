package standalonenexusop

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/server/tests/model"
	. "go.temporal.io/server/tests/testcore/umpire"
)

// rpcs is the per-package RPC catalog.
var rpcs = []RPCSpec{
	model.StartNexusOperationExecutionRPCSpec(
		model.StartNexusOperationExecutionRequestSpec{
			Namespace:              model.NamespaceField,
			Identity:               model.IdentityField,
			RequestID:              model.RequestIDField,
			OperationID:            model.OperationIDField,
			Endpoint:               Field[string](model.NonZeroString("endpoint")),
			Service:                Field[string](model.NonZeroString("service")),
			Operation:              Field[string](model.NonZeroString("operation")),
			ScheduleToCloseTimeout: Field[time.Duration](model.NonZeroDuration("schedule_to_close_timeout")),
			ScheduleToStartTimeout: Field[time.Duration](Ignored[time.Duration]()),
			StartToCloseTimeout:    Field[time.Duration](Ignored[time.Duration]()),
			Input:                  model.StartNexusOperationExecutionRequestInputSpec{Ref: Field[*commonpb.Payload](Ignored[*commonpb.Payload]())},
			IDReusePolicy:          Field[enumspb.NexusOperationIdReusePolicy](Ignored[enumspb.NexusOperationIdReusePolicy]()),
			IDConflictPolicy:       Field[enumspb.NexusOperationIdConflictPolicy](Ignored[enumspb.NexusOperationIdConflictPolicy]()),
			SearchAttributes:       model.StartNexusOperationExecutionRequestSearchAttributesSpec{Ref: Field[*commonpb.SearchAttributes](Ignored[*commonpb.SearchAttributes]())},
			NexusHeader:            Field[any](Ignored[any]()),
			UserMetadata:           model.StartNexusOperationExecutionRequestUserMetadataSpec{Ref: Field[*sdkpb.UserMetadata](Ignored[*sdkpb.UserMetadata]())},
		},
		model.StartNexusOperationExecutionResponseSpec{
			RunID:   model.StartedRunIDField,
			Started: Field[bool](),
		},
	),
	model.RespondNexusTaskCompletedRPCSpec(
		model.RespondNexusTaskCompletedRequestSpec{
			Namespace:     model.NamespaceField,
			Identity:      model.IdentityField,
			TaskToken:     Field[[]byte](Role[[]byte](RPCFieldRoleOpaque), Mutation[[]byte]("wrong-task-token")),
			Response:      model.RespondNexusTaskCompletedRequestResponseSpec{Ref: Field[*nexuspb.Response]()},
			PollerGroupID: Field[string](Ignored[string]()),
		},
		model.RespondNexusTaskCompletedResponseSpec{},
	),
	model.DescribeNexusOperationExecutionRPCSpec(
		model.DescribeNexusOperationExecutionRequestSpec{
			Namespace:      model.NamespaceField,
			OperationID:    model.OperationIDField,
			RunID:          model.RunIDField,
			IncludeInput:   Field[bool](Mutable[bool]()),
			IncludeOutcome: Field[bool](Mutable[bool]()),
			LongPollToken:  Field[[]byte](Role[[]byte](RPCFieldRoleOpaque)),
		},
		model.DescribeNexusOperationExecutionResponseSpec{
			RunID:         model.NondeterministicRunIDField,
			Info:          model.DescribeNexusOperationExecutionResponseInfoSpec{Ref: Field[*nexuspb.NexusOperationExecutionInfo]()},
			Input:         model.DescribeNexusOperationExecutionResponseInputSpec{Ref: Field[*commonpb.Payload](Ignored[*commonpb.Payload]())},
			Result:        model.DescribeNexusOperationExecutionResponseResultSpec{Ref: Field[*commonpb.Payload](Ignored[*commonpb.Payload]())},
			Failure:       model.DescribeNexusOperationExecutionResponseFailureSpec{Ref: Field[*failurepb.Failure](Ignored[*failurepb.Failure]())},
			LongPollToken: Field[[]byte](Role[[]byte](RPCFieldRoleOpaque)),
		},
	),
	model.RequestCancelNexusOperationExecutionRPCSpec(
		model.RequestCancelNexusOperationExecutionRequestSpec{
			Namespace:   model.NamespaceField,
			OperationID: model.OperationIDField,
			RunID:       model.RunIDField,
			Identity:    model.IdentityField,
			RequestID:   model.RequestIDField,
			Reason:      model.ReasonField,
		},
		model.RequestCancelNexusOperationExecutionResponseSpec{},
	),
	model.TerminateNexusOperationExecutionRPCSpec(
		model.TerminateNexusOperationExecutionRequestSpec{
			Namespace:   model.NamespaceField,
			OperationID: model.OperationIDField,
			RunID:       model.RunIDField,
			Identity:    model.IdentityField,
			RequestID:   model.RequestIDField,
			Reason:      model.ReasonField,
		},
		model.TerminateNexusOperationExecutionResponseSpec{},
	),
}
