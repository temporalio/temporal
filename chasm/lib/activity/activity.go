// A note on times and terminology:
//
// We name 3 times in the lifecycle of an activity attempt:
//
// schedule time - the time at which the activity entered SCHEDULED state
// dispatch time - the time at which the activity task will be dispatched to Matching (AddActivityTask)
// start time    - the time at which the activity enters STARTED state (Matching task picked up by poller)
//
// They are always ordered as: (schedule time) <= (dispatch time) < (start time).
//
// A ScheduleToStart timeout applies to the time between dispatch and start. If there is a delay
// before dispatch (i.e. a start delay on the first attempt, or a backoff interval / next retry
// delay on a second or subsequent attempt) then schedule time < dispatch time. Otherwise, they are
// equal.
//
// The main Activity struct has a.ScheduleTime which is the schedule time of the first
// attempt; i.e. the time at which the activity was created. This is never changed.

package activity

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/activityoptions"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payload"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// WorkflowTypeTag is a required workflow tag for standalone activities to ensure consistent
	// metric labeling between workflows and activities.
	WorkflowTypeTag = "__temporal_standalone_activity__"

	// ByIDTokenAttempt is used in synthesized tokens for by-ID API calls where the caller does not specify the attempt.
	// The validator skips the attempt check when it sees this value.
	// 0 is safe because polled tokens always carry Count >= 1 (TransitionScheduled increments from 0).
	ByIDTokenAttempt int32 = 0
)

var (
	TypeSearchAttribute   = chasm.NewSearchAttributeKeyword("ActivityType", chasm.SearchAttributeFieldKeyword01)
	StatusSearchAttribute = chasm.NewSearchAttributeKeyword("ExecutionStatus", chasm.SearchAttributeFieldLowCardinalityKeyword01)
)

var _ chasm.VisibilitySearchAttributesProvider = (*Activity)(nil)
var _ callback.CompletionSource = (*Activity)(nil)

type ActivityStore interface {
	// RecordCompleted applies the provided function to record activity completion
	RecordCompleted(ctx chasm.MutableContext, applyFn func(ctx chasm.MutableContext) error) error
}

// Activity component represents an activity execution persistence object and can be either standalone activity or one
// embedded within a workflow.
type Activity struct {
	chasm.UnimplementedComponent

	*activitypb.ActivityState

	Visibility    chasm.Field[*chasm.Visibility]
	LastAttempt   chasm.Field[*activitypb.ActivityAttemptState]
	LastHeartbeat chasm.Field[*activitypb.ActivityHeartbeatState]
	// Standalone only
	RequestData chasm.Field[*activitypb.ActivityRequestData]
	Outcome     chasm.Field[*activitypb.ActivityOutcome]
	// Pointer to an implementation of the "store". For a workflow activity this would be a parent
	// pointer back to the workflow. For a standalone activity this is nil (Activity itself
	// implements the ActivityStore interface).
	// TODO(saa-preview): figure out better naming.
	Store chasm.ParentPtr[ActivityStore]

	// Callbacks holds completion callbacks to be invoked when this standalone activity reaches a terminal state. Nil
	// for workflow-embedded activities as the workflow handles its own callbacks.
	Callbacks chasm.Map[string, *callback.Callback]
}

// WithToken wraps a request with its deserialized task token.
type WithToken[R any] struct {
	Token   *tokenspb.Task
	Request R
}

// RespondCompletedEvent wraps the RespondActivityTaskCompletedRequest with context-specific data.
type RespondCompletedEvent struct {
	Request *historyservice.RespondActivityTaskCompletedRequest
	Token   *tokenspb.Task
}

// RespondFailedEvent wraps the RespondActivityTaskFailedRequest with context-specific data.
type RespondFailedEvent struct {
	Request *historyservice.RespondActivityTaskFailedRequest
	Token   *tokenspb.Task
}

// RespondCancelledEvent wraps the RespondActivityTaskCanceledRequest with context-specific data.
type RespondCancelledEvent struct {
	Request *historyservice.RespondActivityTaskCanceledRequest
	Token   *tokenspb.Task
}

func (a *Activity) isTerminal() bool {
	switch a.GetStatus() {
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return true
	default:
		return false
	}
}

// LifecycleState implements the chasm.Component interface.
func (a *Activity) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch a.Status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		return chasm.LifecycleStateCompleted
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

func (a *Activity) ContextMetadata(_ chasm.Context) map[string]string {
	md := make(map[string]string, 2)
	if actType := a.GetActivityType().GetName(); actType != "" {
		md[contextutil.MetadataKeyStandaloneActivityType] = actType
	}
	if tq := a.GetTaskQueue().GetName(); tq != "" {
		md[contextutil.MetadataKeyStandaloneActivityTaskQueue] = tq
	}
	if len(md) == 0 {
		return nil
	}
	return md
}

// NewStandaloneActivity creates a new activity component and adds associated tasks to start execution.
func NewStandaloneActivity(
	ctx chasm.MutableContext,
	request *workflowservice.StartActivityExecutionRequest,
) (*Activity, error) {
	visibility := chasm.NewVisibilityWithData(
		ctx,
		request.GetSearchAttributes().GetIndexedFields(),
		nil,
	)

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           request.ActivityType,
			TaskQueue:              request.GetTaskQueue(),
			ScheduleToCloseTimeout: request.GetScheduleToCloseTimeout(),
			ScheduleToStartTimeout: request.GetScheduleToStartTimeout(),
			StartToCloseTimeout:    request.GetStartToCloseTimeout(),
			HeartbeatTimeout:       request.GetHeartbeatTimeout(),
			RetryPolicy:            request.GetRetryPolicy(),
			Priority:               request.Priority,
			StartDelay:             request.GetStartDelay(),
			OriginalOptions: &apiactivitypb.ActivityOptions{
				TaskQueue:              request.GetTaskQueue(),
				ScheduleToCloseTimeout: request.GetScheduleToCloseTimeout(),
				ScheduleToStartTimeout: request.GetScheduleToStartTimeout(),
				StartToCloseTimeout:    request.GetStartToCloseTimeout(),
				HeartbeatTimeout:       request.GetHeartbeatTimeout(),
				RetryPolicy:            request.GetRetryPolicy(),
				Priority:               request.GetPriority(),
				StartDelay:             request.GetStartDelay(),
			},
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{}),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			Input:  request.Input,
			Header: request.Header,
			// Dual-write user_metadata to the legacy ActivityRequestData field so that a
			// rolled-back binary (which only reads from here) keeps showing it. The
			// authoritative copy lives on ChasmComponentAttributes.user_metadata; this
			// field will be dropped once a rollback to pre-migration code is no longer
			// supported.
			UserMetadata: request.GetUserMetadata(), //nolint:staticcheck // intentional dual-write for rollback safety
		}),
		Outcome:    chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
		Visibility: chasm.NewComponentField(ctx, visibility),
	}

	if md := request.GetUserMetadata(); md != nil {
		if err := ctx.SetUserMetadata(activity, md); err != nil {
			return nil, err
		}
	}

	activity.ScheduleTime = timestamppb.New(ctx.Now(activity))

	return activity, nil
}

func NewEmbeddedActivity(
	ctx chasm.MutableContext,
	state *activitypb.ActivityState,
	parent ActivityStore,
) {
}

func (a *Activity) createAddActivityTaskRequest(ctx chasm.Context, namespaceID string) (*matchingservice.AddActivityTaskRequest, error) {
	// Get latest component ref and unmarshal into proto ref
	componentRef, err := ctx.Ref(a)
	if err != nil {
		return nil, err
	}

	// Note: No need to set the vector clock here, as the components track version conflicts for read/write
	// TODO: Need to fill in VersionDirective once we decide how to handle versioning for standalone activities
	return &matchingservice.AddActivityTaskRequest{
		NamespaceId:            namespaceID,
		ScheduleToStartTimeout: a.ScheduleToStartTimeout,
		TaskQueue:              a.GetTaskQueue(),
		Priority:               a.GetPriority(),
		ComponentRef:           componentRef,
		Stamp:                  a.LastAttempt.Get(ctx).GetStamp(),
	}, nil
}

// HandleStarted updates the activity on recording activity task started and populates the response.
func (a *Activity) HandleStarted(ctx chasm.MutableContext, request *historyservice.RecordActivityTaskStartedRequest) (
	*historyservice.RecordActivityTaskStartedResponse, error,
) {
	lastAttempt := a.LastAttempt.Get(ctx)
	// If already started, return existing response if request ID matches to make retry idempotent, else error.
	if a.StateMachineState() == activitypb.ACTIVITY_EXECUTION_STATUS_STARTED && request.GetRequestId() == lastAttempt.GetStartRequestId() {
		return a.GenerateRecordActivityTaskStartedResponse(ctx, request.GetPollRequest().GetNamespace())
	}
	if lastAttempt.GetStamp() != request.GetStamp() {
		return nil, serviceerrors.NewObsoleteMatchingTask("activity attempt stamp mismatch")
	}
	if err := TransitionStarted.Apply(a, ctx, request); err != nil {
		if errors.Is(err, chasm.ErrInvalidTransition) {
			return nil, serviceerrors.NewObsoleteMatchingTask(err.Error())
		}
		return nil, err
	}
	return a.GenerateRecordActivityTaskStartedResponse(ctx, request.GetPollRequest().GetNamespace())
}

// GenerateRecordActivityTaskStartedResponse generates the response for HandleStarted.
func (a *Activity) GenerateRecordActivityTaskStartedResponse(
	ctx chasm.Context,
	namespace string,
) (*historyservice.RecordActivityTaskStartedResponse, error) {
	key := ctx.ExecutionKey()
	lastHeartbeat, _ := a.LastHeartbeat.TryGet(ctx)
	requestData := a.RequestData.Get(ctx)
	attempt := a.LastAttempt.Get(ctx)
	links := ctx.Links(a)

	return &historyservice.RecordActivityTaskStartedResponse{
		StartedTime:                 attempt.GetStartedTime(),
		Attempt:                     attempt.GetCount(),
		Priority:                    a.GetPriority(),
		RetryPolicy:                 a.GetRetryPolicy(),
		ActivityRunId:               key.RunID,
		WorkflowNamespace:           namespace,
		HeartbeatDetails:            lastHeartbeat.GetDetails(),
		CurrentAttemptScheduledTime: a.attemptScheduleTime(attempt),
		ScheduledEvent: &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			EventTime: a.GetScheduleTime(),
			Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
				ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					ActivityId:             key.BusinessID,
					ActivityType:           a.GetActivityType(),
					Input:                  requestData.GetInput(),
					Header:                 requestData.GetHeader(),
					TaskQueue:              a.GetTaskQueue(),
					ScheduleToCloseTimeout: a.GetScheduleToCloseTimeout(),
					ScheduleToStartTimeout: a.GetScheduleToStartTimeout(),
					StartToCloseTimeout:    a.GetStartToCloseTimeout(),
					HeartbeatTimeout:       a.GetHeartbeatTimeout(),
				},
			},
			Links: links,
		},
	}, nil
}

// attemptScheduleTime returns when the given attempt was scheduled to run:
// the activity's schedule time plus start delay for the first attempt, or
// calculated from dispatchTimeForRetry on retries.
func (a *Activity) attemptScheduleTime(attempt *activitypb.ActivityAttemptState) *timestamppb.Timestamp {
	if attempt.GetCount() == 1 {
		return timestamppb.New(a.firstDispatchTime())
	}
	return dispatchTimeForRetry(attempt)
}

// dispatchTimeForRetry computes the time a retried attempt will be dispatched to Matching,
// as complete_time + retry_interval. Returns nil if either field is missing or zero.
func dispatchTimeForRetry(attempt *activitypb.ActivityAttemptState) *timestamppb.Timestamp {
	retryInterval := attempt.GetCurrentRetryInterval()
	completeTime := attempt.GetCompleteTime()
	if retryInterval != nil && retryInterval.AsDuration() > 0 && completeTime != nil {
		return timestamppb.New(completeTime.AsTime().Add(retryInterval.AsDuration()))
	}
	return nil
}

// RecordCompleted applies the provided function to record activity completion.
// For standalone activities, it also triggers any registered completion callbacks.
func (a *Activity) RecordCompleted(ctx chasm.MutableContext, applyFn func(ctx chasm.MutableContext) error) error {
	if err := applyFn(ctx); err != nil {
		return err
	}
	return callback.ScheduleStandbyCallbacks(ctx, a.Callbacks)
}

func (a *Activity) addCompletionCallbacks(
	ctx chasm.MutableContext,
	requestID string,
	completionCallbacks []*commonpb.Callback,
	maxCallbacks int,
) error {
	if len(completionCallbacks) == 0 {
		return nil
	}
	if a.LifecycleState(ctx).IsClosed() {
		return serviceerror.NewFailedPrecondition("cannot attach callbacks to a closed activity")
	}

	currentCount := len(a.Callbacks)
	if len(completionCallbacks)+currentCount > maxCallbacks {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d callbacks to an activity (%d callbacks already attached)",
			maxCallbacks,
			currentCount,
		)
	}

	if a.Callbacks == nil {
		a.Callbacks = make(chasm.Map[string, *callback.Callback], len(completionCallbacks))
	}

	registrationTime := timestamppb.New(ctx.Now(a))

	for idx, cb := range completionCallbacks {
		chasmCB := &callbackspb.Callback{
			Links: cb.GetLinks(),
		}
		switch variant := cb.Variant.(type) {
		case *commonpb.Callback_Nexus_:
			chasmCB.Variant = &callbackspb.Callback_Nexus_{
				Nexus: &callbackspb.Callback_Nexus{
					Url:    variant.Nexus.GetUrl(),
					Header: variant.Nexus.GetHeader(),
				},
			}
		default:
			return serviceerror.NewInvalidArgumentf("unsupported callback variant: %T", variant)
		}

		// requestID (unique per API call) + idx (position within the request) ensures unique,idempotent callback IDs.
		id := fmt.Sprintf("%s-%d", requestID, idx)
		callbackObj := callback.NewCallback(requestID, registrationTime, &callbackspb.CallbackState{}, chasmCB)
		a.Callbacks[id] = chasm.NewComponentField(ctx, callbackObj)
	}
	return nil
}

// effectiveUserMetadata returns the activity's user metadata, preferring the
// framework-level ChasmComponentAttributes.user_metadata and falling back to
// the legacy ActivityRequestData.user_metadata for activities persisted before
// the migration.
func (a *Activity) effectiveUserMetadata(ctx chasm.Context) *sdkpb.UserMetadata {
	if md := ctx.UserMetadata(a); md != nil {
		return md
	}
	return a.RequestData.Get(ctx).GetUserMetadata() //nolint:staticcheck // deprecated, read-only fallback
}

// attachLinks records the given links on the activity keyed by requestID. Duplicates
// within the same batch are skipped. If the requestID has already been used to attach
// links the call is a no-op, making retries idempotent even after the activity has
// closed. Returns an error if the activity is closed (and the requestID is new), if
// the per-component cap would be exceeded, or if the request's per-link size,
// per-request count, or variant shape is invalid.
func (a *Activity) attachLinks(ctx chasm.MutableContext, links []*commonpb.Link, requestID string, validator *linkValidator, namespaceName string) error {
	if len(links) == 0 {
		return nil
	}
	// Idempotency check must run before IsClosed: if a prior attach succeeded but
	// the response was lost and the activity closed before the client retried, we
	// must still return success rather than FailedPrecondition for work already
	// persisted.
	priorForRequest, err := ctx.RequestLinks(a, requestID)
	if err != nil {
		return err
	}
	if len(priorForRequest) > 0 {
		return nil
	}
	if a.LifecycleState(ctx).IsClosed() {
		return serviceerror.NewFailedPrecondition("cannot attach links to a closed activity")
	}
	if err := validator.ValidateRequest(namespaceName, links); err != nil {
		return err
	}
	if err := validator.ValidateComponentTotal(namespaceName, len(ctx.Links(a)), len(links)); err != nil {
		return err
	}
	return ctx.SetRequestLinks(a, requestID, links)
}

// GetNexusCompletion returns the activity's completion data in the format required by the Nexus callback invocation.
// Implements callback.CompletionSource.
func (a *Activity) GetNexusCompletion(ctx chasm.Context, _ string) (nexusrpc.CompleteOperationOptions, error) {
	if !a.LifecycleState(ctx).IsClosed() {
		return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInternal("activity has not completed yet")
	}

	key := ctx.ExecutionKey()
	backLink := commonnexus.ConvertLinkActivityToNexusLink(&commonpb.Link_Activity{
		Namespace:  ctx.NamespaceEntry().Name().String(),
		ActivityId: key.BusinessID,
		RunId:      key.RunID,
	})

	opts := nexusrpc.CompleteOperationOptions{
		StartTime: a.GetScheduleTime().AsTime(),
		CloseTime: ctx.ExecutionInfo().CloseTime,
		Links:     []nexus.Link{backLink},
	}

	outcome := a.Outcome.Get(ctx)
	if successful := outcome.GetSuccessful(); successful != nil {
		// Successful completion: return the first output payload as the result as Nexus supports only a single payload
		var p *commonpb.Payload
		if payloads := successful.GetOutput().GetPayloads(); len(payloads) > 0 {
			p = payloads[0]
		}
		opts.Result = p
		return opts, nil
	}

	failure := a.terminalFailure(ctx)
	if failure != nil {
		state := nexus.OperationStateFailed
		message := "operation failed"
		if a.Status == activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED {
			state = nexus.OperationStateCanceled
			message = "operation canceled"
		}

		nf, err := commonnexus.TemporalFailureToNexusFailure(failure)
		if err != nil {
			return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInternalf("failed to convert failure: %v", err)
		}

		opErr := &nexus.OperationError{
			State:   state,
			Message: message,
			Cause:   &nexus.FailureError{Failure: nf},
		}
		if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		opts.Error = opErr
		return opts, nil
	}

	return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInternalf("activity in status %v has no outcome", a.Status)
}

// HandleCompleted updates the activity on activity completion.
func (a *Activity) HandleCompleted(
	ctx chasm.MutableContext,
	event RespondCompletedEvent,
) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	if err := a.validateActivityTaskToken(ctx, event.Token, event.Request.GetNamespaceId()); err != nil {
		return nil, err
	}

	metricsHandler, err := a.enrichMetricsHandler(ctx, metrics.HistoryRespondActivityTaskCompletedScope)
	if err != nil {
		return nil, err
	}

	if err := TransitionCompleted.Apply(a, ctx, completeEvent{
		req:            event.Request,
		metricsHandler: metricsHandler,
	}); err != nil {
		return nil, err
	}

	return &historyservice.RespondActivityTaskCompletedResponse{}, nil
}

// HandleFailed updates the activity on activity failure. if the activity is retryable, it will be rescheduled
// for retry instead.
func (a *Activity) HandleFailed(
	ctx chasm.MutableContext,
	event RespondFailedEvent,
) (*historyservice.RespondActivityTaskFailedResponse, error) {
	if err := a.validateActivityTaskToken(ctx, event.Token, event.Request.GetNamespaceId()); err != nil {
		return nil, err
	}

	metricsHandler, err := a.enrichMetricsHandler(ctx, metrics.HistoryRespondActivityTaskFailedScope)
	if err != nil {
		return nil, err
	}
	failure := event.Request.GetFailedRequest().GetFailure()

	appFailure := failure.GetApplicationFailureInfo()
	isRetryable := appFailure != nil &&
		!appFailure.GetNonRetryable() &&
		!slices.Contains(a.GetRetryPolicy().GetNonRetryableErrorTypes(), appFailure.GetType())

	if isRetryable {
		rescheduled, err := a.tryReschedule(ctx, appFailure.GetNextRetryDelay().AsDuration(), failure)
		if err != nil {
			return nil, err
		}
		if rescheduled {
			a.emitOnAttemptFailedMetrics(ctx, metricsHandler)

			return &historyservice.RespondActivityTaskFailedResponse{}, nil
		}
	}

	if err := TransitionFailed.Apply(a, ctx, failedEvent{
		req:            event.Request,
		metricsHandler: metricsHandler,
	}); err != nil {
		return nil, err
	}

	return &historyservice.RespondActivityTaskFailedResponse{}, nil
}

// HandleCanceled updates the activity on activity canceled.
func (a *Activity) HandleCanceled(
	ctx chasm.MutableContext,
	event RespondCancelledEvent,
) (*historyservice.RespondActivityTaskCanceledResponse, error) {
	if err := a.validateActivityTaskToken(ctx, event.Token, event.Request.GetNamespaceId()); err != nil {
		return nil, err
	}

	metricsHandler, err := a.enrichMetricsHandler(ctx, metrics.HistoryRespondActivityTaskCanceledScope)
	if err != nil {
		return nil, err
	}

	if err := TransitionCanceled.Apply(a, ctx, cancelEvent{
		details:    event.Request.GetCancelRequest().GetDetails(),
		handler:    metricsHandler,
		fromStatus: a.GetStatus(),
	}); err != nil {
		return nil, err
	}

	return &historyservice.RespondActivityTaskCanceledResponse{}, nil
}

// Terminate implements the chasm.RootComponent interface.
func (a *Activity) Terminate(
	ctx chasm.MutableContext,
	req chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	// If already in terminated state, fail if request ID is different, else no-op
	if a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED {
		newReqID := req.RequestID
		existingReqID := a.GetTerminateState().GetRequestId()

		if existingReqID != newReqID {
			return chasm.TerminateComponentResponse{}, serviceerror.NewFailedPreconditionf(
				"already terminated with request ID %s", existingReqID)
		}

		return chasm.TerminateComponentResponse{}, nil
	}

	metricsHandler, err := a.enrichMetricsHandler(ctx, metrics.ActivityTerminatedScope)
	if err != nil {
		return chasm.TerminateComponentResponse{}, err
	}
	return chasm.TerminateComponentResponse{}, TransitionTerminated.Apply(a, ctx, terminateEvent{
		request:        req,
		metricsHandler: metricsHandler,
		fromStatus:     a.GetStatus(),
	})
}

func (a *Activity) UpdateActivityExecutionOptions(
	ctx chasm.MutableContext,
	req *activitypb.UpdateActivityExecutionOptionsRequest,
) (*activitypb.UpdateActivityExecutionOptionsResponse, error) {
	switch a.Status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
		activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED:
		return nil, serviceerror.NewFailedPreconditionf("Cannot update options for activity in state %s", a.Status.String())
	default:
	}

	frontendReq := req.GetFrontendRequest()

	// start_delay updates are only valid while the activity is still in its delay window.
	var hasStartDelayInMask bool
	if mask := frontendReq.GetUpdateMask(); mask != nil {
		_, hasStartDelayInMask = util.ParseFieldMask(mask)["startDelay"]
	}
	if !frontendReq.GetRestoreOriginal() && hasStartDelayInMask {
		newDelay := frontendReq.GetActivityOptions().GetStartDelay()
		if err := validateStartDelay(newDelay); err != nil {
			return nil, err
		}
		if newDelay.AsDuration() > 0 {
			actCtx := activityContextFromChasm(ctx)
			if !actCtx.config.StartDelayEnabled(frontendReq.GetNamespace()) {
				return nil, serviceerror.NewInvalidArgument("start_delay is not enabled for this namespace")
			}
		}
		if a.GetStatus() != activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED ||
			!a.firstDispatchTime().After(ctx.Now(a)) {
			return nil, serviceerror.NewFailedPrecondition(
				"cannot update start_delay: activity is no longer in its delay window")
		}
	}

	if frontendReq.GetRestoreOriginal() {
		ogOptions := a.GetOriginalOptions()
		a.TaskQueue = common.CloneProto(ogOptions.GetTaskQueue())
		a.ScheduleToCloseTimeout = common.CloneProto(ogOptions.GetScheduleToCloseTimeout())
		a.ScheduleToStartTimeout = common.CloneProto(ogOptions.GetScheduleToStartTimeout())
		a.StartToCloseTimeout = common.CloneProto(ogOptions.GetStartToCloseTimeout())
		a.HeartbeatTimeout = common.CloneProto(ogOptions.GetHeartbeatTimeout())
		a.RetryPolicy = common.CloneProto(ogOptions.GetRetryPolicy())
		a.Priority = common.CloneProto(ogOptions.GetPriority())
		// start_delay only governs the first dispatch. Once the first attempt has started, restoring
		// the original value would shift ScheduleToClose without affecting dispatch timing.
		if a.GetFirstAttemptStartedTime() == nil {
			a.StartDelay = common.CloneProto(ogOptions.GetStartDelay())
		}
	} else {
		if err := a.mergeActivityOptions(frontendReq); err != nil {
			return nil, err
		}
	}

	attempt := a.LastAttempt.Get(ctx)

	// Recalculate the current retry interval based on the (possibly updated) retry policy.
	// This ensures a shortened retry interval takes effect immediately on re-dispatch.
	if a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED && attempt.GetCurrentRetryInterval() != nil {
		newInterval := backoff.CalculateExponentialRetryInterval(a.RetryPolicy, attempt.GetCount()-1)
		attempt.CurrentRetryInterval = durationpb.New(newInterval)
	}

	// Recreate the ScheduleToClose task at the (possibly updated) deadline.
	a.reissueScheduleToClose(ctx)

	attempt.Stamp++

	a.reissueRunningAttemptTimers(ctx, attempt)
	if a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED {
		a.reissueDispatchAndScheduleToStart(ctx, attempt)
	}

	metricsHandler, err := a.enrichMetricsHandler(ctx, metrics.ActivityUpdateOptionsScope)
	if err != nil {
		return nil, err
	}
	a.emitOnUpdateOptionsMetrics(metricsHandler)

	return &activitypb.UpdateActivityExecutionOptionsResponse{
		FrontendResponse: &workflowservice.UpdateActivityExecutionOptionsResponse{
			ActivityOptions: &apiactivitypb.ActivityOptions{
				TaskQueue:              a.GetTaskQueue(),
				ScheduleToCloseTimeout: a.GetScheduleToCloseTimeout(),
				ScheduleToStartTimeout: a.GetScheduleToStartTimeout(),
				StartToCloseTimeout:    a.GetStartToCloseTimeout(),
				HeartbeatTimeout:       a.GetHeartbeatTimeout(),
				RetryPolicy:            a.GetRetryPolicy(),
				Priority:               a.GetPriority(),
				StartDelay:             a.GetStartDelay(),
			},
		},
	}, nil
}

// mergeActivityOptions applies the field mask from the request to the activity state.
// The structure mirrors the field-mask logic in service/history/api/updateactivityoptions/api.go
func (a *Activity) mergeActivityOptions(
	req *workflowservice.UpdateActivityExecutionOptionsRequest,
) error {
	updateFields := util.ParseFieldMask(req.GetUpdateMask())

	// Build an ActivityOptions view of the current Activity state so we can use the shared merge function.
	ao := &apiactivitypb.ActivityOptions{
		TaskQueue:              a.TaskQueue,
		ScheduleToCloseTimeout: a.ScheduleToCloseTimeout,
		ScheduleToStartTimeout: a.ScheduleToStartTimeout,
		StartToCloseTimeout:    a.StartToCloseTimeout,
		HeartbeatTimeout:       a.HeartbeatTimeout,
		Priority:               a.Priority,
		RetryPolicy:            a.RetryPolicy,
		StartDelay:             a.StartDelay,
	}

	if err := activityoptions.MergeActivityOptions(ao, req.GetActivityOptions(), updateFields); err != nil {
		return err
	}

	// Re-normalize timeouts after the update so that relationships like
	// start_to_close <= schedule_to_close and heartbeat <= start_to_close are preserved.
	// This mirrors adjustActivityOptions for workflow-embedded activities.
	if err := validateAndNormalizeTimeouts(req.GetActivityId(), a.GetActivityType().GetName(), durationpb.New(0), ao); err != nil {
		return err
	}

	// Write the merged and normalized options back to the Activity state fields.
	a.TaskQueue = ao.TaskQueue
	a.ScheduleToCloseTimeout = ao.ScheduleToCloseTimeout
	a.ScheduleToStartTimeout = ao.ScheduleToStartTimeout
	a.StartToCloseTimeout = ao.StartToCloseTimeout
	a.HeartbeatTimeout = ao.HeartbeatTimeout
	a.Priority = ao.Priority
	a.RetryPolicy = ao.RetryPolicy
	a.StartDelay = ao.StartDelay

	return nil
}

// getOrCreateLastHeartbeat retrieves the last heartbeat state, initializing it if not present. The heartbeat is lazily created
// to avoid unnecessary writes when heartbeats are not used.
func (a *Activity) getOrCreateLastHeartbeat(ctx chasm.MutableContext) *activitypb.ActivityHeartbeatState {
	heartbeat, ok := a.LastHeartbeat.TryGet(ctx)
	if !ok {
		heartbeat = &activitypb.ActivityHeartbeatState{}
		a.LastHeartbeat = chasm.NewDataField(ctx, heartbeat)
	}
	return heartbeat
}

func (a *Activity) handleCancellationRequested(ctx chasm.MutableContext, request *activitypb.RequestCancelActivityExecutionRequest) (
	*activitypb.RequestCancelActivityExecutionResponse, error,
) {
	req := request.GetFrontendRequest()
	newReqID := req.GetRequestId()
	existingReqID := a.GetCancelState().GetRequestId()

	// If already in cancel requested state, fail if request ID is different, else no-op
	if a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED {
		if existingReqID != newReqID {
			return nil, serviceerror.NewFailedPrecondition(
				fmt.Sprintf("cancellation already requested with request ID %s", existingReqID))
		}

		return &activitypb.RequestCancelActivityExecutionResponse{}, nil
	}

	// SCHEDULED and PAUSED activities have no active worker token so cancel immediately.
	// STARTED and CANCEL_REQUESTED activities wait for the worker to respond.
	originalStatus := a.GetStatus()
	isCancelImmediately := originalStatus == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED ||
		originalStatus == activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED

	if err := TransitionCancelRequested.Apply(a, ctx, req); err != nil {
		return nil, err
	}

	if isCancelImmediately {
		details := &commonpb.Payloads{
			Payloads: []*commonpb.Payload{
				payload.EncodeString(req.GetReason()),
			},
		}

		metricsHandler, err := a.enrichMetricsHandler(ctx, metrics.HistoryRespondActivityTaskCanceledScope)
		if err != nil {
			return nil, err
		}
		err = TransitionCanceled.Apply(a, ctx, cancelEvent{
			details:    details,
			handler:    metricsHandler,
			fromStatus: originalStatus,
		})
		if err != nil {
			return nil, err
		}
	}

	return &activitypb.RequestCancelActivityExecutionResponse{}, nil
}

func (a *Activity) handlePauseRequested(ctx chasm.MutableContext, req *activitypb.PauseActivityExecutionRequest) (
	*activitypb.PauseActivityExecutionResponse, error,
) {
	if a.isTerminal() {
		return nil, serviceerror.NewFailedPreconditionf("activity is in terminal state %v", a.GetStatus())
	}
	if a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED {
		return nil, serviceerror.NewFailedPrecondition("cannot pause an activity with a pending cancellation")
	}
	if a.isPaused() {
		newReqID := req.GetFrontendRequest().GetRequestId()
		existingReqID := a.LastPauseState.GetRequestId()
		if newReqID != "" && existingReqID == newReqID {
			return &activitypb.PauseActivityExecutionResponse{}, nil
		}
		return nil, serviceerror.NewFailedPrecondition("activity is already paused")
	}

	metricsHandler, err := a.enrichMetricsHandler(ctx, metrics.ActivityPausedScope)
	if err != nil {
		return nil, err
	}

	event := pauseEvent{req: req.GetFrontendRequest(), metricsHandler: metricsHandler}
	switch a.GetStatus() {
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED:
		if err := TransitionPaused.Apply(a, ctx, event); err != nil {
			return nil, err
		}
	case activitypb.ACTIVITY_EXECUTION_STATUS_STARTED:
		if err := TransitionPauseRequested.Apply(a, ctx, event); err != nil {
			return nil, err
		}
	default:
		return nil, serviceerror.NewFailedPreconditionf("activity is in non-pausable state %v", a.GetStatus())
	}
	return &activitypb.PauseActivityExecutionResponse{}, nil
}

func (a *Activity) handleUnpauseRequested(ctx chasm.MutableContext, req *activitypb.UnpauseActivityExecutionRequest) (
	*activitypb.UnpauseActivityExecutionResponse, error,
) {
	if a.isTerminal() {
		return nil, serviceerror.NewFailedPreconditionf("activity is in terminal state %v", a.GetStatus())
	}
	if !a.isPaused() {
		return &activitypb.UnpauseActivityExecutionResponse{}, nil
	}

	metricsHandler, err := a.enrichMetricsHandler(ctx, metrics.ActivityUnpausedScope)
	if err != nil {
		return nil, err
	}

	event := unpauseEvent{req: req.GetFrontendRequest(), metricsHandler: metricsHandler}
	switch a.GetStatus() {
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED:
		if err := TransitionUnpaused.Apply(a, ctx, event); err != nil {
			return nil, err
		}
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
		if err := TransitionUnpausedWhilePauseRequested.Apply(a, ctx, event); err != nil {
			return nil, err
		}
	default:
		return nil, serviceerror.NewFailedPreconditionf("activity is in non-unpausable state %v", a.GetStatus())
	}
	a.emitOnUnpausedMetrics(metricsHandler)
	return &activitypb.UnpauseActivityExecutionResponse{}, nil
}

// isPaused reports whether the activity is currently paused (waiting) or has a pending pause request
// (worker still running).
func (a *Activity) isPaused() bool {
	switch a.GetStatus() {
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
		return true
	default:
		return false
	}
}

func (a *Activity) unpause(
	ctx chasm.MutableContext,
	event unpauseEvent,
) {
	attempt := a.LastAttempt.Get(ctx)
	if event.req.GetResetAttempts() {
		attempt.Count = 1
	}
	if event.req.GetResetHeartbeat() {
		a.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{})
	}
	attempt.Stamp++
	attempt.CurrentRetryInterval = nil
	unpauseTime := ctx.Now(a)
	if jitter := event.req.GetJitter().AsDuration(); jitter > 0 {
		unpauseTime = unpauseTime.Add(time.Duration(rand.Int63n(int64(jitter)))) //nolint:gosec
	}
	dispatchTime := a.dispatchTimeRespectingStartDelay(unpauseTime)
	if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
		ctx.AddTask(
			a,
			chasm.TaskAttributes{ScheduledTime: dispatchTime.Add(timeout)},
			&activitypb.ScheduleToStartTimeoutTask{Stamp: attempt.GetStamp()})
	}
	ctx.AddTask(
		a,
		chasm.TaskAttributes{ScheduledTime: dispatchTime},
		&activitypb.ActivityDispatchTask{Stamp: attempt.GetStamp()})
}

func (a *Activity) recordPauseState(
	ctx chasm.MutableContext,
	event pauseEvent,
) {
	a.LastPauseState = &activitypb.ActivityPauseState{
		PauseTime: timestamppb.New(ctx.Now(a)),
		Identity:  event.req.GetIdentity(),
		Reason:    event.req.GetReason(),
		RequestId: event.req.GetRequestId(),
	}
	a.emitOnPausedMetrics(event.metricsHandler)
}

func (a *Activity) clearHeartbeat(ctx chasm.MutableContext) {
	if hb, ok := a.LastHeartbeat.TryGet(ctx); ok {
		hb.Details = nil
		hb.RecordedTime = nil
	}
}

func (a *Activity) reset(ctx chasm.MutableContext, event resetEvent) {
	attempt := a.LastAttempt.Get(ctx)
	attempt.Count = 1
	attempt.Stamp++
	attempt.CurrentRetryInterval = nil
	if event.req.GetResetHeartbeat() {
		a.clearHeartbeat(ctx)
	}
	dispatchTime := a.dispatchTimeRespectingStartDelay(event.resetTime)
	if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
		ctx.AddTask(
			a,
			chasm.TaskAttributes{ScheduledTime: dispatchTime.Add(timeout)},
			&activitypb.ScheduleToStartTimeoutTask{Stamp: attempt.GetStamp()},
		)
	}
	ctx.AddTask(
		a,
		chasm.TaskAttributes{ScheduledTime: dispatchTime},
		&activitypb.ActivityDispatchTask{Stamp: attempt.GetStamp()},
	)
	a.emitOnResetMetrics(event.handler)
}

// handleReset handles the activity execution reset.
// For SCHEDULED/PAUSED activities: immediately re-dispatches at attempt 1.
// For STARTED activities: transitions to RESET_REQUESTED. The worker is notified via
// ActivityReset=true on its next heartbeat response and continues to use its existing task token.
// When the worker yields (failure or timeout with retries remaining), the activity transitions
// back to SCHEDULED at attempt 1 via TransitionResetAttemptFailedToScheduled.
// For CANCEL_REQUESTED activities: rejected with FailedPrecondition; cancel takes precedence.
// RestoreOriginalOptions follows the same split: applied immediately for a non-running activity, and
// deferred to the reset landing for a running one, so the in-flight attempt is never disturbed — every
// restored option takes effect on the new attempt 1.
func (a *Activity) handleReset(ctx chasm.MutableContext, req *activitypb.ResetActivityExecutionRequest) (*activitypb.ResetActivityExecutionResponse, error) {
	frontendReq := req.GetFrontendRequest()
	keepPaused := frontendReq.GetKeepPaused()

	metricsHandler, err := a.enrichMetricsHandler(ctx, metrics.ActivityResetScope)
	if err != nil {
		return nil, err
	}

	resetTime := ctx.Now(a)
	if jitter := frontendReq.GetJitter().AsDuration(); jitter > 0 {
		resetTime = resetTime.Add(time.Duration(rand.Int63n(int64(jitter)))) //nolint:gosec
	}

	restore := frontendReq.GetRestoreOriginalOptions()

	switch a.Status {

	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
		return nil, serviceerror.NewFailedPrecondition("cannot reset an activity with a pending cancellation")
	case activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED:
		// A reset is already pending on this running attempt. Accepting an overlapping reset is a
		// separate semantics question; for now reject it clearly rather than falling through to the
		// "not running" default below (the worker is still running the attempt).
		return nil, serviceerror.NewFailedPrecondition("cannot reset an activity with a pending reset")
	case activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
		if a.Status == activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED && !keepPaused {
			// Unpause; the deferred reset will apply on the next retry via STARTED->SCHEDULED.
			if err := TransitionUnpausedWhilePauseRequested.Apply(a, ctx, unpauseEvent{
				req:            &workflowservice.UnpauseActivityExecutionRequest{},
				metricsHandler: metricsHandler,
			}); err != nil {
				return nil, err
			}
		}
		// The worker is still executing under its existing task token. Defer ALL mutations to the reset
		// landing so the in-flight attempt is left completely undisturbed: the option restore, the
		// heartbeat clear, and the attempt-count rewind all take effect when the worker yields and the
		// activity lands at attempt 1 (TransitionResetAttemptFailedTo{Scheduled,Paused}). Transitioning
		// to RESET_REQUESTED keeps heartbeat/completion calls authenticating in the meantime.
		if restore {
			a.ResetRestoreOptions = true
		}
		if frontendReq.GetResetHeartbeat() {
			a.ResetHeartbeats = true
		}
		// keepPaused on a paused (PAUSE_REQUESTED) activity preserves the pause: when the worker
		// yields the activity lands back in PAUSED rather than SCHEDULED.
		a.ResetKeepPaused = keepPaused && a.Status == activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED
		if err := TransitionResetRequested.Apply(a, ctx, nil); err != nil {
			return nil, err
		}
		a.emitOnResetMetrics(metricsHandler)
		return &activitypb.ResetActivityExecutionResponse{}, nil

	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED:
		// No worker is running, so restore takes effect immediately: it governs the next dispatch when
		// the activity is unpaused. Covers both the keepPaused early return and the fallthrough below.
		if restore {
			a.restoreOriginalOptions(ctx)
		}
		if keepPaused {
			// Reset counts but keep the activity paused.
			attempt := a.LastAttempt.Get(ctx)
			attempt.Count = 1
			attempt.Stamp++
			attempt.CurrentRetryInterval = nil
			if frontendReq.GetResetHeartbeat() {
				a.clearHeartbeat(ctx)
			}
			a.emitOnResetMetrics(metricsHandler)
			return &activitypb.ResetActivityExecutionResponse{}, nil
		}
		fallthrough

	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED:
		// No worker is running: restore immediately so the re-dispatched attempt 1 uses the originals.
		// When reached via the PAUSED fallthrough above, restoreOriginalOptions already ran (a.Status is
		// still PAUSED at that point, since TransitionReset has not been applied), so guard against
		// restoring twice.
		if restore && a.Status == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED {
			a.restoreOriginalOptions(ctx)
		}
		if err := TransitionReset.Apply(a, ctx, resetEvent{
			req:       frontendReq,
			resetTime: resetTime,
			handler:   metricsHandler,
		}); err != nil {
			return nil, err
		}
		return &activitypb.ResetActivityExecutionResponse{}, nil

	default:
		// Terminal or unspecified state.
		return nil, serviceerror.NewFailedPrecondition("activity execution is not running")
	}
}

// recordScheduleToStartOrCloseTimeoutFailure records schedule-to-start or schedule-to-close timeouts. Such timeouts are not retried so we
// set the outcome failure directly and leave the attempt failure as is.
func (a *Activity) recordScheduleToStartOrCloseTimeoutFailure(ctx chasm.MutableContext, timeoutType enumspb.TimeoutType) error {
	outcome := a.Outcome.Get(ctx)

	failure := &failurepb.Failure{
		Message: fmt.Sprintf(common.FailureReasonActivityTimeout, timeoutType.String()),
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: timeoutType,
			},
		},
	}

	outcome.Variant = &activitypb.ActivityOutcome_Failed_{
		Failed: &activitypb.ActivityOutcome_Failed{
			Failure: failure,
		},
	}

	return nil
}

// applyFailedAttempt mutates activity state when a worker yields with retries remaining.
func (a *Activity) applyFailedAttempt(ctx chasm.MutableContext, event rescheduleEvent) error {
	attempt := a.LastAttempt.Get(ctx)
	attempt.Count++
	attempt.Stamp++
	return a.recordFailedAttempt(ctx, event.retryInterval, event.failure, ctx.Now(a), false)
}

// recordFailedAttempt records any failures resulting from a tried attempt, including worker application failures and
// start-to-close timeouts. Since the calls come from retried attempts we update the attempt failure info but leave
// the outcome failure empty to avoid duplication.
func (a *Activity) recordFailedAttempt(
	ctx chasm.MutableContext,
	retryInterval time.Duration,
	failure *failurepb.Failure,
	currentTime time.Time,
	noRetriesLeft bool,
) error {
	attempt := a.LastAttempt.Get(ctx)

	attempt.LastFailureDetails = &activitypb.ActivityAttemptState_LastFailureDetails{
		Failure: failure,
		Time:    timestamppb.New(currentTime),
	}
	attempt.CompleteTime = timestamppb.New(currentTime)

	if noRetriesLeft {
		attempt.CurrentRetryInterval = nil
	} else {
		attempt.CurrentRetryInterval = durationpb.New(retryInterval)
	}
	return nil
}

// tryReschedule attempts to reschedule the activity for retry. Returns true if rescheduled, false
// if retry is not possible. If a reset request has been received then the retry transitions
// through TransitionResetAttemptFailedToScheduled which applies the deferred reset (attempt count
// goes back to 1), unless the reset was issued with keepPaused (ResetKeepPaused), in which case it
// transitions through TransitionResetAttemptFailedToPaused and the activity stays paused.
func (a *Activity) tryReschedule(
	ctx chasm.MutableContext,
	overridingRetryInterval time.Duration,
	failure *failurepb.Failure,
) (bool, error) {
	shouldRetry, retryInterval := a.shouldRetry(ctx, overridingRetryInterval)
	if !shouldRetry {
		return false, nil
	}
	event := rescheduleEvent{retryInterval: retryInterval, failure: failure}
	if a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED {
		return true, TransitionAttemptFailedWhilePauseRequested.Apply(a, ctx, event)
	}
	if a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED {
		// keepPaused=true on a paused activity (ResetKeepPaused) requires the yield to land in
		// PAUSED rather than SCHEDULED so the activity stays paused until unpaused.
		if a.ResetKeepPaused {
			return true, TransitionResetAttemptFailedToPaused.Apply(a, ctx, event)
		}
		return true, TransitionResetAttemptFailedToScheduled.Apply(a, ctx, event)
	}
	return true, TransitionRescheduled.Apply(a, ctx, event)
}

func (a *Activity) shouldRetry(ctx chasm.Context, overridingRetryInterval time.Duration) (bool, time.Duration) {
	if !TransitionRescheduled.Possible(a) &&
		!TransitionAttemptFailedWhilePauseRequested.Possible(a) &&
		!TransitionResetAttemptFailedToScheduled.Possible(a) &&
		!TransitionResetAttemptFailedToPaused.Possible(a) {
		return false, 0
	}
	attempt := a.LastAttempt.Get(ctx)
	retryPolicy := a.RetryPolicy

	enoughAttempts := retryPolicy.GetMaximumAttempts() == 0 || attempt.GetCount() < retryPolicy.GetMaximumAttempts()
	enoughTime, retryInterval := a.hasEnoughTimeForRetry(ctx, overridingRetryInterval)
	return enoughAttempts && enoughTime, retryInterval
}

// hasEnoughTimeForRetry checks if there is enough time left in the schedule-to-close timeout. If sufficient time
// remains, it will also return a valid retry interval.
func (a *Activity) hasEnoughTimeForRetry(ctx chasm.Context, overridingRetryInterval time.Duration) (bool, time.Duration) {
	attempt := a.LastAttempt.Get(ctx)

	// Use overriding retry interval if provided, else calculate based on retry policy
	retryInterval := overridingRetryInterval
	if retryInterval <= 0 {
		retryInterval = backoff.CalculateExponentialRetryInterval(a.RetryPolicy, attempt.Count)
	}

	scheduleToClose := a.GetScheduleToCloseTimeout().AsDuration()
	if scheduleToClose == 0 {
		return true, retryInterval
	}

	deadline := a.scheduleToCloseDeadline()
	return ctx.Now(a).Add(retryInterval).Before(deadline), retryInterval
}

func (a *Activity) firstDispatchTime() time.Time {
	return a.ScheduleTime.AsTime().Add(a.GetStartDelay().AsDuration())
}

// reissueDispatchAndScheduleToStart re-emits the ActivityDispatchTask and ScheduleToStart timeout task for
// a SCHEDULED activity. Retries fire at the retry time; first attempts dispatch now, lifted to
// honor any pending start_delay.
func (a *Activity) reissueDispatchAndScheduleToStart(ctx chasm.MutableContext, attempt *activitypb.ActivityAttemptState) {
	var dispatchTime time.Time
	if retryDispatchTime := dispatchTimeForRetry(attempt); retryDispatchTime != nil {
		dispatchTime = retryDispatchTime.AsTime()
	} else {
		dispatchTime = a.dispatchTimeRespectingStartDelay(ctx.Now(a))
	}
	ctx.AddTask(
		a,
		chasm.TaskAttributes{ScheduledTime: dispatchTime},
		&activitypb.ActivityDispatchTask{Stamp: attempt.GetStamp()},
	)
	if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
		ctx.AddTask(
			a,
			chasm.TaskAttributes{ScheduledTime: dispatchTime.Add(timeout)},
			&activitypb.ScheduleToStartTimeoutTask{Stamp: attempt.GetStamp()},
		)
	}
}

// reissueRunningAttemptTimers re-emits the StartToClose and Heartbeat timeout tasks for the
// currently-running attempt, anchored to the attempt's StartedTime. Called from options-update
// paths after stamp bump so the old tasks are invalidated and replaced with the (possibly
// updated) timeouts. No-op unless the activity is in a status where a worker holds the task token
// (STARTED / CANCEL_REQUESTED / PAUSE_REQUESTED / RESET_REQUESTED).
func (a *Activity) reissueRunningAttemptTimers(ctx chasm.MutableContext, attempt *activitypb.ActivityAttemptState) {
	if a.GetStatus() != activitypb.ACTIVITY_EXECUTION_STATUS_STARTED &&
		a.GetStatus() != activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED &&
		a.GetStatus() != activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED &&
		a.GetStatus() != activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED {
		return
	}
	if timeout := a.GetStartToCloseTimeout().AsDuration(); timeout > 0 {
		deadline := attempt.GetStartedTime().AsTime().Add(timeout)
		ctx.AddTask(
			a,
			chasm.TaskAttributes{ScheduledTime: deadline},
			&activitypb.StartToCloseTimeoutTask{Stamp: attempt.GetStamp()},
		)
	}
	if hbTimeout := a.GetHeartbeatTimeout().AsDuration(); hbTimeout > 0 {
		// Next heartbeat fires at max(last recorded heartbeat, current attempt start) + heartbeat timeout.
		lastHb, _ := a.LastHeartbeat.TryGet(ctx)
		lastHbTime := util.MaxTime(
			lastHb.GetRecordedTime().AsTime(),
			attempt.GetStartedTime().AsTime(),
		).Add(hbTimeout)
		ctx.AddTask(
			a,
			chasm.TaskAttributes{ScheduledTime: lastHbTime},
			&activitypb.HeartbeatTimeoutTask{Stamp: attempt.GetStamp()},
		)
	}
}

// dispatchTimeRespectingStartDelay advances a candidate dispatch time t to the first dispatch time
// (ScheduleTime + start_delay) while the activity has not yet been picked up by a worker, so that
// pre-dispatch re-scheduling (unpause, reset, options update) honors any remaining start_delay.
// Returns t unchanged if the first attempt has already started.
func (a *Activity) dispatchTimeRespectingStartDelay(t time.Time) time.Time {
	if a.GetFirstAttemptStartedTime() != nil {
		return t
	}
	if dispatchTime := a.firstDispatchTime(); dispatchTime.After(t) {
		return dispatchTime
	}
	return t
}

// reissueScheduleToClose bumps ScheduleToCloseStamp and re-emits the ScheduleToClose timeout task
// at the current deadline. Called whenever an operation can change the deadline (UpdateOptions,
// Reset(RestoreOriginalOptions)).
func (a *Activity) reissueScheduleToClose(ctx chasm.MutableContext) {
	if deadline := a.scheduleToCloseDeadline(); !deadline.IsZero() {
		a.ScheduleToCloseStamp++
		ctx.AddTask(
			a,
			chasm.TaskAttributes{ScheduledTime: deadline},
			&activitypb.ScheduleToCloseTimeoutTask{Stamp: a.GetScheduleToCloseStamp()},
		)
	}
}

// applyDeferredOptionRestore applies a Reset(RestoreOriginalOptions) that was deferred because a
// worker was running an attempt at reset time (see handleReset). Called from the reset landing
// transitions so the restored options take effect on the next attempt, leaving the in-flight attempt
// undisturbed.
func (a *Activity) applyDeferredOptionRestore(ctx chasm.MutableContext) {
	if !a.ResetRestoreOptions {
		return
	}
	a.ResetRestoreOptions = false
	a.restoreOriginalOptions(ctx)
}

// restoreOriginalOptions resets the activity's options to the values it was originally scheduled with
// (Reset(RestoreOriginalOptions)) and re-arms the ScheduleToClose timer at the resulting deadline.
// start_delay is restored only if the activity has never started, since it governs the first dispatch
// alone. Callers invoke this at the point the activity (re)starts at attempt 1: immediately for a
// non-running activity, or deferred to the reset landing for a running one (see
// applyDeferredOptionRestore), so a running attempt is never disturbed.
func (a *Activity) restoreOriginalOptions(ctx chasm.MutableContext) {
	og := a.GetOriginalOptions()
	a.TaskQueue = common.CloneProto(og.GetTaskQueue())
	a.ScheduleToCloseTimeout = common.CloneProto(og.GetScheduleToCloseTimeout())
	a.ScheduleToStartTimeout = common.CloneProto(og.GetScheduleToStartTimeout())
	a.StartToCloseTimeout = common.CloneProto(og.GetStartToCloseTimeout())
	a.HeartbeatTimeout = common.CloneProto(og.GetHeartbeatTimeout())
	a.RetryPolicy = common.CloneProto(og.GetRetryPolicy())
	a.Priority = common.CloneProto(og.GetPriority())
	if a.GetFirstAttemptStartedTime() == nil {
		a.StartDelay = common.CloneProto(og.GetStartDelay())
	}
	a.reissueScheduleToClose(ctx)
}

// scheduleToCloseDeadline returns the absolute time at which the ScheduleToClose timeout expires,
// accounting for start delay. Returns zero time if no ScheduleToClose timeout is set.
func (a *Activity) scheduleToCloseDeadline() time.Time {
	timeout := a.GetScheduleToCloseTimeout().AsDuration()
	if timeout == 0 {
		return time.Time{}
	}
	return a.firstDispatchTime().Add(timeout)
}

func createStartToCloseTimeoutFailure() *failurepb.Failure {
	return &failurepb.Failure{
		Message: fmt.Sprintf(common.FailureReasonActivityTimeout, enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()),
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			},
		},
	}
}

func createHeartbeatTimeoutFailure() *failurepb.Failure {
	return &failurepb.Failure{
		Message: fmt.Sprintf(common.FailureReasonActivityTimeout, enumspb.TIMEOUT_TYPE_HEARTBEAT.String()),
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
			},
		},
	}
}

// RecordHeartbeat records a heartbeat for the activity.
func (a *Activity) RecordHeartbeat(
	ctx chasm.MutableContext,
	input WithToken[*historyservice.RecordActivityTaskHeartbeatRequest],
) (*historyservice.RecordActivityTaskHeartbeatResponse, error) {
	err := a.validateActivityTaskToken(ctx, input.Token, input.Request.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	prevHeartbeat, _ := a.LastHeartbeat.TryGet(ctx)
	a.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
		RecordedTime:        timestamppb.New(ctx.Now(a)),
		Details:             input.Request.GetHeartbeatRequest().GetDetails(),
		TotalHeartbeatCount: prevHeartbeat.GetTotalHeartbeatCount() + 1,
	})
	if heartbeatTimeout := a.GetHeartbeatTimeout().AsDuration(); heartbeatTimeout > 0 {
		ctx.AddTask(
			a,
			chasm.TaskAttributes{
				ScheduledTime: ctx.Now(a).Add(heartbeatTimeout),
			},
			&activitypb.HeartbeatTimeoutTask{
				Stamp: a.LastAttempt.Get(ctx).GetStamp(),
			},
		)
	}
	return &historyservice.RecordActivityTaskHeartbeatResponse{
		CancelRequested: a.Status == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		ActivityPaused:  a.Status == activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED || (a.Status == activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED && a.ResetKeepPaused),
		ActivityReset:   a.Status == activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
	}, nil
}

// InternalStatusToAPIStatus converts internal activity execution status to API status.
func InternalStatusToAPIStatus(status activitypb.ActivityExecutionStatus) enumspb.ActivityExecutionStatus {
	switch status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
	case activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
	default:
		panic(fmt.Sprintf("unknown activity execution status: %v", status)) //nolint:forbidigo
	}
}

func internalStatusToRunState(status activitypb.ActivityExecutionStatus) enumspb.PendingActivityState {
	switch status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED:
		return enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
	case activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED:
		// RESET_REQUESTED surfaces as STARTED externally — the worker is still executing
		// under its existing task token; the public PendingActivityState enum does not have
		// a RESET_REQUESTED variant. The reset is surfaced to the worker via
		// ActivityReset=true on its next heartbeat response.
		return enumspb.PENDING_ACTIVITY_STATE_STARTED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
		return enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED:
		return enumspb.PENDING_ACTIVITY_STATE_PAUSED
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
		return enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED:
		return enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	default:
		panic(fmt.Sprintf("unknown activity execution status: %v", status)) //nolint:forbidigo
	}
}

func (a *Activity) buildActivityExecutionInfo(ctx chasm.Context) *apiactivitypb.ActivityExecutionInfo {
	status := InternalStatusToAPIStatus(a.GetStatus())
	runState := internalStatusToRunState(a.GetStatus())

	requestData := a.RequestData.Get(ctx)
	attempt := a.LastAttempt.Get(ctx)
	heartbeat, _ := a.LastHeartbeat.TryGet(ctx)
	key := ctx.ExecutionKey()
	executionInfo := ctx.ExecutionInfo()
	var closeTime *timestamppb.Timestamp
	var executionDuration *durationpb.Duration
	if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
		executionDuration = durationpb.New(executionInfo.CloseTime.Sub(a.GetScheduleTime().AsTime()))
		closeTime = timestamppb.New(executionInfo.CloseTime)
	}

	var expirationTime *timestamppb.Timestamp
	if deadline := a.scheduleToCloseDeadline(); !deadline.IsZero() {
		expirationTime = timestamppb.New(deadline)
	}

	sa := &commonpb.SearchAttributes{
		IndexedFields: a.Visibility.Get(ctx).CustomSearchAttributes(ctx),
	}

	info := &apiactivitypb.ActivityExecutionInfo{
		ActivityId:              key.BusinessID,
		ActivityType:            a.GetActivityType(),
		ActualStartTime:         a.GetFirstAttemptStartedTime(),
		Attempt:                 attempt.GetCount(),
		CanceledReason:          a.CancelState.GetReason(),
		CloseTime:               closeTime,
		CurrentRetryInterval:    attempt.GetCurrentRetryInterval(),
		ExecutionDuration:       executionDuration,
		ExpirationTime:          expirationTime,
		Header:                  requestData.GetHeader(),
		HeartbeatDetails:        heartbeat.GetDetails(),
		HeartbeatTimeout:        a.GetHeartbeatTimeout(),
		Links:                   ctx.Links(a),
		TotalHeartbeatCount:     heartbeat.GetTotalHeartbeatCount(),
		LastAttemptCompleteTime: attempt.GetCompleteTime(),
		LastFailure:             attempt.GetLastFailureDetails().GetFailure(),
		LastHeartbeatTime:       heartbeat.GetRecordedTime(),
		LastStartedTime:         attempt.GetStartedTime(),
		LastWorkerIdentity:      attempt.GetLastWorkerIdentity(),
		SdkName:                 attempt.GetSdkName(),
		SdkVersion:              attempt.GetSdkVersion(),
		NextAttemptScheduleTime: dispatchTimeForRetry(attempt),
		Priority:                a.GetPriority(),
		RetryPolicy:             a.GetRetryPolicy(),
		RequestedStartTime:      timestamppb.New(a.firstDispatchTime()),
		RunId:                   key.RunID,
		RunState:                runState,
		ScheduleTime:            a.GetScheduleTime(),
		ScheduleToCloseTimeout:  a.GetScheduleToCloseTimeout(),
		ScheduleToStartTimeout:  a.GetScheduleToStartTimeout(),
		StartDelay:              a.GetStartDelay(),
		StartToCloseTimeout:     a.GetStartToCloseTimeout(),
		StateSizeBytes:          int64(executionInfo.ApproximateStateSize),
		StateTransitionCount:    executionInfo.StateTransitionCount,
		SearchAttributes:        sa,
		Status:                  status,
		TaskQueue:               a.GetTaskQueue().GetName(),
		UserMetadata:            a.effectiveUserMetadata(ctx),
	}

	return info
}

func (a *Activity) buildDescribeActivityExecutionResponse(
	ctx chasm.Context,
	req *activitypb.DescribeActivityExecutionRequest,
) (*activitypb.DescribeActivityExecutionResponse, error) {
	request := req.GetFrontendRequest()

	token, err := ctx.Ref(a)
	if err != nil {
		return nil, err
	}

	info := a.buildActivityExecutionInfo(ctx)

	var input *commonpb.Payloads
	if request.GetIncludeInput() {
		input = a.RequestData.Get(ctx).GetInput()
	}

	callbackInfos, err := a.buildCallbackInfos(ctx)
	if err != nil {
		return nil, err
	}

	response := &workflowservice.DescribeActivityExecutionResponse{
		Info:          info,
		RunId:         ctx.ExecutionKey().RunID,
		Input:         input,
		LongPollToken: token,
		Callbacks:     callbackInfos,
	}

	if request.GetIncludeOutcome() {
		response.Outcome = a.outcome(ctx)
	}

	return &activitypb.DescribeActivityExecutionResponse{
		FrontendResponse: response,
	}, nil
}

func (a *Activity) buildCallbackInfos(ctx chasm.Context) ([]*apiactivitypb.CallbackInfo, error) {
	if len(a.Callbacks) == 0 {
		return nil, nil
	}

	cbInfos := make([]*apiactivitypb.CallbackInfo, 0, len(a.Callbacks))
	for _, field := range a.Callbacks {
		cb := field.Get(ctx)

		cbSpec, err := cb.ToAPICallback()
		if err != nil {
			return nil, err
		}

		var state enumspb.CallbackState
		switch cb.Status {
		case callbackspb.CALLBACK_STATUS_UNSPECIFIED:
			return nil, serviceerror.NewInternal("callback with UNSPECIFIED state")
		case callbackspb.CALLBACK_STATUS_STANDBY:
			state = enumspb.CALLBACK_STATE_STANDBY
		case callbackspb.CALLBACK_STATUS_SCHEDULED:
			state = enumspb.CALLBACK_STATE_SCHEDULED
		case callbackspb.CALLBACK_STATUS_BACKING_OFF:
			state = enumspb.CALLBACK_STATE_BACKING_OFF
		case callbackspb.CALLBACK_STATUS_FAILED:
			state = enumspb.CALLBACK_STATE_FAILED
		case callbackspb.CALLBACK_STATUS_SUCCEEDED:
			state = enumspb.CALLBACK_STATE_SUCCEEDED
		default:
			return nil, serviceerror.NewInternalf("unknown callback state: %v", cb.Status)
		}

		cbInfos = append(cbInfos, &apiactivitypb.CallbackInfo{
			Trigger: &apiactivitypb.CallbackInfo_Trigger{
				Variant: &apiactivitypb.CallbackInfo_Trigger_ActivityClosed{},
			},
			Info: &callbackpb.CallbackInfo{
				Callback:                cbSpec,
				RegistrationTime:        cb.RegistrationTime,
				State:                   state,
				Attempt:                 cb.Attempt,
				LastAttemptCompleteTime: cb.LastAttemptCompleteTime,
				LastAttemptFailure:      cb.LastAttemptFailure,
				NextAttemptScheduleTime: cb.NextAttemptScheduleTime,
			},
		})
	}
	return cbInfos, nil
}

func (a *Activity) buildPollActivityExecutionResponse(
	ctx chasm.Context,
) *activitypb.PollActivityExecutionResponse {
	return &activitypb.PollActivityExecutionResponse{
		FrontendResponse: &workflowservice.PollActivityExecutionResponse{
			RunId:   ctx.ExecutionKey().RunID,
			Outcome: a.outcome(ctx),
		},
	}
}

// outcome retrieves the activity outcome (result or failure) if the activity has completed.
// Returns nil if the activity has not completed.
func (a *Activity) outcome(ctx chasm.Context) *apiactivitypb.ActivityExecutionOutcome {
	if !a.LifecycleState(ctx).IsClosed() {
		return nil
	}
	activityOutcome := a.Outcome.Get(ctx)
	if successful := activityOutcome.GetSuccessful(); successful != nil {
		return &apiactivitypb.ActivityExecutionOutcome{
			Value: &apiactivitypb.ActivityExecutionOutcome_Result{Result: successful.GetOutput()},
		}
	}
	if failure := a.terminalFailure(ctx); failure != nil {
		return &apiactivitypb.ActivityExecutionOutcome{
			Value: &apiactivitypb.ActivityExecutionOutcome_Failure{Failure: failure},
		}
	}
	return nil
}

// terminalFailure returns the failure for a closed activity. The failure may be stored in Outcome.Failed
// (terminated, canceled, timed out) or in LastAttempt.LastFailureDetails (failed after exhausting retries).
// Returns nil if no failure is found.
func (a *Activity) terminalFailure(ctx chasm.Context) *failurepb.Failure {
	if f := a.Outcome.Get(ctx).GetFailed(); f != nil {
		return f.GetFailure()
	}
	if details := a.LastAttempt.Get(ctx).GetLastFailureDetails(); details != nil {
		return details.GetFailure()
	}
	return nil
}

// StoreOrSelf returns the store for the activity. If the store is not set as a field (e.g.
// standalone activities), it returns the activity itself.
func (a *Activity) StoreOrSelf(ctx chasm.Context) ActivityStore {
	store, ok := a.Store.TryGet(ctx)
	if ok {
		return store
	}
	return a
}

// validateActivityTaskToken validates a task token against the current activity state.
func (a *Activity) validateActivityTaskToken(
	ctx chasm.Context,
	token *tokenspb.Task,
	requestNamespaceID string,
) error {
	if a.Status != activitypb.ACTIVITY_EXECUTION_STATUS_STARTED &&
		a.Status != activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED &&
		a.Status != activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED &&
		a.Status != activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED {
		return serviceerror.NewNotFound("activity task not found")
	}
	if token.Attempt != ByIDTokenAttempt && token.Attempt != a.LastAttempt.Get(ctx).GetCount() {
		return serviceerror.NewNotFound("activity task not found")
	}

	ref, err := chasm.DeserializeComponentRef(token.GetComponentRef())
	if err != nil {
		return serviceerror.NewInvalidArgument("malformed token")
	}

	// Validate that the request namespace matches the token's namespace.
	// This prevents cross-namespace token reuse attacks where an attacker could use a valid token from namespace B to
	// complete an activity in namespace A.
	if requestNamespaceID != ref.NamespaceID {
		return serviceerror.NewInvalidArgument("token does not match namespace")
	}

	return nil
}

func (a *Activity) enrichMetricsHandler(ctx chasm.Context, operationTag string) (metrics.Handler, error) {
	// activityContextFromChasm panics if the context value is missing; this is intentional and
	// indicates a library registration bug rather than a runtime error.
	actCtx := activityContextFromChasm(ctx)
	namespaceName, err := actCtx.namespaceRegistry.GetNamespaceName(namespace.ID(ctx.ExecutionKey().NamespaceID))
	if err != nil {
		return nil, err
	}
	breakdownMetricsByTaskQueue := actCtx.config.BreakdownMetricsByTaskQueue
	taskQueueFamily := a.GetTaskQueue().GetName()
	return metrics.GetPerTaskQueueFamilyScope(
		ctx.MetricsHandler(),
		namespaceName.String(),
		tqid.UnsafeTaskQueueFamily(namespaceName.String(), taskQueueFamily),
		breakdownMetricsByTaskQueue(namespaceName.String(), taskQueueFamily, enumspb.TASK_QUEUE_TYPE_ACTIVITY),
		metrics.OperationTag(operationTag),
		metrics.ActivityTypeTag(a.GetActivityType().GetName()),
		metrics.VersioningBehaviorTag(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED),
		metrics.WorkflowTypeTag(WorkflowTypeTag),
	), nil
}

func (a *Activity) emitOnAttemptTimedOutMetrics(ctx chasm.Context, handler metrics.Handler, timeoutType enumspb.TimeoutType) {
	attempt := a.LastAttempt.Get(ctx)
	startedTime := attempt.GetStartedTime().AsTime()

	latency := time.Since(startedTime)
	metrics.ActivityStartToCloseLatency.With(handler).Record(latency)

	timeoutTag := metrics.StringTag("timeout_type", timeoutType.String())
	metrics.ActivityTaskTimeout.With(handler).Record(1, timeoutTag)
}

func (a *Activity) emitOnAttemptFailedMetrics(ctx chasm.Context, handler metrics.Handler) {
	attempt := a.LastAttempt.Get(ctx)
	startedTime := attempt.GetStartedTime().AsTime()

	latency := time.Since(startedTime)
	metrics.ActivityStartToCloseLatency.With(handler).Record(latency)

	metrics.ActivityTaskFail.With(handler).Record(1)
}

func (a *Activity) emitOnCompletedMetrics(ctx chasm.Context, handler metrics.Handler) {
	attempt := a.LastAttempt.Get(ctx)
	startedTime := attempt.GetStartedTime().AsTime()

	startToCloseLatency := time.Since(startedTime)
	metrics.ActivityStartToCloseLatency.With(handler).Record(startToCloseLatency)

	scheduleToCloseLatency := time.Since(a.GetScheduleTime().AsTime())
	metrics.ActivityScheduleToCloseLatency.With(handler).Record(scheduleToCloseLatency)

	metrics.ActivitySuccess.With(handler).Record(1)
}

func (a *Activity) emitOnFailedMetrics(ctx chasm.Context, handler metrics.Handler) {
	attempt := a.LastAttempt.Get(ctx)
	startedTime := attempt.GetStartedTime().AsTime()

	startToCloseLatency := time.Since(startedTime)
	metrics.ActivityStartToCloseLatency.With(handler).Record(startToCloseLatency)

	scheduleToCloseLatency := time.Since(a.GetScheduleTime().AsTime())
	metrics.ActivityScheduleToCloseLatency.With(handler).Record(scheduleToCloseLatency)

	metrics.ActivityTaskFail.With(handler).Record(1)
	metrics.ActivityFail.With(handler).Record(1)
}

func (a *Activity) emitOnTerminatedMetrics(
	handler metrics.Handler,
) {
	// Terminated activities do not count as properly finished activities so we do not
	// record any of the latency metrics.
	metrics.ActivityTerminate.With(handler).Record(1)
}

func (a *Activity) emitOnCanceledMetrics(
	ctx chasm.Context,
	handler metrics.Handler,
	fromStatus activitypb.ActivityExecutionStatus,
) {
	// Only record start-to-close latency if a current attempt was running. If it in scheduled status, it means the current attempt never started.
	if fromStatus != activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED {
		startedTime := a.LastAttempt.Get(ctx).GetStartedTime().AsTime()
		startToCloseLatency := time.Since(startedTime)
		metrics.ActivityStartToCloseLatency.With(handler).Record(startToCloseLatency)
	}

	scheduleToCloseLatency := time.Since(a.GetScheduleTime().AsTime())
	metrics.ActivityScheduleToCloseLatency.With(handler).Record(scheduleToCloseLatency)

	metrics.ActivityCancel.With(handler).Record(1)
}

func (a *Activity) emitOnTimedOutMetrics(
	ctx chasm.Context,
	handler metrics.Handler,
	timeoutType enumspb.TimeoutType,
	fromStatus activitypb.ActivityExecutionStatus,
) {
	// Only record start-to-close latency if a current attempt was running. If it in scheduled status, it means the current attempt never started.
	if fromStatus != activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED {
		startedTime := a.LastAttempt.Get(ctx).GetStartedTime().AsTime()
		startToCloseLatency := time.Since(startedTime)
		metrics.ActivityStartToCloseLatency.With(handler).Record(startToCloseLatency)
	}

	scheduleToCloseLatency := time.Since(a.GetScheduleTime().AsTime())
	metrics.ActivityScheduleToCloseLatency.With(handler).Record(scheduleToCloseLatency)

	timeoutTag := metrics.StringTag("timeout_type", timeoutType.String())
	metrics.ActivityTaskTimeout.With(handler).Record(1, timeoutTag)
	metrics.ActivityTimeout.With(handler).Record(1, timeoutTag)
}

func (a *Activity) emitOnPausedMetrics(
	handler metrics.Handler,
) {
	metrics.ActivityPause.With(handler).Record(1)
}

func (a *Activity) emitOnUpdateOptionsMetrics(
	handler metrics.Handler,
) {
	metrics.ActivityUpdateOptions.With(handler).Record(1)
}

func (a *Activity) emitOnUnpausedMetrics(
	handler metrics.Handler,
) {
	metrics.ActivityUnpause.With(handler).Record(1)
}

func (a *Activity) emitOnResetMetrics(
	handler metrics.Handler,
) {
	metrics.ActivityReset.With(handler).Record(1)
}

// SearchAttributes implements chasm.VisibilitySearchAttributesProvider interface.
// Returns the current search attribute values for this activity execution.
func (a *Activity) SearchAttributes(_ chasm.Context) []chasm.SearchAttributeKeyValue {
	return []chasm.SearchAttributeKeyValue{
		TypeSearchAttribute.Value(a.GetActivityType().GetName()),
		StatusSearchAttribute.Value(InternalStatusToAPIStatus(a.GetStatus()).String()),
		chasm.SearchAttributeTaskQueue.Value(a.GetTaskQueue().GetName()),
	}
}
