package activity

import (
	"errors"
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/retrypolicy"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/consts"
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

func (a *Activity) hasAttemptInProgress() bool {
	switch a.GetStatus() {
	case activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED:
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
				TaskQueue:              common.CloneProto(request.GetTaskQueue()),
				ScheduleToCloseTimeout: common.CloneProto(request.GetScheduleToCloseTimeout()),
				ScheduleToStartTimeout: common.CloneProto(request.GetScheduleToStartTimeout()),
				StartToCloseTimeout:    common.CloneProto(request.GetStartToCloseTimeout()),
				HeartbeatTimeout:       common.CloneProto(request.GetHeartbeatTimeout()),
				RetryPolicy:            common.CloneProto(request.GetRetryPolicy()),
				Priority:               common.CloneProto(request.GetPriority()),
				StartDelay:             common.CloneProto(request.GetStartDelay()),
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

// HandleStarted updates the activity on recording activity task started and populates the response.
func (a *Activity) HandleStarted(ctx chasm.MutableContext, request *historyservice.RecordActivityTaskStartedRequest) (
	*historyservice.RecordActivityTaskStartedResponse, error,
) {
	lastAttempt := a.LastAttempt.Get(ctx)
	// Return the existing response for a matching retry while the attempt is still in progress.
	if a.hasAttemptInProgress() && request.GetRequestId() == lastAttempt.GetStartRequestId() {
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
	if dispatchTime := a.dispatchTimeForAttempt(lastAttempt); dispatchTime != nil {
		metrics.TaskScheduleToStartLatency.With(a.taskScheduleToStartMetricsHandler(ctx)).Record(
			lastAttempt.GetStartedTime().AsTime().Sub(dispatchTime.AsTime()),
		)
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
		CurrentAttemptScheduledTime: a.dispatchTimeForAttempt(attempt),
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
	if err := validator.ValidateTotal(namespaceName, len(ctx.Links(a)), len(links)); err != nil {
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
	if err := a.validateActivityTaskToken(ctx, event.Token, event.Request.GetNamespaceId(), true); err != nil {
		return nil, err
	}

	baseHandler := a.baseMetricsHandler(ctx, metrics.HistoryRespondActivityTaskCompletedScope)
	enrichedHandler := a.enrichedMetricsHandler(ctx, metrics.HistoryRespondActivityTaskCompletedScope)

	if err := TransitionCompleted.Apply(a, ctx, completeEvent{
		req:             event.Request,
		baseHandler:     baseHandler,
		enrichedHandler: enrichedHandler,
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
	if err := a.validateActivityTaskToken(ctx, event.Token, event.Request.GetNamespaceId(), false); err != nil {
		return nil, err
	}

	baseHandler := a.baseMetricsHandler(ctx, metrics.HistoryRespondActivityTaskFailedScope)
	enrichedHandler := a.enrichedMetricsHandler(ctx, metrics.HistoryRespondActivityTaskFailedScope)
	failedRequest := event.Request.GetFailedRequest()
	failure := failedRequest.GetFailure()

	if details := failedRequest.GetLastHeartbeatDetails(); details != nil {
		heartbeat := a.getOrCreateLastHeartbeat(ctx)
		heartbeat.Details = details
		heartbeat.RecordedTime = timestamppb.New(ctx.Now(a))
		heartbeat.TotalHeartbeatCount++
		a.emitHeartbeatMetrics(ctx, details)
	}

	nextRetryDelay := failure.GetApplicationFailureInfo().GetNextRetryDelay().AsDuration()
	retryState, err := a.tryReschedule(
		ctx,
		retrypolicy.IsRetryableFailure(failure, a.GetRetryPolicy().GetNonRetryableErrorTypes()),
		nextRetryDelay,
		failure,
	)
	if err != nil {
		return nil, err
	}
	if retryState == enumspb.RETRY_STATE_IN_PROGRESS {
		a.emitOnAttemptFailedMetrics(ctx, enrichedHandler)

		return &historyservice.RespondActivityTaskFailedResponse{}, nil
	}

	if err := TransitionFailed.Apply(a, ctx, failedEvent{
		req:             event.Request,
		retryState:      retryState,
		baseHandler:     baseHandler,
		enrichedHandler: enrichedHandler,
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
	if err := a.validateActivityTaskToken(ctx, event.Token, event.Request.GetNamespaceId(), false); err != nil {
		return nil, err
	}
	if !TransitionCanceled.Possible(a) {
		return nil, consts.ErrActivityTaskNotCancelRequested
	}

	metricsHandler := a.enrichedMetricsHandler(ctx, metrics.HistoryRespondActivityTaskCanceledScope)

	if err := TransitionCanceled.Apply(a, ctx, cancelEvent{
		details:        event.Request.GetCancelRequest().GetDetails(),
		metricsHandler: metricsHandler,
		fromStatus:     a.GetStatus(),
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

	metricsHandler := a.enrichedMetricsHandler(ctx, metrics.ActivityTerminatedScope)
	return chasm.TerminateComponentResponse{}, TransitionTerminated.Apply(a, ctx, terminateEvent{
		request:        req,
		metricsHandler: metricsHandler,
		fromStatus:     a.GetStatus(),
	})
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

// lastHeartbeatDetails returns the details recorded by the most recent heartbeat, or nil if
// the activity never heartbeated.
func (a *Activity) lastHeartbeatDetails(ctx chasm.Context) *commonpb.Payloads {
	heartbeat, ok := a.LastHeartbeat.TryGet(ctx)
	if !ok {
		return nil
	}
	return heartbeat.GetDetails()
}

// RecordHeartbeat records a heartbeat for the activity.
func (a *Activity) RecordHeartbeat(
	ctx chasm.MutableContext,
	input WithToken[*historyservice.RecordActivityTaskHeartbeatRequest],
) (*historyservice.RecordActivityTaskHeartbeatResponse, error) {
	err := a.validateActivityTaskToken(ctx, input.Token, input.Request.GetNamespaceId(), false)
	if err != nil {
		return nil, err
	}
	details := input.Request.GetHeartbeatRequest().GetDetails()
	prevHeartbeat, _ := a.LastHeartbeat.TryGet(ctx)
	a.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
		RecordedTime:        timestamppb.New(ctx.Now(a)),
		Details:             details,
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
	a.emitHeartbeatMetrics(ctx, details)

	response := &historyservice.RecordActivityTaskHeartbeatResponse{}
	switch a.Status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
		response.CancelRequested = true
	case activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED:
		response.ActivityReset = true
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
		response.ActivityPaused = true
	default:
		// no-op
	}
	return response, nil
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
//
// allowForceCompleteWithNoAttempt permits a by-ID token to pass even though the activity has no
// attempt in progress (Scheduled or Paused). Only HandleCompleted sets this, mirroring the
// workflow-activity behavior of letting RespondActivityTaskCompletedById force-complete an
// activity before any worker has started it.
func (a *Activity) validateActivityTaskToken(
	ctx chasm.Context,
	token *tokenspb.Task,
	requestNamespaceID string,
	allowForceCompleteWithNoAttempt bool,
) error {
	forceCompleteWithNoAttempt := allowForceCompleteWithNoAttempt &&
		token.Attempt == ByIDTokenAttempt &&
		!a.hasAttemptInProgress()
	if !a.hasAttemptInProgress() && !forceCompleteWithNoAttempt {
		return serviceerror.NewNotFound("activity task not found")
	}
	if token.Attempt != ByIDTokenAttempt && token.Attempt != a.LastAttempt.Get(ctx).GetCount() {
		return serviceerror.NewNotFound("activity task not found")
	}
	tokenStamp := token.GetActivityAttemptStamp()
	startedStamp := a.LastAttempt.Get(ctx).GetStartedStamp()
	// Matching versions without stamped tokens leave tokenStamp zero;
	// History versions without StartedStamp persistence leave startedStamp zero.
	requiresLegacyStampCompatibility := tokenStamp == 0 || startedStamp == 0
	if !requiresLegacyStampCompatibility && tokenStamp != startedStamp {
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

// SearchAttributes implements chasm.VisibilitySearchAttributesProvider interface.
// Returns the current search attribute values for this activity execution.
func (a *Activity) SearchAttributes(_ chasm.Context) []chasm.SearchAttributeKeyValue {
	return []chasm.SearchAttributeKeyValue{
		TypeSearchAttribute.Value(a.GetActivityType().GetName()),
		StatusSearchAttribute.Value(InternalStatusToAPIStatus(a.GetStatus()).String()),
		chasm.SearchAttributeTaskQueue.Value(a.GetTaskQueue().GetName()),
		chasm.SearchAttributeExecutionTime.Value(a.firstDispatchTime()),
	}
}
