package activity

// Model-conformance explorer for the activity archetype. It drives the event alphabet of
// chasm/lib/activity/model against a real in-memory CHASM engine (chasm/chasmtest) with a virtual
// clock, and checks every step against that model. Timeouts and backoffs are realized by advancing
// clock.EventTimeSource, so the BFS traversal and random walk pay no wall-clock waits. The counterpart
// explorer over a onebox server is in tests/activity_standalone_conformance.go.

import (
	"context"
	"errors"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas // activitypb is the generated Activity component package
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const testNamespaceID = "activity-test-ns"

// driver holds a registry, an in-memory engine, and a virtual clock, shared across the fresh
// activities a traversal or walk starts.
type driver struct {
	t          *testing.T
	ctx        context.Context
	engine     *chasmtest.Engine
	ts         *clock.EventTimeSource
	cfg        model.Config
	nowStart   time.Time
	numStarted int
}

func newDriver(t *testing.T, cfg model.Config) *driver {
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	// start_delay is off by default; the explorer drives UpdateOptions[setsStartDelay], and onebox has
	// it enabled, so without this the two tiers would reject that event differently.
	config := ConfigProvider(dynamicconfig.NewCollection(
		dynamicconfig.StaticClient{StartDelayEnabled.Key(): true}, log.NewNoopLogger()))
	require.NoError(t, registry.Register(newLibrary(
		nil,
		newActivityDispatchTaskHandler(activityDispatchTaskHandlerOptions{}),
		newScheduleToStartTimeoutTaskHandler(),
		newScheduleToCloseTimeoutTaskHandler(),
		newStartToCloseTimeoutTaskHandler(),
		newHeartbeatTimeoutTaskHandler(),
		config,
	)))

	ts := clock.NewEventTimeSource()
	now := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	ts.Update(now)
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts))
	return &driver{
		t:      t,
		ctx:    chasm.NewEngineContext(context.Background(), engine),
		engine: engine,
		ts:     ts, cfg: cfg, nowStart: now,
	}
}

// handle is a handle to one activity instance.
type handle struct {
	d    *driver
	ref  chasm.ComponentRef
	path []model.Event
	// stamp deltas across the last observed() read; see apply.
	prevStamp, curStamp       int32
	prevSTCStamp, curSTCStamp int32
	lastHeartbeat             *historyservice.RecordActivityTaskHeartbeatResponse
	// establishedReqID[eventType] is the request id that established the current state for an operator
	// command, which a SameRequestID replay of that command reuses. lastReqID is the id the most recent
	// such command carried; apply promotes it once that command changes state.
	establishedReqID map[model.EventType]string
	lastReqID        string
}

// backoffInterval is the retry backoff. The virtual clock is advanced over it, so it can be long.
const backoffInterval = 30 * time.Second

// startDelayInterval is the first-dispatch delay when cfg.HasStartDelay is set, long enough that the
// first dispatch is always still in the future.
const startDelayInterval = time.Hour

func (d *driver) start() *handle {
	d.ts.Update(d.nowStart) // fresh activities all start at the same virtual instant
	d.numStarted++
	id := "test-activity"
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:           testNamespaceID,
		ActivityId:          id,
		ActivityType:        &commonpb.ActivityType{Name: "test-activity-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: id},
		StartToCloseTimeout: durationpb.New(time.Hour),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(backoffInterval), BackoffCoefficient: 1.0,
			MaximumInterval: durationpb.New(backoffInterval), MaximumAttempts: d.cfg.MaxAttempts,
		},
		RequestId: uuid.NewString(),
	}
	if d.cfg.HasScheduleToClose {
		req.ScheduleToCloseTimeout = durationpb.New(24 * time.Hour)
	}
	if d.cfg.HasHeartbeat {
		req.HeartbeatTimeout = durationpb.New(10 * time.Minute)
	}
	if d.cfg.HasStartDelay {
		req.StartDelay = durationpb.New(startDelayInterval)
	}
	// Terminate any prior run, so business-id reuse does not conflict.
	key := chasm.ExecutionKey{NamespaceID: testNamespaceID, BusinessID: id}
	result, err := chasm.StartExecution(d.ctx, key,
		func(mc chasm.MutableContext, r *workflowservice.StartActivityExecutionRequest) (*Activity, error) {
			a, err := NewStandaloneActivity(mc, r)
			if err != nil {
				return nil, err
			}
			return a, TransitionScheduled.Apply(a, mc, nil)
		},
		req,
		chasm.WithRequestID(req.RequestId),
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyAllowDuplicate, chasm.BusinessIDConflictPolicyTerminateExisting),
	)
	require.NoError(d.t, err)
	return &handle{
		d: d,
		ref: chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
			NamespaceID: testNamespaceID, BusinessID: id, RunID: result.ExecutionKey.RunID,
		}),
		establishedReqID: map[model.EventType]string{},
	}
}

// observed is the activity's internal state as the model's AbstractState, refreshing the stamp deltas.
func (a *handle) observed() model.AbstractState {
	o, err := chasm.ReadComponent(a.d.ctx, a.ref, func(act *Activity, cctx chasm.Context, _ struct{}) (model.Observed, error) {
		attempt := act.LastAttempt.Get(cctx)
		return model.Observed{
			Status:               act.GetStatus(),
			Count:                attempt.GetCount(),
			Stamp:                attempt.GetStamp(),
			ScheduleToCloseStamp: act.GetScheduleToCloseStamp(),
			ResetKeepPaused:      act.GetResetShouldPause(),
			ResetRestoreOptions:  act.GetResetRestoreOptions(),
			FirstAttemptStarted:  act.GetFirstAttemptStartedTime() != nil,
			DispatchTimeSet:      attempt.GetDispatchTime() != nil,
		}, nil
	}, struct{}{})
	require.NoError(a.d.t, err)
	a.prevStamp, a.curStamp = a.curStamp, o.Stamp
	a.prevSTCStamp, a.curSTCStamp = a.curSTCStamp, o.ScheduleToCloseStamp
	return model.Abstract(o)
}

// describe is the public status, run state, and attempt, via the production Describe builder.
func (a *handle) describe() (enumspb.ActivityExecutionStatus, enumspb.PendingActivityState, int32) {
	resp, err := chasm.ReadComponent(a.d.ctx, a.ref, func(act *Activity, cctx chasm.Context, req *activitypb.DescribeActivityExecutionRequest) (*activitypb.DescribeActivityExecutionResponse, error) {
		return act.buildDescribeActivityExecutionResponse(cctx, req)
	}, &activitypb.DescribeActivityExecutionRequest{})
	require.NoError(a.d.t, err)
	info := resp.GetFrontendResponse().GetInfo()
	return info.GetStatus(), info.GetRunState(), info.GetAttempt()
}

func (a *handle) read(fn func(*Activity, chasm.Context) any) any {
	v, err := chasm.ReadComponent(a.d.ctx, a.ref, func(act *Activity, cctx chasm.Context, _ struct{}) (any, error) {
		return fn(act, cctx), nil
	}, struct{}{})
	require.NoError(a.d.t, err)
	return v
}

func (a *handle) stamp() int32 {
	return a.read(func(act *Activity, c chasm.Context) any { return act.LastAttempt.Get(c).GetStamp() }).(int32)
}

func (a *handle) token() *tokenspb.Task {
	return a.read(func(act *Activity, c chasm.Context) any {
		refBytes, _ := c.Ref(act)
		return &tokenspb.Task{ComponentRef: refBytes, Attempt: act.LastAttempt.Get(c).GetCount()}
	}).(*tokenspb.Task)
}

func (a *handle) update(fn func(*Activity, chasm.MutableContext) error) error {
	_, _, err := chasm.UpdateComponent(a.d.ctx, a.ref, func(act *Activity, mc chasm.MutableContext, _ any) (any, error) {
		return nil, fn(act, mc)
	}, nil)
	return err
}

// rpc realizes a non-Poll, non-wall-clock event by invoking the component method its worker RPC would,
// and returns the reject error, nil on accept.
func (a *handle) rpc(e model.Event) error {
	switch e.Type {
	case model.HeartbeatType:
		return a.update(func(act *Activity, mc chasm.MutableContext) error {
			resp, err := act.RecordHeartbeat(mc, WithToken[*historyservice.RecordActivityTaskHeartbeatRequest]{
				Token: a.token(),
				Request: &historyservice.RecordActivityTaskHeartbeatRequest{
					NamespaceId:      testNamespaceID,
					HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{Identity: "worker"},
				},
			})
			a.lastHeartbeat = resp
			return err
		})
	case model.RespondCompletedType:
		return a.update(func(act *Activity, mc chasm.MutableContext) error {
			_, err := act.HandleCompleted(mc, RespondCompletedEvent{Token: a.token(), Request: &historyservice.RespondActivityTaskCompletedRequest{
				NamespaceId:     testNamespaceID,
				CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{Identity: "worker"},
			}})
			return err
		})
	case model.RespondFailedType:
		return a.update(func(act *Activity, mc chasm.MutableContext) error {
			_, err := act.HandleFailed(mc, RespondFailedEvent{Token: a.token(), Request: &historyservice.RespondActivityTaskFailedRequest{
				NamespaceId: testNamespaceID,
				FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{Identity: "worker",
					Failure: &failurepb.Failure{Message: "drive", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "drive", NonRetryable: e.Failure != nil && e.Failure.NonRetryable}}}},
			}})
			return err
		})
	case model.RespondCanceledType:
		return a.update(func(act *Activity, mc chasm.MutableContext) error {
			_, err := act.HandleCanceled(mc, RespondCancelledEvent{Token: a.token(), Request: &historyservice.RespondActivityTaskCanceledRequest{
				NamespaceId:   testNamespaceID,
				CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{Identity: "worker"},
			}})
			return err
		})
	case model.PauseType:
		return a.update(func(act *Activity, mc chasm.MutableContext) error {
			_, err := act.handlePauseRequested(mc, &activitypb.PauseActivityExecutionRequest{
				FrontendRequest: &workflowservice.PauseActivityExecutionRequest{
					Identity: "operator", Reason: "drive", RequestId: a.reqID(e),
				},
			})
			return err
		})
	case model.UnpauseType:
		return a.update(func(act *Activity, mc chasm.MutableContext) error {
			_, err := act.handleUnpauseRequested(mc, &activitypb.UnpauseActivityExecutionRequest{
				FrontendRequest: &workflowservice.UnpauseActivityExecutionRequest{Identity: "operator"},
			})
			return err
		})
	case model.ResetType:
		return a.update(func(act *Activity, mc chasm.MutableContext) error {
			_, err := act.handleReset(mc, &activitypb.ResetActivityExecutionRequest{
				FrontendRequest: &workflowservice.ResetActivityExecutionRequest{
					Identity: "operator", KeepPaused: e.KeepPaused, RestoreOriginalOptions: e.RestoreOriginal,
					RequestId: a.reqID(e),
				},
			})
			return err
		})
	case model.UpdateOptionsType:
		return a.update(func(act *Activity, mc chasm.MutableContext) error {
			_, err := act.UpdateActivityExecutionOptions(mc, &activitypb.UpdateActivityExecutionOptionsRequest{
				FrontendRequest: updateOptionsRequest(e),
			})
			return err
		})
	case model.RequestCancelType:
		return a.update(func(act *Activity, mc chasm.MutableContext) error {
			_, err := act.handleCancellationRequested(mc, &activitypb.RequestCancelActivityExecutionRequest{
				FrontendRequest: &workflowservice.RequestCancelActivityExecutionRequest{
					Identity: "operator", Reason: "drive", RequestId: a.reqID(e),
				},
			})
			return err
		})
	case model.TerminateType:
		return a.update(func(act *Activity, mc chasm.MutableContext) error {
			_, err := act.Terminate(mc, chasm.TerminateComponentRequest{
				Identity: "operator", Reason: "drive", RequestID: a.reqID(e),
			})
			return err
		})
	default:
		a.d.t.Fatalf("unhandled rpc type %v", e.Type)
		return nil
	}
}

// updateOptionsRequest is the frontend request an UpdateOptions event carries: a restore, a start-delay
// set, or a minimal always-valid update that re-sets the heartbeat timeout.
func updateOptionsRequest(e model.Event) *workflowservice.UpdateActivityExecutionOptionsRequest {
	req := &workflowservice.UpdateActivityExecutionOptionsRequest{Identity: "operator"}
	switch {
	case e.RestoreOriginal:
		req.RestoreOriginal = true
	case e.SetsStartDelay:
		req.ActivityOptions = &apiactivitypb.ActivityOptions{StartDelay: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}}
	default:
		req.ActivityOptions = &apiactivitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}}
	}
	return req
}

// reqID is the request id an operator command carries: the id that established the current state for
// that command type when the event is a SameRequestID replay, else a fresh one.
func (a *handle) reqID(e model.Event) string {
	id := uuid.NewString()
	if e.SameRequestID {
		if est, ok := a.establishedReqID[e.Type]; ok {
			id = est
		}
	}
	a.lastReqID = id
	return id
}

// dispatchable reports whether a SCHEDULED attempt's dispatch time has arrived, so that a poll
// would return a task.
func (a *handle) dispatchable() bool {
	return a.read(func(act *Activity, c chasm.Context) any {
		if act.GetStatus() != activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED {
			return false
		}
		dt := act.LastAttempt.Get(c).GetDispatchTime()
		return dt == nil || !dt.AsTime().After(c.Now(act))
	}).(bool)
}

// realize applies one event to the activity and returns the reject error, nil on accept or no-op.
func (a *handle) realize(e model.Event) error {
	switch e.Type {
	case model.PollType:
		if a.dispatchable() {
			stamp := a.stamp()
			return a.update(func(act *Activity, mc chasm.MutableContext) error {
				_, err := act.HandleStarted(mc, &historyservice.RecordActivityTaskStartedRequest{
					Stamp:       stamp,
					PollRequest: &workflowservice.PollActivityTaskQueueRequest{Namespace: testNamespaceID, Identity: "worker"},
				})
				return err
			})
		}
		return nil // not dispatchable: poll finds nothing
	case model.BackoffElapsesType:
		a.d.ts.Update(a.d.ts.Now().Add(backoffInterval + time.Second))
		return nil
	case model.StartToCloseElapsesType:
		a.advanceTo(a.timerDeadline(e.Type))
		handler, task := newStartToCloseTimeoutTaskHandler(), &activitypb.StartToCloseTimeoutTask{Stamp: a.stamp()}
		return a.fireTimer(func(act *Activity, mc chasm.MutableContext) (bool, error) {
			return handler.Validate(mc, act, chasm.TaskInvocation{}, task)
		}, func(act *Activity, mc chasm.MutableContext) error {
			return handler.Execute(mc, act, chasm.TaskAttributes{}, task)
		})
	case model.HeartbeatElapsesType:
		deadline := a.timerDeadline(e.Type)
		a.advanceTo(deadline)
		handler, task := newHeartbeatTimeoutTaskHandler(), &activitypb.HeartbeatTimeoutTask{Stamp: a.stamp()}
		return a.fireTimer(func(act *Activity, mc chasm.MutableContext) (bool, error) {
			return handler.Validate(mc, act, chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: deadline}}, task)
		}, func(act *Activity, mc chasm.MutableContext) error {
			return handler.Execute(mc, act, chasm.TaskAttributes{}, task)
		})
	case model.ScheduleToCloseElapsesType:
		a.advanceTo(a.timerDeadline(e.Type))
		stc := a.read(func(act *Activity, c chasm.Context) any { return act.GetScheduleToCloseStamp() }).(int32)
		handler, task := newScheduleToCloseTimeoutTaskHandler(), &activitypb.ScheduleToCloseTimeoutTask{Stamp: stc}
		return a.fireTimer(func(act *Activity, mc chasm.MutableContext) (bool, error) {
			return handler.Validate(mc, act, chasm.TaskInvocation{}, task)
		}, func(act *Activity, mc chasm.MutableContext) error {
			return handler.Execute(mc, act, chasm.TaskAttributes{}, task)
		})
	default:
		return a.rpc(e)
	}
}

// timerDeadline is the instant the given timeout's timer is due, computed from current state exactly as
// the transition that scheduled it did. Zero when the timer does not apply in the current state, such as
// a start-to-close timer while not started; firing it then is a validated no-op.
func (a *handle) timerDeadline(eventType model.EventType) time.Time {
	return a.read(func(act *Activity, c chasm.Context) any {
		attempt := act.LastAttempt.Get(c)
		switch eventType {
		case model.StartToCloseElapsesType:
			// StartedTime is carried across a reschedule, so gate on the attempt being in progress. Otherwise a
			// backing-off attempt yields a stale deadline, advancing the clock for a timer its own Validate
			// then rejects.
			if !act.hasAttemptInProgress() {
				return time.Time{}
			}
			return attempt.GetStartedTime().AsTime().Add(act.GetStartToCloseTimeout().AsDuration())
		case model.HeartbeatElapsesType:
			if !act.hasAttemptInProgress() {
				return time.Time{}
			}
			base := attempt.GetStartedTime().AsTime()
			if lastHb, ok := act.LastHeartbeat.TryGet(c); ok && lastHb.GetRecordedTime() != nil {
				if t := lastHb.GetRecordedTime().AsTime(); t.After(base) {
					base = t
				}
			}
			return base.Add(act.GetHeartbeatTimeout().AsDuration())
		case model.ScheduleToCloseElapsesType:
			return act.scheduleToCloseDeadline()
		default:
			return time.Time{}
		}
	}).(time.Time)
}

// advanceTo moves the virtual clock just past deadline if that is in the future. Otherwise a no-op, so
// an inapplicable timer fires at the current instant and is rejected by its own Validate.
func (a *handle) advanceTo(deadline time.Time) {
	if !deadline.IsZero() && deadline.After(a.d.ts.Now()) {
		a.d.ts.Update(deadline.Add(time.Second))
	}
}

// fireTimer runs a pure timeout task handler, Validate then Execute, as the task processor would.
func (a *handle) fireTimer(validate func(*Activity, chasm.MutableContext) (bool, error), execute func(*Activity, chasm.MutableContext) error) error {
	return a.update(func(act *Activity, mc chasm.MutableContext) error {
		if ok, err := validate(act, mc); err != nil || !ok {
			return err
		}
		return execute(act, mc)
	})
}

// trackRequestID maintains the ids a SameRequestID replay reuses. An id is established by the request
// that changed state, and dropped once the model stops treating that command's replay as an idempotent
// no-op: beyond that point the server dedupes the consumed id while the model, which keeps no id
// history, expects a fresh command.
func (a *handle) trackRequestID(e model.Event, cur model.AbstractState, out model.Outcome, got model.ErrorKind) {
	if model.CarriesReqID(e.Type) && got == model.NoError && out.Next.Status != cur.Status {
		a.establishedReqID[e.Type] = a.lastReqID
	}
	for k := range a.establishedReqID {
		probe := model.Transition(a.d.cfg, out.Next, model.Event{Type: k, SameRequestID: true})
		if probe.Reject != model.NoError || !probe.Next.SameObserved(out.Next) {
			delete(a.establishedReqID, k)
		}
	}
}

func rejectKind(err error) model.ErrorKind {
	if err == nil {
		return model.NoError
	}
	var nf *serviceerror.NotFound
	var fp *serviceerror.FailedPrecondition
	var ia *serviceerror.InvalidArgument
	switch {
	case errors.As(err, &nf):
		return model.NotFound
	case errors.As(err, &fp):
		return model.FailedPrecondition
	case errors.As(err, &ia):
		return model.InvalidArgument
	default:
		return model.ErrorKind(-1)
	}
}

// --- conformance + explorers -----------------------------------------------------------------

// candidateEvents is the event alphabet: the worker RPCs plus the wall-clock timeouts and backoff.
//
// A timeout the config omits is left out rather than left to model.Possible to reject, because this
// explorer realizes a timeout by constructing its task and firing it. Nothing scheduled that task, so
// an unconfigured timeout would fire anyway and the server would time the attempt out — a divergence
// manufactured by the explorer.
func (d *driver) candidateEvents() []model.Event {
	events := []model.Event{
		{Type: model.PollType},
		{Type: model.HeartbeatType},
		{Type: model.RespondCompletedType},
		{Type: model.RespondFailedType, Failure: &model.Failure{}},
		{Type: model.RespondFailedType, Failure: &model.Failure{NonRetryable: true}},
		{Type: model.RespondCanceledType},
		{Type: model.BackoffElapsesType},
		{Type: model.StartToCloseElapsesType},
		{Type: model.UnpauseType},
		{Type: model.UpdateOptionsType},
		// TODO(dan): UpdateOptions[setsStartDelay] is excluded. It opens a start-delay window, and this
		// explorer realizes a timeout by advancing the virtual clock to that timeout's deadline — an
		// advance that silently elapses the start delay too, while the model still holds
		// StartDelayPending because no StartDelayElapses event was driven. Driving it needs advanceTo to
		// refuse to step over an earlier pending deadline.
	}
	for _, sameReqID := range []bool{false, true} {
		events = append(events,
			model.Event{Type: model.PauseType, SameRequestID: sameReqID},
			model.Event{Type: model.RequestCancelType, SameRequestID: sameReqID},
			model.Event{Type: model.TerminateType, SameRequestID: sameReqID},
		)
	}
	for _, keepPaused := range []bool{false, true} {
		for _, restoreOriginal := range []bool{false, true} {
			events = append(events, model.Event{Type: model.ResetType, KeepPaused: keepPaused, RestoreOriginal: restoreOriginal})
		}
	}
	if d.cfg.HasHeartbeat {
		events = append(events, model.Event{Type: model.HeartbeatElapsesType})
	}
	if d.cfg.HasScheduleToClose {
		events = append(events, model.Event{Type: model.ScheduleToCloseElapsesType})
	}
	return events
}

// possibleEvents is the alphabet restricted to what can occur in state s, so a walk spends its steps on
// events that do something rather than on clocks that are not running. The traversal deliberately does
// not filter: driving a clock that is stopped is how it checks that a stale timer is invalidated.
func (d *driver) possibleEvents(s model.AbstractState) []model.Event {
	var events []model.Event
	for _, e := range d.candidateEvents() {
		if model.Possible(d.cfg, s, e.Type) {
			events = append(events, e)
		}
	}
	return events
}

// verifyPath starts a fresh activity, replays path, and checks its final edge against the model. A
// prefix divergence aborts the replay silently; that edge is reported when it is the final edge of its
// own shorter path. Reports whether the final edge verified.
func (d *driver) verifyPath(path []model.Event) bool {
	a := d.start()
	a.path = path
	cur := model.Initial(d.cfg)
	if obs := a.observed(); !cur.SameObserved(obs) {
		d.t.Errorf("cfg %+v: state after Start disagrees with Initial\n  observed=%s want=%s", d.cfg, model.Fingerprint(obs), model.Fingerprint(cur))
		return false
	}
	for i, e := range path {
		out := model.Transition(d.cfg, cur, e)
		final := i == len(path)-1
		if !a.apply(e, cur, out, final) {
			return false
		}
		cur = out.Next
	}
	return true
}

// apply realizes e, whose predicted outcome from cur is out, and checks the observed reject kind, state,
// public Describe, task invalidation, and — for Poll — dispatch readiness against the model. It reports
// only on the final edge.
func (a *handle) apply(e model.Event, cur model.AbstractState, out model.Outcome, final bool) bool {
	if e.Type == model.PollType && cur.Status == model.Scheduled {
		wantDispatchable := cur.Dispatchability == model.Dispatchable
		if a.dispatchable() != wantDispatchable {
			if final {
				a.d.t.Errorf("%s: dispatch readiness disagrees — driver=%v model=%v\n  path: %s",
					e, a.dispatchable(), wantDispatchable, pathString(a.path))
			}
			return false
		}
	}
	gotKind := rejectKind(a.realize(e))
	a.trackRequestID(e, cur, out, gotKind)
	obs := a.observed()
	ok := gotKind == out.Reject && out.Next.SameObserved(obs)
	if !final {
		return ok
	}
	if gotKind != out.Reject {
		a.d.t.Errorf("%s from %s: reject kind disagrees — driver=%v model=%v\n  path: %s",
			e, cur.Status, gotKind, out.Reject, pathString(a.path))
	}
	if !out.Next.SameObserved(obs) {
		a.d.t.Errorf("%s from %s: state disagrees\n  observed=%s\n  model=   %s\n  path: %s",
			e, cur.Status, model.Fingerprint(obs), model.Fingerprint(out.Next), pathString(a.path))
	}
	// An edge invalidates the prior attempt's tasks by bumping a stamp, so compare the stamp delta across
	// this edge, refreshed by observed() above, to the model's per-transition invalidation bools.
	gotAttempt, gotSTC := a.curStamp != a.prevStamp, a.curSTCStamp != a.prevSTCStamp
	if gotAttempt != out.AttemptTasksInvalidated {
		a.d.t.Errorf("%s from %s: attempt-task invalidation disagrees — driver=%v model=%v\n  path: %s",
			e, cur.Status, gotAttempt, out.AttemptTasksInvalidated, pathString(a.path))
	}
	if gotSTC != out.ScheduleToCloseTaskInvalidated {
		a.d.t.Errorf("%s from %s: schedule-to-close-task invalidation disagrees — driver=%v model=%v\n  path: %s",
			e, cur.Status, gotSTC, out.ScheduleToCloseTaskInvalidated, pathString(a.path))
	}
	st, rs, attempt := a.describe()
	wantSt, wantRs := model.ExpectedDescribe(out.Next)
	if st != wantSt || rs != wantRs || attempt != out.Next.AttemptCount {
		a.d.t.Errorf("%s from %s: Describe disagrees — driver=(%v,%v,attempt=%d) model=(%v,%v,attempt=%d)\n  path: %s",
			e, cur.Status, st, rs, attempt, wantSt, wantRs, out.Next.AttemptCount, pathString(a.path))
	}
	return gotKind == out.Reject && out.Next.SameObserved(obs)
}

func pathString(path []model.Event) string {
	parts := make([]string, 0, len(path)+1)
	parts = append(parts, "Schedule")
	for _, e := range path {
		parts = append(parts, e.String())
	}
	return joinArrows(parts)
}

func joinArrows(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += " → "
		}
		out += p
	}
	return out
}

// traverse does a depth-bounded breadth-first walk of the model's reachable states, verifying every
// decided edge against the engine.
func (d *driver) traverse(maxDepth int) {
	type node struct {
		path  []model.Event
		state model.AbstractState
	}
	start := model.Initial(d.cfg)
	visited := map[string]bool{model.Fingerprint(start): true}
	frontier := []node{{nil, start}}
	edges, states := 0, 1
	d.verifyPath(nil)
	for depth := 0; depth < maxDepth && len(frontier) > 0; depth++ {
		var next []node
		for _, nd := range frontier {
			for _, e := range d.candidateEvents() {
				out := model.Transition(d.cfg, nd.state, e)
				edges++
				path := append(append([]model.Event{}, nd.path...), e)
				d.verifyPath(path)
				if out.Reject != model.NoError {
					continue
				}
				fp := model.Fingerprint(out.Next)
				if !visited[fp] {
					visited[fp] = true
					states++
					next = append(next, node{path, out.Next})
				}
			}
		}
		frontier = next
	}
	d.t.Logf("cfg %+v: verified %d edges across %d states (depth<=%d)", d.cfg, edges, states, maxDepth)
}

// randomWalk drives one activity, picking a random applicable event each step and checking it against
// the model. On reaching a terminal state it restarts, until the step budget is spent.
func (d *driver) randomWalk(rng *rand.Rand, steps int) {
	// freshWalk seeds the stamp-delta baseline via observed(), so the first edge's invalidation check
	// measures that edge and not the start.
	freshWalk := func() (*handle, model.AbstractState) {
		a := d.start()
		cur := model.Initial(d.cfg)
		require.True(d.t, cur.SameObserved(a.observed()))
		return a, cur
	}
	a, cur := freshWalk()
	var trace []model.Event
	seen := map[string]bool{model.Fingerprint(cur): true}
	for range steps {
		if cur.Status.Terminal() {
			a, cur = freshWalk()
			trace = nil
			continue
		}
		events := d.possibleEvents(cur)
		e := events[rng.Intn(len(events))]
		trace = append(trace, e)
		a.path = trace
		out := model.Transition(d.cfg, cur, e)
		if !a.apply(e, cur, out, true) {
			a, cur = freshWalk() // diverged (already reported); restart from a known state
			trace = nil
			continue
		}
		cur = out.Next
		seen[model.Fingerprint(cur)] = true
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	d.t.Logf("cfg %+v: random walk covered %d distinct states", d.cfg, len(keys))
}

func TestConformance(t *testing.T) {
	configs := []model.Config{
		{MaxAttempts: 3},
		{MaxAttempts: 2, HasScheduleToClose: true, HasHeartbeat: true},
	}
	t.Run("BFSGraphTraversal", func(t *testing.T) {
		for _, cfg := range configs {
			newDriver(t, cfg).traverse(model.MaxDepth())
		}
	})
	t.Run("RandomWalk", func(t *testing.T) {
		seed, steps := model.WalkSeed(), model.WalkSteps()
		t.Logf("random walk: seed=%d steps=%d/cfg (override TEMPORAL_SAASPEC_WALK_SEED / _WALK_STEPS)", seed, steps)
		for i, cfg := range configs {
			newDriver(t, cfg).randomWalk(rand.New(rand.NewSource(seed+int64(i))), steps)
		}
	})
}
