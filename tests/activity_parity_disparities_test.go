package tests

// WFA <-> SAA parity tests, mapped one to one onto the parity survey in .task/wfa-saa-parity.md.
//
// The survey is the index. Each of its entries has a label — A for core activity, R for reset, P for
// pause/unpause, U for update options — and there is one top-level test per section. Each subtest is
// named for the entry it checks, beginning with that entry's label, and checks that entry alone, so a
// reader can go from an entry to the test that establishes it and back.
//
// Parity claims that hold on both surfaces have no survey entry, since the survey records differences;
// they live in the other activity_parity files rather than here, as does the harness these tests drive
// activities with.
//
// Each subtest asserts the behaviour the entry has been resolved to, which is not always the behaviour
// either surface has today. A "Currently failing:" line names the surface that has still to change, so
// a green subtest with such a line is a stale comment to delete rather than a test to weaken.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestActivityCore covers the survey's core-activity (A) entries.
func (s *activityParityTestSuite) TestActivityCore() {
	t := s.T()
	env := newActivityParityEnv(t)
	ns := env.Namespace().String()

	// A01: a worker that reports a schedule-to-start or schedule-to-close timeout itself must close the
	// activity the same way the server's own timer does. Both surfaces already agree on TIMED_OUT with
	// retry state Timeout when the timer fires server-side (TestScheduleToStartTimeout,
	// TestParityScheduleToCloseTimeout), and which side noticed the deadline is not something a user
	// should be able to see. TestSyntheticFailuresHaveRetryParity pins the current divergence, one
	// expectation per surface; this states the outcome both ought to give.
	// Currently failing: SAA.
	t.Run("A01_WorkerReportedScheduleTimeout", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}
		for _, tc := range []struct {
			name    string
			event   model.Event
			timeout enumspb.TimeoutType
		}{
			{"ScheduleToStart", model.FailByIDWithScheduleToStartTimeoutFailure, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START},
			{"ScheduleToClose", model.FailByIDWithScheduleToCloseTimeoutFailure, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE},
		} {
			t.Run(tc.name, func(t *testing.T) {
				expected := activityTerminalProjection{
					Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
					FailureType: tc.timeout.String(),
					RetryState:  enumspb.RETRY_STATE_TIMEOUT,
				}
				parityDrive(t, env, cfg, []model.Event{model.Poll, tc.event},
					func(t *testing.T, a parityActivity) {
						require.Equalf(t, expected, a.terminal(t),
							"a worker-reported %s timeout must close the activity as the server's own timer does",
							tc.timeout)
					})
			})
		}
	})

	// A02: an operator command naming an activity that has already closed answers NotFound. Workflow
	// answers that uniformly across Signal, Update, UpdateOptions, Terminate, Pause and Unpause, and
	// consistency across execution types is what settles it. Note that the behavior model still requires
	// FailedPrecondition from a terminal status (model.terminalOutcome), so it has to move with this.
	// Currently failing: SAA.
	t.Run("A02_OperatorCommandAfterClose", func(t *testing.T) {
		parityDriveOutlivingActivity(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.Complete},
			func(t *testing.T, a parityActivity) {
				for _, e := range []model.Event{model.Pause, model.Unpause, model.Reset, model.UpdateOptions} {
					var notFound *serviceerror.NotFound
					require.ErrorAsf(t, a.rpc(t, e), &notFound,
						"%s naming a closed activity must answer NotFound, as every workflow command does", e)
				}
			})
	})

	// A03 is about which describe API exists rather than about one behavior: a standalone activity is an
	// execution that is retained after it closes and keeps reporting its terminal status, close time and
	// duration, while a workflow activity is only ever visible as an entry in its workflow's pending
	// set, which it leaves when it closes. Per-surface arms.
	t.Run("A03_DescribeAfterClose", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1}
		trace := []model.Event{model.Poll, model.Complete}

		t.Run("WorkflowActivity", func(t *testing.T) {
			d := newWFADriver(t, env, cfg)
			d.holdOpen = true // the workflow must outlive the activity, or there is nothing left to describe
			a := d.driveTrace(t, trace)
			await.Require(a.testContext(), t, func(t *await.T) {
				t.Require().Nil(a.pendingActivityInfo(t),
					"a closed workflow activity leaves the pending set, so nothing is reported about it")
			}, activityDriverTimeout, activityDriverPollInterval)
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, a.terminal(t).Status)
			info := a.describe(t).GetInfo()
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, info.GetStatus(),
				"a closed standalone activity must still report its terminal status")
			require.NotNil(t, info.GetCloseTime(), "...and when it closed")
			require.NotNil(t, info.GetExecutionDuration(), "...and how long it ran")
		})
	})

	// A04: the activity is closed by its schedule-to-close deadline — the retry the attempt's timeout
	// asked for cannot be scheduled before it — so that deadline, not the per-attempt timeout, is the
	// timeout a user must be told about, with retry state Timeout saying that retries were given up for
	// lack of time. The trace stops at the poll and lets the heartbeat timeout fire on its own, because
	// the driver's timer wait insists on seeing the per-attempt timeout reported, which is the very
	// thing in question here. TestParityTimeoutTypeOnInsufficientTimeForRetry asserts this for the
	// standalone surface only, skipping the workflow one.
	// Currently failing: both, on the Cause arm.
	t.Run("A04_TimeoutTypeWhenRetryCannotFitBeforeScheduleToClose", func(t *testing.T) {
		cfg := activityConfig{
			MaxAttempts:      2,
			RetryInterval:    30 * time.Second,
			ScheduleToClose:  10 * time.Second,
			HeartbeatTimeout: activityShortTimeout,
		}
		expected := activityTerminalProjection{
			Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
			FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String(),
			RetryState:  enumspb.RETRY_STATE_TIMEOUT,
		}
		parityDrive(t, env, cfg, []model.Event{model.Poll}, func(t *testing.T, a parityActivity) {
			require.Equal(t, expected, a.terminal(t),
				"an activity whose retry cannot fit before its schedule-to-close deadline must report that deadline as the timeout that closed it")
		})

		// The second half of the claim: whether the timeout that actually ended the attempt survives as the
		// terminal failure's cause. The projection above cannot see it, and each surface exposes a cause
		// through a different shape, so each arm reads its own.
		const chained = "the timeout that ended the attempt must survive as the cause of the deadline that closed the activity"
		t.Run("Cause", func(t *testing.T) {
			t.Run("WorkflowActivity", func(t *testing.T) {
				a := newWFADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
				timeoutErr, ok := errors.AsType[*temporal.TimeoutError](a.run.Get(a.testContext(), nil))
				require.True(t, ok)
				require.Error(t, timeoutErr.Unwrap(), chained)
			})
			t.Run("StandaloneActivity", func(t *testing.T) {
				a := newSAADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
				// awaitTerminal first: the outcome does not exist until the deadline closes the activity, so
				// describing it before then would report a missing cause for the wrong reason.
				require.NotNil(t, a.awaitTerminal(t).GetOutcome().GetFailure().GetCause(), chained)
			})
		})
	})

	// A05 (API shape): a standalone activity is addressable on its own, so it has a per-activity result
	// API; a workflow activity is not, and its result reaches the user through the workflow that
	// scheduled it. Per-surface arms.
	t.Run("A05_WaitForOneActivityResult", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1}
		trace := []model.Event{model.Poll, model.Complete}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
				a.terminal(t), "the workflow result is the only place a workflow activity's outcome appears")
			_, err := env.FrontendClient().PollActivityExecution(a.testContext(), &workflowservice.PollActivityExecutionRequest{
				Namespace: ns, ActivityId: a.activityID,
			})
			var notFound *serviceerror.NotFound
			require.ErrorAs(t, err, &notFound,
				"a workflow activity's id names no activity execution, so it cannot be polled for a result")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			resp, err := env.FrontendClient().PollActivityExecution(a.testContext(), &workflowservice.PollActivityExecutionRequest{
				Namespace: ns, ActivityId: a.activityID, RunId: a.runID,
			})
			require.NoError(t, err)
			require.NotEmpty(t, resp.GetRunId(), "PollActivityExecution must resolve once the activity is terminal")
			require.NotNil(t, resp.GetOutcome().GetResult(), "...carrying the result the worker returned")
			require.NotNil(t, a.describe(t).GetOutcome().GetResult(),
				"DescribeActivityExecution must return the outcome when asked for it")
		})
	})

	// A06: an operator command carrying a request id the server has already applied must not be applied
	// again — a client retrying a call whose response it never saw would otherwise silently undo a later
	// change. Update-options is the command used here because its effect is a value that can be read
	// back. Neither driver sends a request id for update-options, so each arm issues the calls itself,
	// and reads the installed timeout from where its surface reports it.
	// Currently failing: both.
	t.Run("A06_RepeatedRequestIDIsDeduplicated", func(t *testing.T) {
		const firstTimeout, laterTimeout = time.Hour, 2 * time.Hour
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		check := func(
			t *testing.T,
			update func(requestID string, heartbeatTimeout time.Duration) error,
			installed func() time.Duration,
		) {
			replayed := uuid.NewString()
			require.NoError(t, update(replayed, firstTimeout))
			require.NoError(t, update(uuid.NewString(), laterTimeout))
			require.NoError(t, update(replayed, firstTimeout))
			require.Equal(t, laterTimeout, installed(),
				"replaying an update whose request id was already applied must not undo the later update")
		}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, nil)
			check(t, func(requestID string, heartbeatTimeout time.Duration) error {
				_, err := env.FrontendClient().UpdateActivityExecutionOptions(a.testContext(),
					&workflowservice.UpdateActivityExecutionOptionsRequest{
						Namespace: ns, WorkflowId: a.workflowID, RunId: a.runID, ActivityId: a.activityID,
						Identity: env.Tv().ClientIdentity(), RequestId: requestID,
						ActivityOptions: &activitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(heartbeatTimeout)},
						UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
					})
				return err
			}, func() time.Duration {
				return a.pendingActivityInfo(t).GetActivityOptions().GetHeartbeatTimeout().AsDuration()
			})
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			check(t, func(requestID string, heartbeatTimeout time.Duration) error {
				_, err := env.FrontendClient().UpdateActivityExecutionOptions(a.testContext(),
					&workflowservice.UpdateActivityExecutionOptionsRequest{
						Namespace: ns, RunId: a.runID, ActivityId: a.activityID,
						Identity: env.Tv().ClientIdentity(), RequestId: requestID,
						ActivityOptions: &activitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(heartbeatTimeout)},
						UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
					})
				return err
			}, func() time.Duration {
				return a.describe(t).GetInfo().GetHeartbeatTimeout().AsDuration()
			})
		})
	})

	// A07 (API shape): the workflow-activity operator RPCs take a workflow execution plus a target that
	// may be an activity id, an activity type, or every activity of the run; the standalone RPCs address
	// exactly one activity, by id. Per-surface arms.
	t.Run("A07_TargetingManyActivitiesInOneCall", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, nil)
			execution := &commonpb.WorkflowExecution{WorkflowId: a.workflowID, RunId: a.runID}
			_, err := env.FrontendClient().PauseActivity(a.testContext(), &workflowservice.PauseActivityRequest{
				Namespace: ns, Execution: execution, Identity: env.Tv().ClientIdentity(), Reason: "A07",
				Activity: &workflowservice.PauseActivityRequest_Type{Type: coreParityWFAActivityType},
			})
			require.NoError(t, err)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, a.activityInfo(t).RunState,
				"naming an activity type, with no activity id, must pause the run's activities of that type")

			_, err = env.FrontendClient().UnpauseActivity(a.testContext(), &workflowservice.UnpauseActivityRequest{
				Namespace: ns, Execution: execution, Identity: env.Tv().ClientIdentity(),
				Activity: &workflowservice.UnpauseActivityRequest_UnpauseAll{UnpauseAll: true},
			})
			require.NoError(t, err)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, a.activityInfo(t).RunState,
				"unpause_all must unpause every activity of the run")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			_, err := env.FrontendClient().PauseActivityExecution(a.testContext(), &workflowservice.PauseActivityExecutionRequest{
				Namespace: ns, RunId: a.runID, Identity: env.Tv().ClientIdentity(), Reason: "A07",
				RequestId: uuid.NewString(),
			})
			var invalidArgument *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgument,
				"a standalone pause must name the one activity it targets: there is no by-type or all form")
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, a.activityInfo(t).RunState,
				"the refused call must change nothing")
		})
	})

	// A08 (API shape): each surface reports the worker's version through its own fields, and what the
	// entry is about is which fields exist. Reading them is the compile-time half of the assertion; the
	// harness's worker is unversioned, so the runtime half is that each reports nothing. The
	// assigned_build_id oneof's versioning-1 form is not read here: only its build-id member has a value
	// an unversioned dispatch is guaranteed to leave empty.
	t.Run("A08_WorkerVersionReporting", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1}
		trace := []model.Event{model.Poll}

		// The four deprecated getters are read deliberately: which fields WFA reports is what the entry
		// is about.
		//nolint:staticcheck
		t.Run("WorkflowActivity", func(t *testing.T) {
			pa := newWFADriver(t, env, cfg).driveTrace(t, trace).pendingActivityInfo(t)
			require.Empty(t, pa.GetLastIndependentlyAssignedBuildId(), "an unversioned worker is assigned no build id")
			require.Nil(t, pa.GetLastWorkerVersionStamp(), "...and stamps no version")
			require.Nil(t, pa.GetLastDeployment(), "...and belongs to no deployment")
			require.Empty(t, pa.GetLastWorkerDeploymentVersion(), "...and to no deployment version")
			require.Nil(t, pa.GetLastDeploymentVersion(), "...by either name")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			info := newSAADriver(t, env, cfg).driveTrace(t, trace).describe(t).GetInfo()
			require.Nil(t, info.GetLastDeploymentVersion(),
				"last_deployment_version is the one version field a standalone activity reports, and an unversioned worker leaves it unset")
		})
	})

	// A09 (API shape): per-activity metadata exists on the standalone surface only. For a workflow
	// activity this information belongs to the enclosing workflow and PendingActivityInfo has no field
	// for any of it, so there is nothing to read on that side and no workflow arm here. sdk_name and
	// sdk_version are not asserted: the harness's worker is a bare poll RPC that sends no SDK headers.
	t.Run("A09_PerActivityMetadata", func(t *testing.T) {
		d := newSAADriver(t, env, activityConfig{MaxAttempts: 1})
		d.customizeStart = func(req *workflowservice.StartActivityExecutionRequest) {
			req.SearchAttributes = coreParitySearchAttributes
			req.Header = coreParityHeader
			req.UserMetadata = coreParityUserMetadata
			req.Links = coreParityLinks(ns)
		}
		a := d.driveTrace(t, []model.Event{model.Poll, model.Heartbeat})
		info := a.describe(t).GetInfo()

		protorequire.ProtoEqual(t, coreParitySearchAttributes, info.GetSearchAttributes())
		protorequire.ProtoEqual(t, coreParityHeader, info.GetHeader())
		protorequire.ProtoEqual(t, coreParityUserMetadata, info.GetUserMetadata())
		require.Len(t, info.GetLinks(), 1, "the links the activity was started with must be reported")
		require.EqualValues(t, 1, info.GetTotalHeartbeatCount(), "the worker's one heartbeat must be counted")
		require.Equal(t, a.taskQueue, info.GetTaskQueue(), "the task queue the activity dispatches on must be reported")
		require.Positive(t, info.GetStateTransitionCount(), "a started activity has undergone state transitions")
		require.Positive(t, info.GetStateSizeBytes(), "...and occupies state")
		require.NotNil(t, info.GetExecutionTime(), "...and reports when it became dispatchable")
	})

	// A10 (API shape): the timeouts and retry policy the user configured are read back from different
	// places — nested in activity_options for a workflow activity, with maximum_attempts also lifted out
	// on its own, and flat on ActivityExecutionInfo for a standalone one. Per-surface arms, so that
	// either shape changing fails the test.
	t.Run("A10_ConfiguredTimeoutsAndRetryPolicy", func(t *testing.T) {
		cfg := activityConfig{
			MaxAttempts:      3,
			RetryInterval:    activityLongDuration,
			StartToClose:     time.Hour,
			ScheduleToStart:  2 * time.Hour,
			ScheduleToClose:  3 * time.Hour,
			HeartbeatTimeout: 30 * time.Minute,
		}

		t.Run("WorkflowActivity", func(t *testing.T) {
			pa := newWFADriver(t, env, cfg).driveTrace(t, nil).pendingActivityInfo(t)
			options := pa.GetActivityOptions()
			require.Equal(t, cfg.StartToClose, options.GetStartToCloseTimeout().AsDuration(),
				"the configured timeouts are reported nested in activity_options")
			require.Equal(t, cfg.ScheduleToStart, options.GetScheduleToStartTimeout().AsDuration())
			require.Equal(t, cfg.ScheduleToClose, options.GetScheduleToCloseTimeout().AsDuration())
			require.Equal(t, cfg.HeartbeatTimeout, options.GetHeartbeatTimeout().AsDuration())
			require.Equal(t, cfg.MaxAttempts, options.GetRetryPolicy().GetMaximumAttempts(),
				"the retry policy is reported nested in activity_options too")
			require.Equal(t, cfg.MaxAttempts, pa.GetMaximumAttempts(),
				"maximum_attempts is also exposed on its own")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			info := newSAADriver(t, env, cfg).driveTrace(t, nil).describe(t).GetInfo()
			require.Equal(t, cfg.StartToClose, info.GetStartToCloseTimeout().AsDuration(),
				"the configured timeouts are flat fields on ActivityExecutionInfo")
			require.Equal(t, cfg.ScheduleToStart, info.GetScheduleToStartTimeout().AsDuration())
			require.Equal(t, cfg.ScheduleToClose, info.GetScheduleToCloseTimeout().AsDuration())
			require.Equal(t, cfg.HeartbeatTimeout, info.GetHeartbeatTimeout().AsDuration())
			require.Equal(t, cfg.MaxAttempts, info.GetRetryPolicy().GetMaximumAttempts(),
				"...as is the retry policy")
		})
	})

	// A11 (API shape): the same value — when the activity was scheduled — is named scheduled_time on
	// PendingActivityInfo and schedule_time on ActivityExecutionInfo. Each arm reads its surface's name,
	// so renaming either fails the test.
	t.Run("A11_ScheduleTimeFieldName", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("WorkflowActivity", func(t *testing.T) {
			require.NotNil(t, newWFADriver(t, env, cfg).driveTrace(t, nil).pendingActivityInfo(t).GetScheduledTime(),
				"a workflow activity reports its schedule time as scheduled_time")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			require.NotNil(t, newSAADriver(t, env, cfg).driveTrace(t, nil).describe(t).GetInfo().GetScheduleTime(),
				"a standalone activity reports its schedule time as schedule_time")
		})
	})

	// A12 (intended divergence): only a standalone activity has a start delay of its own. A workflow
	// decides when to schedule its activity — it can sleep on a durable timer first — so the activity it
	// schedules is dispatchable at once.
	t.Run("A12_StartDelay", func(t *testing.T) {
		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}).
				driveTrace(t, nil)
			require.Nil(t, a.pendingActivityInfo(t).GetActivityOptions().GetStartDelay(),
				"a workflow activity has no start delay to report")
			require.NotNil(t, a.pollForTask(t, activityDriverTimeout),
				"its first attempt is dispatchable as soon as the workflow schedules it")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration, StartDelay: activityLongDuration}
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			info := a.describe(t).GetInfo()
			require.Equal(t, cfg.StartDelay, info.GetStartDelay().AsDuration(),
				"start_delay must be reported as configured")
			require.Equal(t, info.GetScheduleTime().AsTime().Add(cfg.StartDelay), info.GetExecutionTime().AsTime(),
				"the first dispatch is held back until schedule_time + start_delay")
			require.Nil(t, a.pollForTask(t, activityDriverTimeout),
				"no task may be dispatched during the start delay")
		})
	})

	// A13 (intended divergence): the two surfaces take different options at creation. The workflow-side
	// options the survey names — eager execution and use_workflow_build_id — are fields of the
	// workflow's ScheduleActivityTask command that the harness's wrapper workflow cannot set through the
	// SDK's ActivityOptions, so there is no workflow arm; the standalone arm asserts a start request
	// carrying the standalone-only options is accepted and that the ones ActivityExecutionInfo echoes
	// come back. Completion callbacks and on-conflict options are exercised by A22.
	t.Run("A13_StartOptions", func(t *testing.T) {
		d := newSAADriver(t, env, activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration})
		d.customizeStart = func(req *workflowservice.StartActivityExecutionRequest) {
			req.IdReusePolicy = enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE
			req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL
			req.SearchAttributes = coreParitySearchAttributes
			req.UserMetadata = coreParityUserMetadata
			req.Links = coreParityLinks(ns)
			req.StartDelay = durationpb.New(activityLongDuration)
		}
		info := d.driveTrace(t, nil).describe(t).GetInfo()
		protorequire.ProtoEqual(t, coreParitySearchAttributes, info.GetSearchAttributes())
		protorequire.ProtoEqual(t, coreParityUserMetadata, info.GetUserMetadata())
		require.Len(t, info.GetLinks(), 1)
		require.Equal(t, activityLongDuration, info.GetStartDelay().AsDuration(),
			"a start request carrying the standalone-only options must be accepted and reported")
	})

	// A14: how the terminal timeout reads to the caller. A workflow activity's user gets a typed
	// *temporal.TimeoutError from the SDK, which renders the timeout type into the message; a standalone
	// caller must get the same, since the difference is in the SDK rather than in what the server
	// recorded. Both arms therefore go through the Go SDK — the standalone one through the activity
	// client, ExecuteActivity and ActivityHandle.Get, rather than through the harness's raw driver, which
	// reads the server's proto directly and so could only ever pin the raw text.
	t.Run("A14_TimeoutFailureMessage", func(t *testing.T) {
		assertRendersTimeout := func(t *testing.T, err error) {
			var timeoutErr *temporal.TimeoutError
			require.ErrorAs(t, err, &timeoutErr,
				"a timed-out activity must reach its caller as a typed timeout error")
			require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, timeoutErr.TimeoutType())
			require.Equal(t,
				fmt.Sprintf("%s (type: %s)", timeoutErr.Message(), enumspb.TIMEOUT_TYPE_START_TO_CLOSE),
				timeoutErr.Error(),
				"...whose message the SDK renders from the timeout type and the server's text")
		}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, activityConfig{MaxAttempts: 1}).
				driveTrace(t, []model.Event{model.Poll, model.StartToCloseElapses})
			assertRendersTimeout(t, a.run.Get(a.testContext(), nil))
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			ctx := testcontext.For(t)
			taskQueue := testcore.RandomizeStr("a14-act")
			handle, err := env.SdkClient().ExecuteActivity(ctx, sdkclient.StartActivityOptions{
				ID:                  testcore.RandomizeStr("a14"),
				TaskQueue:           taskQueue,
				StartToCloseTimeout: activityShortTimeout,
				RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
			}, "activityType")
			require.NoError(t, err)
			// The attempt has to start, or schedule-to-start is what closes the activity.
			require.NotNil(t, activityPollForTask(ctx, t, "a14", env, taskQueue, activityDriverTimeout))
			assertRendersTimeout(t, handle.Get(ctx, nil))
		})
	})

	// A15: an activity id means different things. A workflow activity's id is scoped to its workflow
	// run, so two runs can each schedule one called "act" and the two are unrelated activities. A
	// standalone activity id is namespace-scoped and names at most one running activity, so a second
	// start under a running id is refused rather than creating a second activity.
	t.Run("A15_ActivityIDScope", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("WorkflowActivity", func(t *testing.T) {
			d := newWFADriver(t, env, cfg)
			first := d.driveTrace(t, []model.Event{model.Poll})
			second := d.driveTrace(t, nil)
			require.Equal(t, first.activityID, second.activityID, "both runs schedule the same activity id")
			require.NotEqual(t, first.workflowID, second.workflowID)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, first.activityInfo(t).RunState)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, second.activityInfo(t).RunState,
				"an activity id names one activity per workflow run, so the two do not collide")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			_, err := env.FrontendClient().StartActivityExecution(a.testContext(),
				a.d.startRequest(a.d.cfg, a.activityID, a.taskQueue))
			var alreadyStarted *serviceerror.ActivityExecutionAlreadyStarted
			require.ErrorAs(t, err, &alreadyStarted,
				"an activity id is namespace-scoped, so a second run may not overlap the first")
		})
	})

	// A16: a workflow activity cannot outlive the workflow run that owns it — terminating the run takes
	// the activity with it, and the worker holding its task can no longer report on it. A standalone
	// activity has no owner that could end it early: not even a workflow of the same id, which does not
	// exist, so the activity runs to its own terminal state.
	t.Run("A16_OwnerClosesWhileActivityPending", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
			require.NoError(t, env.SdkClient().TerminateWorkflow(a.testContext(), a.workflowID, a.runID, "A16"))
			// The entry's checkable claim is that the activity does not carry on: the worker that still
			// holds its task can no longer report on it. Whether the closed run keeps listing the activity
			// in its pending set is not asserted — the entry reads either way, and it does keep listing it.
			var notFound *serviceerror.NotFound
			require.ErrorAs(t, a.rpc(t, model.Complete), &notFound,
				"a completion for an activity whose workflow has closed must be rejected")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
			var notFound *serviceerror.NotFound
			require.ErrorAs(t, env.SdkClient().TerminateWorkflow(a.testContext(), a.activityID, "", "A16"), &notFound,
				"a standalone activity has no owning workflow that could close it")
			a.driveEvent(t, model.Complete)
			require.Equal(t, activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
				a.terminal(t), "it reaches its own terminal state instead")
		})
	})

	// A17: a standalone activity is an execution a user can search for — it has its own visibility
	// record and its own list and count APIs — while a workflow activity has none, so a user searches
	// for the enclosing workflow instead.
	t.Run("A17_Visibility", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			query := fmt.Sprintf("ActivityId = '%s'", a.activityID)
			coreParityAwaitVisible(t, env, query, 1)
			resp, err := env.FrontendClient().CountActivityExecutions(a.testContext(),
				&workflowservice.CountActivityExecutionsRequest{Namespace: ns, Query: query})
			require.NoError(t, err)
			require.EqualValues(t, 1, resp.GetCount(), "a standalone activity must be countable by query")
		})

		t.Run("WorkflowActivity", func(t *testing.T) {
			wfa := newWFADriver(t, env, cfg).driveTrace(t, nil)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, wfa.activityInfo(t).RunState)
			// A standalone activity started alongside is the control: once its record is indexed, the
			// workflow activity's absence is not simply indexing lag.
			control := newSAADriver(t, env, cfg).driveTrace(t, nil)
			coreParityAwaitVisible(t, env, fmt.Sprintf("ActivityId = '%s'", control.activityID), 1)

			resp, err := env.FrontendClient().ListActivityExecutions(wfa.testContext(),
				&workflowservice.ListActivityExecutionsRequest{
					Namespace: ns, PageSize: 10,
					Query: fmt.Sprintf("ActivityType = '%s'", coreParityWFAActivityType),
				})
			require.NoError(t, err)
			require.Empty(t, resp.GetExecutions(), "a workflow activity has no visibility record of its own")
		})
	})

	// A18 has no subtest. Its claim is about routing: a workflow activity can be routed by Workflow
	// Build ID inheritance or a Task Queue versioning directive, and a standalone activity cannot.
	// Establishing that needs a versioned worker deployment — a registered worker deployment version and
	// task-queue versioning rules — which the harness cannot express: its worker is a bare
	// PollActivityTaskQueue call that carries no deployment options, and the standalone half of the
	// claim is the absence of a request field, which has no runtime observation.

	// A19: an activity that names no priority of its own must be dispatched with the priority of
	// whatever created it. A workflow activity has a workflow to inherit from; a standalone activity has
	// nothing above it, so the namespace default applies. The dispatched task is where a user sees the
	// effective priority, so that is what each arm reads.
	t.Run("A19_PriorityInheritance", func(t *testing.T) {
		const priorityKey = 1
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("WorkflowActivity", func(t *testing.T) {
			taskQueue, _ := coreParityActivityUnderWorkflow(t, env, cfg,
				sdkclient.StartWorkflowOptions{Priority: temporal.Priority{PriorityKey: priorityKey}})
			resp := activityPollForTask(testcontext.For(t), t, "coreParity", env, taskQueue, activityDriverTimeout)
			require.NotNil(t, resp, "the activity must be dispatched")
			require.EqualValues(t, priorityKey, resp.GetPriority().GetPriorityKey(),
				"an activity with no priority of its own must be dispatched with its workflow's")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			resp := newSAADriver(t, env, cfg).driveTrace(t, nil).pollForTask(t, activityDriverTimeout)
			require.NotNil(t, resp, "the activity must be dispatched")
			require.Zero(t, resp.GetPriority().GetPriorityKey(),
				"a standalone activity has no parent priority to inherit, so nothing overrides the default")
		})
	})

	// A20: what bounds an activity whose user supplied only a start-to-close timeout. A workflow
	// activity is bounded by the run that owns it, so its missing schedule-to-close deadline is defaulted
	// from the workflow run timeout; a standalone activity has no such parent, so the deadline stays
	// unset and it has no expiration time.
	t.Run("A20_MissingScheduleTimeouts", func(t *testing.T) {
		const runTimeout = time.Hour
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration, StartToClose: time.Minute}

		t.Run("WorkflowActivity", func(t *testing.T) {
			_, pending := coreParityActivityUnderWorkflow(t, env, cfg,
				sdkclient.StartWorkflowOptions{WorkflowRunTimeout: runTimeout})
			pa := pending(t)
			scheduleToClose := pa.GetActivityOptions().GetScheduleToCloseTimeout().AsDuration()
			require.Positive(t, scheduleToClose,
				"a workflow activity's missing schedule-to-close deadline is defaulted from the workflow run timeout")
			require.LessOrEqual(t, scheduleToClose, runTimeout, "...and bounded by it")
			require.NotNil(t, pa.GetExpirationTime(), "...so the activity has an absolute deadline")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			info := newSAADriver(t, env, cfg).driveTrace(t, nil).describe(t).GetInfo()
			require.Zero(t, info.GetScheduleToCloseTimeout().AsDuration(),
				"there is no parent run timeout to default a standalone activity's schedule-to-close deadline from")
			require.Nil(t, info.GetExpirationTime(),
				"...so it has no expiration time either")
		})
	})

	// A21: terminate and delete act on a standalone activity execution and have no workflow-activity
	// form — their requests have no workflow_id field at all — because a workflow activity is not an
	// independently retained execution. The closest a user can come is naming the activity id, which
	// finds nothing and leaves the workflow's activity alone.
	t.Run("A21_TerminateAndDelete", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
			var notFound *serviceerror.NotFound
			_, err := env.FrontendClient().TerminateActivityExecution(a.testContext(),
				&workflowservice.TerminateActivityExecutionRequest{
					Namespace: ns, ActivityId: a.activityID, Identity: env.Tv().ClientIdentity(),
					Reason: "A21", RequestId: uuid.NewString(),
				})
			require.ErrorAs(t, err, &notFound,
				"a workflow activity is not an activity execution these APIs can terminate")
			_, err = env.FrontendClient().DeleteActivityExecution(a.testContext(),
				&workflowservice.DeleteActivityExecutionRequest{Namespace: ns, ActivityId: a.activityID})
			require.ErrorAs(t, err, &notFound, "...nor one they can delete")
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, a.activityInfo(t).RunState,
				"the workflow's activity is untouched")
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
			a.driveEvent(t, model.Terminate)
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED, a.terminal(t).Status,
				"terminate must close the standalone execution")
			_, err := env.FrontendClient().DeleteActivityExecution(a.testContext(),
				&workflowservice.DeleteActivityExecutionRequest{Namespace: ns, ActivityId: a.activityID, RunId: a.runID})
			require.NoError(t, err)
			await.Require(a.testContext(), t, func(t *await.T) {
				_, err := env.FrontendClient().DescribeActivityExecution(t.Context(),
					&workflowservice.DescribeActivityExecutionRequest{
						Namespace: ns, ActivityId: a.activityID, RunId: a.runID,
					})
				var notFound *serviceerror.NotFound
				t.Require().ErrorAs(err, &notFound, "a deleted activity execution must no longer be described")
			}, activityDriverTimeout, activityDriverPollInterval)
		})
	})

	// A22: a completion callback can be attached to a standalone activity, including to one that already
	// exists, via id conflict policy USE_EXISTING with attach_completion_callbacks. A workflow's
	// ScheduleActivityTask command has no callback facility, and a start request naming a workflow
	// activity's id does not reach that activity — it creates a new standalone one — so there is no way
	// to attach a callback to a workflow activity.
	t.Run("A22_AttachCompletionCallbackToExistingActivity", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}
		defer env.OverrideDynamicConfig(activity.EnableCallbacks, true)()
		defer env.OverrideDynamicConfig(callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}})()

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, nil)
			d := newSAADriver(t, env, cfg)
			req := d.startRequest(cfg, a.activityID, a.taskQueue)
			req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING
			// attach_completion_callbacks is only accepted alongside attach_request_id.
			req.OnConflictOptions = &commonpb.OnConflictOptions{AttachRequestId: true, AttachCompletionCallbacks: true}
			req.CompletionCallbacks = coreParityCallbacks("http://localhost/a22-wfa")
			resp, err := env.FrontendClient().StartActivityExecution(a.testContext(), req)
			require.NoError(t, err)
			require.True(t, resp.GetStarted(),
				"a start naming a workflow activity's id creates a new standalone activity rather than attaching to it")
			require.NotEqual(t, a.runID, resp.GetRunId())
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			d := newSAADriver(t, env, cfg)
			d.customizeStart = func(req *workflowservice.StartActivityExecutionRequest) {
				req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING
				req.CompletionCallbacks = coreParityCallbacks("http://localhost/a22-first")
			}
			a := d.driveTrace(t, nil)

			req := d.startRequest(d.cfg, a.activityID, a.taskQueue)
			// attach_completion_callbacks is only accepted alongside attach_request_id.
			req.OnConflictOptions = &commonpb.OnConflictOptions{AttachRequestId: true, AttachCompletionCallbacks: true}
			req.CompletionCallbacks = coreParityCallbacks("http://localhost/a22-second")
			resp, err := env.FrontendClient().StartActivityExecution(a.testContext(), req)
			require.NoError(t, err)
			require.False(t, resp.GetStarted(), "the second start must attach to the running activity")
			require.Equal(t, a.runID, resp.GetRunId())
			require.Len(t, a.describe(t).GetCallbacks(), 2,
				"the callback the second start carried must be attached to the existing activity")
		})
	})

	// A23: the standalone operator-command feature gate. With it off, the four operator commands must be
	// refused for a standalone activity while remaining available for a workflow activity, which the same
	// four RPCs also serve. The standalone arm pauses one activity before turning the gate off, so that
	// the unpause it then attempts would otherwise have succeeded.
	t.Run("A23_OperatorCommandFeatureGate", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, nil)
			defer env.OverrideDynamicConfig(activity.EnableStandaloneActivityOperatorCommands, false)()
			// The driver holds each call to the model, which permits all four from these states.
			for _, e := range []model.Event{model.Pause, model.Unpause, model.Reset, model.UpdateOptions} {
				a.driveEvent(t, e)
			}
		})

		t.Run("StandaloneActivity", func(t *testing.T) {
			d := newSAADriver(t, env, cfg)
			scheduled := d.driveTrace(t, nil)
			paused := d.driveTrace(t, []model.Event{model.Pause})
			defer env.OverrideDynamicConfig(activity.EnableStandaloneActivityOperatorCommands, false)()

			for _, e := range []model.Event{model.Pause, model.Reset, model.UpdateOptions} {
				require.Errorf(t, scheduled.rpc(t, e),
					"%s must be refused while the standalone operator-command gate is off", e)
			}
			require.Error(t, paused.rpc(t, model.Unpause),
				"unpause must be refused while the standalone operator-command gate is off")
		})
	})
}

// TestReset covers the survey's reset (R) entries. Every entry here concerns a reset requested while a
// worker owns the attempt, so that the reset cannot be applied at once.
//
// The reset request's jitter option has no entry and no subtest: the trace vocabulary has no way to send
// it, and it speaks only to an activity waiting out a backoff, which one with an attempt in progress is
// not.
func (s *activityParityTestSuite) TestReset() {
	t := s.T()
	env := newActivityParityEnv(t)

	// A long retry interval makes the reset's effect on dispatch observable: an ordinary retry would
	// have to wait it out, whereas the reset attempt must be dispatchable at once.
	deferredResetCfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}
	// spentBudgetCfg leaves the activity no retry, so an attempt that ends would close it if the reset
	// were not there to rewind the attempt counter.
	spentBudgetCfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

	failRetryablyByID := model.Event{Type: model.RespondFailedByIDType, Failure: &model.Failure{}}

	// assertResetApplied is what both surfaces must report once a pending reset has been applied: a fresh
	// attempt 1, with no backoff to serve before it dispatches.
	resetAttempt := activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 1}
	assertResetApplied := func(t *testing.T, a parityActivity) {
		require.Equal(t, resetAttempt, a.activityInfo(t),
			"the pending reset must be applied when the attempt ends, starting the activity over")
		a.driveEvent(t, model.Poll)
		require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1}, a.activityInfo(t),
			"the reset attempt must dispatch without waiting out a retry backoff")
	}

	// R01: the reset is held until the attempt ends, and what it then starts is a fresh attempt 1 with no
	// backoff to serve. Driven by each shape of failure a worker can report the attempt ending with, and
	// by a keep_paused reset with no pause to keep, which is a default reset.
	// Currently failing: WFA.
	t.Run("R01_ResetWhileAttemptInProgress", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			trace []model.Event
		}{
			{name: "RetryableFailure", trace: []model.Event{model.Poll, model.Reset, model.FailRetryably}},
			{name: "OmittedFailure", trace: []model.Event{model.Poll, model.Reset, model.FailWithoutFailure}},
			{name: "FailureByID", trace: []model.Event{model.Poll, model.Reset, failRetryablyByID}},
			{
				name:  "KeepPausedWithNoPauseToKeep",
				trace: []model.Event{model.Poll, model.ResetKeepPaused, model.FailRetryably},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, deferredResetCfg, tc.trace, assertResetApplied)
			})
		}
	})

	// R02: a non-retryable failure closes the activity and the pending reset is discarded. A worker that
	// obeys the reset it was told of reports a *retryable* ActivityReset error, which is the path R08
	// covers; a non-retryable failure therefore means the attempt failed on its own merits, and the user
	// declared that class of failure not worth retrying. The reset does not change retryability, so it
	// does not get to override it. The retry budget makes no difference either way.
	// Currently failing: SAA.
	t.Run("R02_NonRetryableFailureWhileResetPending", func(t *testing.T) {
		failedNonRetryably := activityTerminalProjection{
			Status:      enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
			FailureType: "TestFailure",
			RetryState:  enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		}
		for _, tc := range []struct {
			name string
			cfg  activityConfig
		}{
			{name: "WithRetriesLeft", cfg: deferredResetCfg},
			{name: "WithRetryBudgetSpent", cfg: spentBudgetCfg},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, tc.cfg, []model.Event{model.Poll, model.Reset, model.FailNonRetryably},
					func(t *testing.T, a parityActivity) {
						require.Equal(t, failedNonRetryably, a.terminal(t),
							"a non-retryable failure closes the activity, and the pending reset is discarded")
					})
			})
		}
	})

	// R03: a reset that does not ask to keep the pause withdraws it. keep_paused exists precisely so that a
	// user can ask for the pause to survive the reset, so a reset without it must not leave one pending:
	// the activity goes back to being an ordinary running attempt, and the reset it defers lands SCHEDULED
	// rather than PAUSED.
	// Currently failing: WFA.
	t.Run("R03_ResetWhilePauseRequested", func(t *testing.T) {
		parityDrive(t, env, deferredResetCfg, []model.Event{model.Poll, model.Pause, model.Reset},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1}, a.activityInfo(t),
					"a reset that does not ask to keep the pause must withdraw it")
				a.driveEvent(t, model.FailRetryably)
				assertResetApplied(t, a)
			})
	})

	// R04: keep_paused asks for the pause to survive the reset, so when the worker goes silent and the
	// attempt's own timeout ends it, both must land: the attempt counter is rewound and the activity is
	// PAUSED at attempt 1. Attempt 1 is the discriminant — a retry that ignored the reset would be at
	// attempt 2. Driven by each per-attempt timeout, since either is how a silent worker's attempt ends.
	// The timeout is not in the trace, so its window is configured explicitly, and it is absorbed by the
	// reset rather than advancing the attempt counter, so the driver's timer wait cannot express it.
	// Currently failing: WFA.
	t.Run("R04_KeepPausedResetWhilePauseRequestedAndWorkerGoesSilent", func(t *testing.T) {
		pausedResetAttempt := activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 1}
		for _, tc := range []struct {
			name string
			cfg  activityConfig
		}{
			{
				name: "StartToCloseTimeout",
				cfg:  activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration, StartToClose: activityShortTimeout},
			},
			{
				name: "HeartbeatTimeout",
				cfg:  activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration, HeartbeatTimeout: activityShortTimeout},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, tc.cfg, []model.Event{model.Poll, model.Pause, model.ResetKeepPaused},
					func(t *testing.T, a parityActivity) {
						require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, Attempt: 1},
							a.activityInfo(t),
							"the pause and the keep_paused reset must both be pending on the attempt that is about to time out")
						awaitAbsorbedAttemptTimeout(t, a, pausedResetAttempt, activityShortTimeout)
					})
			})
		}
	})

	// R05: a cancellation is the end of the activity, so there is nothing left for a reset to start over
	// and the call is refused rather than recording an intent that can never be applied. Cancel outranks
	// reset (see the model's order of precedence), which is what makes refusal the answer here.
	// Currently failing: WFA.
	t.Run("R05_ResetWhileCancellationIsPending", func(t *testing.T) {
		parityDrive(t, env, deferredResetCfg, []model.Event{model.Poll, model.RequestCancel},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Reset))
			})
	})

	// R06: a second reset supersedes the one still pending rather than being refused. The common case is
	// the same operator correcting a request they have just sent, and a second operator's reset must not
	// be lost either. Superseding means replacing, not merging: a flag the first reset set and the second
	// did not must be cleared by the second. reset_heartbeat is the discriminator, being the deferred
	// intent whose effect both surfaces report (R07). Both resets and the failure are raw RPCs, because
	// driving them through the trace vocabulary would check them against a model that still refuses the
	// second reset.
	// Currently failing: SAA; WFA on the SecondResetWithdrawsClearHeartbeat arm.
	t.Run("R06_ResetWhileAnEarlierResetIsPending", func(t *testing.T) {
		for _, tc := range []struct {
			name                 string
			first, second        model.Event
			lastHeartbeatDetails []byte
		}{
			{name: "SecondResetAddsClearHeartbeat", first: model.Reset, second: model.ResetClearingHeartbeat},
			{
				name:                 "SecondResetWithdrawsClearHeartbeat",
				first:                model.ResetClearingHeartbeat,
				second:               model.Reset,
				lastHeartbeatDetails: activityMarshalPayloads(activityRecordedHeartbeatDetails),
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, deferredResetCfg, []model.Event{model.Poll, model.Heartbeat},
					func(t *testing.T, a parityActivity) {
						require.NoError(t, a.rpc(t, tc.first))
						require.NoError(t, a.rpc(t, tc.second),
							"a second reset must supersede the one still pending, not be refused")
						require.NoError(t, a.rpc(t, model.FailRetryably))
						require.Equal(t, tc.lastHeartbeatDetails, a.activityInfo(t).LastHeartbeatDetails,
							"the reset that lands is the second one, with the first's flags replaced rather than merged")
					})
			})
		}
	})

	// R07: reset carries the heartbeat checkpoint into the attempt it starts, and reset_heartbeat discards
	// it. The checkpoint here is reported after the reset was requested, so what is at stake is the flag
	// still being honoured when the deferred reset is applied. Only the checkpoint is asserted; the attempt
	// and backoff the same trace reports are R01's subject.
	// Currently failing: WFA, on the trace rather than on the assertion — the failure that ends the
	// attempt retries to attempt 2 instead of applying the pending reset, which is R08's subject.
	t.Run("R07_HeartbeatProgressAcrossAReset", func(t *testing.T) {
		for _, tc := range []struct {
			name                 string
			resetEvent           model.Event
			lastHeartbeatDetails []byte
		}{
			{
				name:                 "Keep",
				resetEvent:           model.Reset,
				lastHeartbeatDetails: activityMarshalPayloads(activityRecordedHeartbeatDetails),
			},
			{name: "Clear", resetEvent: model.ResetClearingHeartbeat},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, deferredResetCfg,
					[]model.Event{model.Poll, tc.resetEvent, model.Heartbeat, model.FailRetryably},
					func(t *testing.T, a parityActivity) {
						require.Equal(t, tc.lastHeartbeatDetails, a.activityInfo(t).LastHeartbeatDetails)
					})
			})
		}
	})

	// R08: an attempt ending with the retry budget spent would close the activity, but the pending reset
	// rewinds the attempt counter, so the budget is no longer spent and the activity must continue. This
	// is what reset exists to rescue. Driven by a retryable failure, and by each per-attempt timeout,
	// which is how the attempt ends when the worker simply goes silent.
	// Currently failing: WFA.
	t.Run("R08_AttemptEndsWithRetryBudgetSpentWhileResetPending", func(t *testing.T) {
		t.Run("RetryableFailure", func(t *testing.T) {
			parityDrive(t, env, spentBudgetCfg, []model.Event{model.Poll, model.Reset, model.FailRetryably},
				assertResetApplied)
		})
		for _, tc := range []struct {
			name string
			cfg  activityConfig
		}{
			{
				name: "StartToCloseTimeout",
				cfg:  activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration, StartToClose: activityShortTimeout},
			},
			{
				name: "HeartbeatTimeout",
				cfg:  activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration, HeartbeatTimeout: activityShortTimeout},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, tc.cfg, []model.Event{model.Poll, model.Reset},
					func(t *testing.T, a parityActivity) {
						awaitAbsorbedAttemptTimeout(t, a, resetAttempt, activityShortTimeout)
					})
			})
		}
	})

	// R09: restore_original_options is part of the reset, so on a deferred reset it lands with the reset
	// rather than ahead of it: the attempt in progress keeps running under the options it was dispatched
	// with. The heartbeat timeout stands in for the options as a whole, being the only one each surface
	// reports; each arm reads its own, as there is no shared projection of an activity's current options.
	// Currently failing: WFA.
	t.Run("R09_RestoreOriginalOptionsWhileAttemptInProgress", func(t *testing.T) {
		const originalHeartbeatTimeout = 2 * time.Hour
		// The heartbeat timeout model.UpdateOptions installs; see the drivers' updateOptions.
		const updatedHeartbeatTimeout = time.Hour
		cfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration, HeartbeatTimeout: originalHeartbeatTimeout}
		// The update is read back before the reset is requested, so that a surface reporting the original
		// timeout later is a restore rather than an update that was never visible.
		trace := []model.Event{model.Poll, model.UpdateOptions}
		const updated = "the update must be visible on the activity it was applied to"
		const inForce = "the superseded attempt keeps the options it was dispatched with"
		const restored = "applying the reset must restore the options the activity was started with"

		restoreOriginal := model.Event{Type: model.ResetType, RestoreOriginal: true}
		assertRestoreDeferred := func(t *testing.T, a parityActivity, heartbeatTimeout func() time.Duration) {
			require.Equal(t, updatedHeartbeatTimeout, heartbeatTimeout(), updated)
			a.driveEvent(t, restoreOriginal)
			require.Equal(t, updatedHeartbeatTimeout, heartbeatTimeout(), inForce)
			a.driveEvent(t, model.FailRetryably)
			require.Equal(t, originalHeartbeatTimeout, heartbeatTimeout(), restored)
		}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, trace)
			assertRestoreDeferred(t, a, func() time.Duration {
				return a.pendingActivityInfo(t).GetActivityOptions().GetHeartbeatTimeout().AsDuration()
			})
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			assertRestoreDeferred(t, a, func() time.Duration {
				return a.describe(t).GetInfo().GetHeartbeatTimeout().AsDuration()
			})
		})
	})

	// R10: once a cancellation has superseded the pending reset, the reset will never be applied, so the
	// worker must not be told of it.
	// Currently failing: WFA.
	t.Run("R10_HeartbeatAfterCancellationSupersedesAPendingReset", func(t *testing.T) {
		parityDrive(t, env, deferredResetCfg,
			[]model.Event{model.Poll, model.Reset, model.RequestCancel, model.Heartbeat},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, model.HeartbeatFlags{CancelRequested: true}, a.heartbeatFlags(),
					"the worker is told to cancel, and no longer told of a reset that will not happen")
			})
	})

	// R11: a worker that finishes its work is not superseded by a reset that was waiting for it to yield:
	// the result closes the activity, whether reported by token or by id. The after-a-retry variant is
	// what discriminates for the survey entry's claim about token staleness: a reset applied at once
	// rewinds the attempt counter under the token the worker is still holding, which only shows on a
	// later attempt.
	// Currently failing: WFA, on the ByTokenAfterARetry arm.
	t.Run("R11_AttemptCompletesWhileResetPending", func(t *testing.T) {
		assertCompleted := func(t *testing.T, a parityActivity) {
			require.Equal(t, activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
				a.terminal(t))
		}
		for _, tc := range []struct {
			name  string
			cfg   activityConfig
			trace []model.Event
		}{
			{name: "ByToken", cfg: deferredResetCfg, trace: []model.Event{model.Poll, model.Reset, model.Complete}},
			{name: "ByID", cfg: deferredResetCfg, trace: []model.Event{model.Poll, model.Reset, model.CompleteByID}},
			{
				name:  "ByTokenAfterARetry",
				cfg:   activityConfig{MaxAttempts: 10, RetryInterval: activityShortDispatchDelay},
				trace: []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.Reset, model.Complete},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, tc.cfg, tc.trace, assertCompleted)
			})
		}
	})

	// R12: priority is one of the activity's options, so restore_original_options must put it back like any
	// other. Each arm issues the priority update itself, since the trace vocabulary cannot carry one, and
	// reads the priority its own surface reports. No attempt is in progress, so the reset lands at once:
	// whether the restore is deferred while one is running is R09's subject, not this one.
	// Currently failing: WFA.
	t.Run("R12_RestoreOriginalOptionsAfterUpdatingPriority", func(t *testing.T) {
		const updatedPriorityKey = 4
		restoreOriginal := model.Event{Type: model.ResetType, RestoreOriginal: true}
		cfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}

		// The original priority is read before the update, so the assertion is that the reset put back
		// whatever the activity was started with, rather than any particular value.
		assertPriorityRestored := func(t *testing.T, a parityActivity, ref resetParityRef, priorityKey func() int32) {
			original := priorityKey()
			require.NotEqual(t, int32(updatedPriorityKey), original,
				"the update must change the priority whose restoration is asserted")
			resetParityUpdatePriority(t, env, ref, updatedPriorityKey)
			require.EqualValues(t, updatedPriorityKey, priorityKey(),
				"the priority update must be visible on the activity it was applied to")
			a.driveEvent(t, restoreOriginal)
			require.Equal(t, original, priorityKey(),
				"restore_original_options must restore the priority the activity was started with")
		}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, nil)
			assertPriorityRestored(t, a, resetParityRef{testContext: a.testContext, workflowID: a.workflowID, activityID: a.activityID, runID: a.runID},
				func() int32 { return a.pendingActivityInfo(t).GetPriority().GetPriorityKey() })
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			assertPriorityRestored(t, a, resetParityRef{testContext: a.testContext, activityID: a.activityID, runID: a.runID},
				func() int32 { return a.describe(t).GetInfo().GetPriority().GetPriorityKey() })
		})
	})

	// R13: a reset returns the activity to attempt 1, and everything the public projection says about
	// what it has already done goes with the counter. On attempt 1 there is no last failure, no last
	// started time, no last completed attempt and no last worker, so reporting any of them after a reset
	// describes an activity that no longer exists. Wiping the counter and the two retry deadlines while
	// leaving the rest standing is the arbitrary half-measure. The heartbeat checkpoint is the one
	// deliberate survivor, because reset_heartbeat exists to discard it separately (R07).
	//
	// sdk_name, sdk_version and last_deployment_version belong to the same set but are not asserted: the
	// harness polls with a bare RPC from an unversioned worker, so they are already empty before a reset.
	// Each field is asserted on its own, so that a surface which never reports one of them while backing
	// off does not hide what the reset then does to the others.
	// Currently failing: SAA; WFA on every field but LastFailure.
	t.Run("R13_ResetWhileWaitingInRetryBackoff", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}
		trace := []model.Event{model.Poll, model.FailRetryably}
		const recorded = "the attempt that just failed must be recorded while the retry backs off"
		const wiped = "a reset returns the activity to attempt 1, where this has nothing to report"

		// A field is a string or a message pointer depending on which one it is, and "reported" means
		// something different for each.
		requireReported := func(t require.TestingT, value any, msg string) {
			if s, ok := value.(string); ok {
				require.NotEmpty(t, s, msg)
				return
			}
			require.NotNil(t, value, msg)
		}
		requireWiped := func(t require.TestingT, value any, msg string) {
			if s, ok := value.(string); ok {
				require.Empty(t, s, msg)
				return
			}
			require.Nil(t, value, msg)
		}

		assertWipedByReset := func(t *testing.T, a parityActivity, read func() any) {
			requireReported(t, read(), recorded)
			a.driveEvent(t, model.Reset)
			require.Equal(t, resetAttempt, a.activityInfo(t),
				"the reset must have been applied, taking the attempt counter and both retry deadlines with it")
			requireWiped(t, read(), wiped)
		}

		for _, tc := range []struct {
			name string
			wfa  func(*wfaHandle, require.TestingT) any
			saa  func(*saaHandle, require.TestingT) any
		}{
			{
				name: "LastFailure",
				wfa:  func(a *wfaHandle, t require.TestingT) any { return a.pendingActivityInfo(t).GetLastFailure() },
				saa:  func(a *saaHandle, t require.TestingT) any { return a.describe(t).GetInfo().GetLastFailure() },
			},
			{
				name: "LastStartedTime",
				wfa:  func(a *wfaHandle, t require.TestingT) any { return a.pendingActivityInfo(t).GetLastStartedTime() },
				saa:  func(a *saaHandle, t require.TestingT) any { return a.describe(t).GetInfo().GetLastStartedTime() },
			},
			{
				name: "LastAttemptCompleteTime",
				wfa: func(a *wfaHandle, t require.TestingT) any {
					return a.pendingActivityInfo(t).GetLastAttemptCompleteTime()
				},
				saa: func(a *saaHandle, t require.TestingT) any {
					return a.describe(t).GetInfo().GetLastAttemptCompleteTime()
				},
			},
			{
				name: "LastWorkerIdentity",
				wfa:  func(a *wfaHandle, t require.TestingT) any { return a.pendingActivityInfo(t).GetLastWorkerIdentity() },
				saa:  func(a *saaHandle, t require.TestingT) any { return a.describe(t).GetInfo().GetLastWorkerIdentity() },
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Run("WorkflowActivity", func(t *testing.T) {
					a := newWFADriver(t, env, cfg).driveTrace(t, trace)
					assertWipedByReset(t, a, func() any { return tc.wfa(a, t) })
				})
				t.Run("StandaloneActivity", func(t *testing.T) {
					a := newSAADriver(t, env, cfg).driveTrace(t, trace)
					assertWipedByReset(t, a, func() any { return tc.saa(a, t) })
				})
			})
		}
	})

	// R14: both intents are pending and the worker is told of both. A keep_paused reset does not subsume
	// the pause — Describe reports the activity as pause-requested, so a heartbeat response that named
	// only the reset would contradict what an operator reading the activity is shown. The flags are
	// independent, and each says something true about what will happen when the attempt yields.
	// model.ExpectedHeartbeatFlags has to move with this, since it currently requires reset alone.
	// Currently failing: SAA.
	t.Run("R14_HeartbeatWhilePauseAndKeepPausedResetArePending", func(t *testing.T) {
		parityDrive(t, env, deferredResetCfg,
			[]model.Event{model.Poll, model.Pause, model.ResetKeepPaused, model.Heartbeat},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, model.HeartbeatFlags{ActivityPaused: true, ActivityReset: true}, a.heartbeatFlags(),
					"a worker whose attempt is both superseded and pause-bound must be told of both")
			})
	})

	// R15: a token from an attempt a reset superseded must be refused however the attempt counter has
	// moved. A token checked against the attempt number alone matches whenever the counter comes back
	// round to the number the token carries, and accepting it closes the activity with the result of an
	// abandoned attempt, discarding the one the reset started. The two arms are the two routes to that
	// coincidence, and the worker whose token it is differs between them.
	//
	// AttemptEndedBeforeTheReset: the counter lands back where the token was issued. Attempt 1 failed, the
	// retry made it attempt 2, and the reset rewound it to 1, which is where the token was issued.
	//
	// AttemptStillRunningAtTheReset: the counter climbs back to where the token was issued. The reset
	// arrives during attempt 2, so WFA rewinds the counter to 1 under a worker that is still running and
	// is not told (R16); the attempt's start-to-close timeout then ends it and the retry off 1 makes it
	// attempt 2 again, which is where that worker's token was issued. So the worker the reset abandoned is
	// still alive and can now close the activity with the very result the reset was asking to discard.
	// Currently failing: WFA.
	t.Run("R15_StaleTokenFromBeforeAResetIsRejected", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			cfg   activityConfig
			trace []model.Event
			// supersede requests the reset, and returns the attempt the activity has started over on.
			// The token of the attempt it supersedes has already been captured.
			supersede func(*testing.T, parityActivity) int32
		}{
			{
				name:  "AttemptEndedBeforeTheReset",
				cfg:   deferredResetCfg,
				trace: []model.Event{model.Poll},
				supersede: func(t *testing.T, a parityActivity) int32 {
					for _, e := range []model.Event{model.FailRetryably, model.Reset, model.Poll} {
						a.driveEvent(t, e)
					}
					return a.driverState().startedAttempt
				},
			},
			{
				name: "AttemptStillRunningAtTheReset",
				cfg: activityConfig{
					MaxAttempts:   10,
					RetryInterval: activityShortDispatchDelay,
					StartToClose:  activityShortTimeout,
				},
				trace: []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
				supersede: func(t *testing.T, a parityActivity) int32 {
					a.driveEvent(t, model.Reset)
					// The timeout that ends the superseded attempt is absorbed by the reset on one surface
					// and advances the counter on the other, so the driver's timer wait cannot express it;
					// what both must do is start the activity over, which a poll establishes.
					resumed := a.pollForTask(t, activityShortTimeout+activityShortDispatchDelay+activityDriverTimerMargin)
					require.NotNil(t, resumed,
						"the superseded attempt's start-to-close timeout must end it and the activity must start over")
					return resumed.GetAttempt()
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, tc.cfg, tc.trace, func(t *testing.T, a parityActivity) {
					superseded := a.driverState().token
					attempt := tc.supersede(t, a)
					a.driverState().token = superseded
					var notFound *serviceerror.NotFound
					require.ErrorAs(t, a.rpc(t, model.Complete), &notFound,
						"a token from an attempt the reset superseded must not be accepted")
					require.Equal(t,
						activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: attempt},
						a.activityInfo(t),
						"the attempt the reset started must still be running")
				})
			})
		}
	})

	// R16 (candidate entry): a reset requested during an attempt must be reported to that worker on its
	// next heartbeat. SAA reports it on every attempt. WFA reports it on attempt 1 only: from attempt 2
	// onwards WFA's reset rewinds the attempt counter straight away, the worker's token stops matching,
	// and the server validates the token before it reads the reset flag, so the heartbeat is refused and
	// the worker learns nothing. The arms sit either side of that boundary: OnFirstAttempt
	// holds on both surfaces, AfterARetry is the entry.
	// Currently failing: WFA, on the AfterARetry arm.
	t.Run("R16_HeartbeatAfterAResetIsToldOfIt", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			cfg   activityConfig
			trace []model.Event
		}{
			{
				name:  "OnFirstAttempt",
				cfg:   deferredResetCfg,
				trace: []model.Event{model.Poll, model.Reset},
			},
			{
				name:  "AfterARetry",
				cfg:   activityConfig{MaxAttempts: 10, RetryInterval: activityShortDispatchDelay},
				trace: []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.Reset},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				parityDrive(t, env, tc.cfg, tc.trace, func(t *testing.T, a parityActivity) {
					require.NoError(t, a.rpc(t, model.Heartbeat),
						"the worker is still the one holding the attempt, so its heartbeat must be answered")
					require.Equal(t, model.HeartbeatFlags{ActivityReset: true}, a.heartbeatFlags(),
						"a worker whose attempt a reset superseded must be told of the reset")
				})
			})
		}
	})

	// R17 (candidate entry): what a reset arriving during a pending cancellation costs the activity, as
	// distinct from what the reset call answers, which is R05. The worker has been told to cancel and
	// will acknowledge; that acknowledgement is how the activity comes to close as CANCELED. SAA refuses
	// the reset, so the token the worker holds stays valid and the acknowledgement lands. WFA accepts it
	// and rewinds the attempt counter, which on any attempt past the first revokes that same token — so
	// the acknowledgement is refused, nothing the worker says is heard again, and the activity closes on
	// a timeout instead. The cancellation still prevails either way; the reset only changes how, and
	// under what name, it gets there.
	// Currently failing: WFA.
	t.Run("R17_CancellationIsAcknowledgedAfterAReset", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 10, RetryInterval: activityShortDispatchDelay}
		trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.RequestCancel}
		parityDrive(t, env, cfg, trace, func(t *testing.T, a parityActivity) {
			_ = a.rpc(t, model.Reset) // accepted by WFA, refused by SAA; R05 is the row for which
			require.NoError(t, a.rpc(t, model.RespondCanceled),
				"a reset must not revoke the token the cancelled worker needs to acknowledge with")
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, a.terminal(t).Status,
				"an activity cancelled before the reset arrived must still close as CANCELED")
		})
	})
}

// TestPauseUnpause covers the survey's pause/unpause (P) entries.
func (s *activityParityTestSuite) TestPauseUnpause() {
	t := s.T()
	env := newActivityParityEnv(t)

	// pausedRetryCfg leaves the activity retries to spare and a retry interval long enough that a retry
	// which lands paused stays where the pause put it, rather than being raced by a dispatch.
	pausedRetryCfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}
	// pausedRetry is where an attempt that ends under a pending pause must leave the activity: a second
	// attempt, held back by the pause, so with no dispatch deadline or retry interval to report.
	pausedRetry := activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 2}

	timedOutOnScheduleToClose := activityTerminalProjection{
		Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String(),
		RetryState:  enumspb.RETRY_STATE_TIMEOUT,
	}

	// P01: a pending pause must not cost the running attempt the server-side timeout that ends it when
	// the worker goes silent. The start-to-close timeout fires, and the retry it schedules is where the
	// pause takes effect — otherwise a paused activity with an unresponsive worker never pauses at all.
	// Also covered, for both surfaces, by TestPauseActivityExecution/StartToCloseTimeoutWhilePauseRequested.
	// Currently failing: WFA.
	t.Run("P01_StartToCloseTimeoutWhilePausePending", func(t *testing.T) {
		cfg := pausedRetryCfg
		cfg.StartToClose = activityShortTimeout
		parityDrive(t, env, cfg, []model.Event{model.Poll, model.Pause, model.StartToCloseElapses},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, pausedRetry, a.activityInfo(t),
					"the start-to-close timeout must fire under a pending pause, landing the retry paused")
			})
	})

	// P02: as P01 for the heartbeat timeout, which is the other clock a worker that stops responding
	// runs out. Also covered by TestPauseActivityExecution/HeartbeatTimeoutWhilePauseRequested.
	// Currently failing: WFA.
	t.Run("P02_HeartbeatTimeoutWhilePausePending", func(t *testing.T) {
		cfg := pausedRetryCfg
		cfg.HeartbeatTimeout = activityShortTimeout
		parityDrive(t, env, cfg, []model.Event{model.Poll, model.Pause, model.HeartbeatElapses},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, pausedRetry, a.activityInfo(t),
					"the heartbeat timeout must fire under a pending pause, landing the retry paused")
			})
	})

	// P03: schedule-to-close is the user's absolute bound on the activity. Pausing suspends dispatch,
	// not that deadline, so the deadline still closes a paused activity — a pause that suspended it
	// would leave the activity alive with no bound at all. Also covered by
	// TestPauseActivityExecution/ScheduleToCloseTimeoutWhilePaused.
	// Currently failing: WFA.
	t.Run("P03_ScheduleToCloseDeadlinePassesWhilePaused", func(t *testing.T) {
		parityDrive(t, env, activityConfig{RetryInterval: activityLongDuration},
			[]model.Event{model.Pause, model.ScheduleToCloseElapses},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, timedOutOnScheduleToClose, a.terminal(t),
					"a paused activity is still subject to its schedule-to-close deadline")
			})
	})

	// P04: a pause is identified by its request id, so a redelivery of one the user has since unpaused
	// must be recognised as already applied. Re-pausing would silently undo the unpause. Also covered by
	// TestPauseActivityExecution/PauseReplayAfterUnpause_IsDeduplicated.
	// Currently failing: WFA.
	t.Run("P04_PauseReplayAfterUnpause", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration},
			[]model.Event{model.Pause},
			func(t *testing.T, a parityActivity) {
				establishRequestID(a, model.PauseType)
				a.driveEvent(t, model.Unpause)
				a.driveEvent(t, model.Event{Type: model.PauseType, SameRequestID: true})
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 1},
					a.activityInfo(t),
					"a replayed pause whose request id was already consumed must not re-pause the activity")
				// The dispatch the unpause released must still reach a poller, which the run state alone
				// does not establish.
				a.driveEvent(t, model.Poll)
			})
	})

	// P05: there is no pause to withdraw, so unpause is refused. Reporting success would tell the user
	// they undid something they never did. Also covered by TestUnpauseWithoutPause, whose WFA arm is
	// skipped.
	// Currently failing: WFA.
	t.Run("P05_UnpauseWithoutPause", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}, nil,
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Unpause))
			})
	})

	// P06: cancellation outranks pausing — the activity is on its way out, so there is nothing to
	// suspend it for, and accepting the pause would suggest the cancellation had been held back. Also
	// covered by TestPauseActivityExecution/PauseWhileCancelRequested.
	// Currently failing: WFA.
	t.Run("P06_PauseWhileCancelRequested", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.RequestCancel},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Pause))
			})
	})

	// P07: a pending cancellation is not a pause, so unpause has nothing to undo. Also covered by
	// TestUnpauseActivityExecution/UnpauseWhileCancelRequestedFails.
	// Currently failing: WFA.
	t.Run("P07_UnpauseWhileCancelRequested", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.RequestCancel},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Unpause))
			})
	})

	// P08: a pending reset is likewise not a state to pause from: the reset is waiting for the attempt
	// to end, and a pause accepted now would silently change what the reset lands in. Also covered by
	// TestPauseActivityExecution/PauseWhileResetRequested.
	// Currently failing: WFA.
	t.Run("P08_PauseWhileResetRequested", func(t *testing.T) {
		parityDrive(t, env, pausedRetryCfg, []model.Event{model.Poll, model.Reset},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Pause))
			})
	})

	// P09: reset_attempts and reset_heartbeat rewind the attempt counter and discard the checkpoint as
	// part of the unpause. Only the workflow-activity UnpauseActivity request carries those fields —
	// UnpauseActivityExecution, the request both surfaces share, has neither — so what is at stake is
	// the shape of each surface's API rather than a behavior they could both be held to. Hence
	// per-surface arms. TestResetSubstitutesForUnpauseFlags asserts, for both surfaces, that Reset
	// covers what the flags cover.
	t.Run("P09_UnpauseResetAttemptsAndResetHeartbeat", func(t *testing.T) {
		// Paused while backing off on attempt 2, with a checkpoint from attempt 1 to discard, so both
		// flags have something to do.
		trace := []model.Event{model.Poll, model.Heartbeat, model.FailRetryably, model.Pause}
		recorded := activityMarshalPayloads(activityRecordedHeartbeatDetails)

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, pausedRetryCfg).driveTrace(t, trace)
			require.Equal(t, recorded, a.activityInfo(t).LastHeartbeatDetails,
				"the paused retry carries the checkpoint the flag is to discard")
			_, err := env.FrontendClient().UnpauseActivity(a.testContext(), &workflowservice.UnpauseActivityRequest{
				Namespace:      env.Namespace().String(),
				Execution:      &commonpb.WorkflowExecution{WorkflowId: a.workflowID, RunId: a.runID},
				Activity:       &workflowservice.UnpauseActivityRequest_Id{Id: a.activityID},
				Identity:       env.Tv().ClientIdentity(),
				ResetAttempts:  true,
				ResetHeartbeat: true,
			})
			require.NoError(t, err)
			info := a.activityInfo(t)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.RunState,
				"the unpause must release the retry")
			require.Equal(t, int32(1), info.Attempt, "reset_attempts must rewind the attempt counter")
			require.Nil(t, info.LastHeartbeatDetails, "reset_heartbeat must discard the checkpoint")
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, pausedRetryCfg).driveTrace(t, trace)
			a.driveEvent(t, model.Unpause)
			info := a.activityInfo(t)
			require.Equal(t, int32(2), info.Attempt,
				"the standalone unpause has no reset_attempts flag, so the attempt counter stands")
			require.Equal(t, recorded, info.LastHeartbeatDetails,
				"the standalone unpause has no reset_heartbeat flag, so the checkpoint stands")
			a.driveEvent(t, model.ResetClearingHeartbeat)
			info = a.activityInfo(t)
			require.Equal(t, int32(1), info.Attempt, "a separate Reset is what rewinds the attempt counter")
			require.Nil(t, info.LastHeartbeatDetails,
				"a separate Reset carrying reset_heartbeat is what discards the checkpoint")
		})
	})

	// P10: an activity must report who issued each operator command against it, and why, so that whoever
	// arrives at a paused or reset activity next can see whose decision it was and act on it. Today WFA
	// reports this for pause alone, through pause_info, and SAA reports nothing for any of the four, so
	// every arm fails.
	//
	// Reset and UpdateActivityExecutionOptions carry an identity but have no reason field on the wire,
	// so only their identity is asserted; asking for a reason there needs an api change first. What is
	// searched is the whole Describe response rather than a named field, because on both surfaces the
	// fields these answers would live in do not exist yet, and a field that does not exist cannot be
	// read. Each command uses its own identity and reason so that a hit cannot come from an earlier one.
	// Currently failing: SAA; WFA on every command but Pause.
	t.Run("P10_WhoIssuedAnOperatorCommandAndWhy", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		for _, tc := range []struct {
			name string
			// prepare puts the activity in a state the command is accepted from.
			prepare []model.Event
			// reason is empty for a command whose request has no reason field.
			reason string
			issue  func(context.Context, operatorTarget, string, string) error
		}{
			{name: "Pause", reason: "p10-reason-pause", issue: operatorPause},
			{name: "Unpause", prepare: []model.Event{model.Pause}, reason: "p10-reason-unpause", issue: operatorUnpause},
			{name: "Reset", issue: operatorReset},
			{name: "UpdateOptions", issue: operatorUpdateOptions},
		} {
			t.Run(tc.name, func(t *testing.T) {
				identity := "p10-identity-" + tc.name
				assertAttributed := func(t *testing.T, ctx context.Context, target operatorTarget, describe func() string) {
					require.NoError(t, tc.issue(ctx, target, identity, tc.reason))
					require.Contains(t, describe(), identity,
						"the activity must report who issued the command")
					if tc.reason != "" {
						require.Contains(t, describe(), tc.reason,
							"...and the reason they gave for it")
					}
				}

				t.Run("WorkflowActivity", func(t *testing.T) {
					a := newWFADriver(t, env, cfg).driveTrace(t, tc.prepare)
					assertAttributed(t, a.testContext(),
						operatorTarget{env: env, workflowID: a.workflowID, activityID: a.activityID, runID: a.runID},
						func() string { return a.pendingActivityInfo(t).String() })
				})
				t.Run("StandaloneActivity", func(t *testing.T) {
					a := newSAADriver(t, env, cfg).driveTrace(t, tc.prepare)
					assertAttributed(t, a.testContext(),
						operatorTarget{env: env, activityID: a.activityID, runID: a.runID},
						func() string { return a.describe(t).String() })
				})
			})
		}
	})

	// P11: the pause takes effect even though the retry budget is spent. A paused activity is one the
	// user has stopped, not one the server has finished with, and closing it on the retry policy would
	// decide the question the pause was asked in order to postpone — the operator paused precisely so
	// they could look at it before it went anywhere. The activity parks PAUSED at the next attempt; the
	// budget is consulted when it is unpaused, not before.
	// Currently failing: SAA.
	t.Run("P11_LastPermittedAttemptFailsRetryablyWhilePausePending", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.Pause, model.FailRetryably},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, pausedRetry, a.activityInfo(t),
					"a pause must be honoured whether or not the activity has retries left")
			})
	})

	// P12: an unpause is identified by its request id, as a pause is (P04), so a redelivery of one the
	// user has since re-paused past must be recognised as already applied: the later pause stands. Both
	// pauses and the replay are sent as raw RPCs, because the trace vocabulary sends no request id on
	// unpause, and driving them raw leaves the model cursor behind — nothing may be driven afterwards.
	// Currently failing: WFA.
	t.Run("P12_UnpauseReplayAfterPause", func(t *testing.T) {
		assertReplayIgnored := func(t *testing.T, a parityActivity, unpause pauseParityUnpause) {
			spent := uuid.NewString()
			require.NoError(t, a.rpc(t, model.Pause))
			require.NoError(t, unpause(spent, 0))
			require.NoError(t, a.rpc(t, model.Pause))
			// Whether a recognised replay is reported as success or refused is not this claim; what the
			// activity is left in is.
			_ = unpause(spent, 0)
			require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, a.activityInfo(t).RunState,
				"a replayed unpause whose request id was already consumed must not undo the later pause")
		}
		cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, nil)
			assertReplayIgnored(t, a, pauseParityUnpauseWFA(a))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, nil)
			assertReplayIgnored(t, a, pauseParityUnpauseSAA(a))
		})
	})

	// P13: unpause means resume now. An operator who paused an activity, fixed whatever was wrong and
	// unpaused it is not thinking about how far into attempt 10's backoff it happened to be, and making
	// them wait out a deadline computed before the fix serves nothing. So the remaining backoff is
	// discarded and the dispatch is scheduled from the unpause, within the jitter window the caller
	// asked for. A start delay is the opposite case and is honoured, being an absolute time the caller
	// chose rather than a deadline the retry policy computed.
	//
	// The retry interval here is far longer than the jitter, so the deadline reported after the unpause
	// discriminates cleanly. Per-surface arms, because the trace vocabulary cannot send jitter and the
	// shared activityInfo projection reduces the deadline to a bool.
	// Currently failing: SAA.
	t.Run("P13_UnpauseAnActivityPausedInRetryBackoff", func(t *testing.T) {
		trace := []model.Event{model.Poll, model.FailRetryably}
		assertBackoffDiscarded := func(
			t *testing.T,
			a parityActivity,
			nextAttemptScheduleTime func() *timestamppb.Timestamp,
			unpause pauseParityUnpause,
		) {
			deadline := nextAttemptScheduleTime()
			require.NotNil(t, deadline, "an activity backing off must report the deadline its retry dispatches at")
			a.driveEvent(t, model.Pause)
			unpausedAt := time.Now()
			require.NoError(t, unpause(uuid.NewString(), pauseParityUnpauseJitter))
			released := nextAttemptScheduleTime()
			require.NotNil(t, released, "the released retry must still report the deadline it dispatches at")
			require.True(t, released.AsTime().Before(deadline.AsTime()),
				"unpausing must discard the remaining backoff rather than serving out a deadline set before the pause")
			// Bounded on both sides by the test's own clock, so the margin covers skew against the server's.
			require.False(t, released.AsTime().Before(unpausedAt.Add(-activityDriverTimerMargin)),
				"...and must not dispatch before the unpause that released it")
			require.False(t, released.AsTime().After(unpausedAt.Add(pauseParityUnpauseJitter+activityDriverTimerMargin)),
				"...placing the dispatch inside the jitter window the unpause asked for")
		}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, pausedRetryCfg).driveTrace(t, trace)
			assertBackoffDiscarded(t, a, func() *timestamppb.Timestamp {
				return a.pendingActivityInfo(t).GetNextAttemptScheduleTime()
			}, pauseParityUnpauseWFA(a))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, pausedRetryCfg).driveTrace(t, trace)
			assertBackoffDiscarded(t, a, func() *timestamppb.Timestamp {
				return a.describe(t).GetInfo().GetNextAttemptScheduleTime()
			}, pauseParityUnpauseSAA(a))
		})
	})

	// P14: as P08 for unpause. A pending reset is not a pause, so there is nothing for unpause to undo,
	// and accepting it would suggest the reset had been withdrawn.
	// Currently failing: WFA.
	t.Run("P14_UnpauseWhileResetRequested", func(t *testing.T) {
		parityDrive(t, env, pausedRetryCfg, []model.Event{model.Poll, model.Reset},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Unpause))
			})
	})
}

// matrixAnswer is what an operator command answers when it arrives in a given state.
type matrixAnswer int

const (
	matrixAccepted matrixAnswer = iota
	matrixRefused               // FailedPrecondition
	matrixAbsent                // NotFound
)

// matrixState is a state an activity can be in when an operator command reaches it, and the trace
// that puts it there. flags is what a heartbeat reports while it sits there, which a refused command
// must leave alone; it is only read where an attempt is running, since otherwise no worker is holding
// a token to heartbeat with.
type matrixState struct {
	name           string
	trace          []model.Event
	attemptRunning bool
	flags          model.HeartbeatFlags
	terminal       bool
	// answers is one entry per entry of matrixCommands, in that order.
	answers []matrixAnswer
}

// matrixCommand is an operator command under test. issue bypasses the driver's model check, so
// nothing may be driven on the handle after it. onWFA is false for the commands a workflow activity
// does not have as an operator RPC at all: it has no terminate and no per-activity delete, and its
// cancellation arrives as a workflow command rather than a call an operator can make and be answered.
type matrixCommand struct {
	name  string
	onWFA bool
	issue func(t *testing.T, a parityActivity) error
}

// TestOperatorCommandMatrix drives every cell of the operator-command matrix: for each state an
// activity can be in when a command arrives, what that command answers. The individual survey entries
// each establish one cell and say why it is what it is; this establishes that the whole table holds
// together, and covers the cells no entry claims because both surfaces already agree on them.
//
// Three rules generate the table. A closed activity answers NotFound (A02). Pending transitions are
// ranked Terminate > Cancel > Reset, and a pending one refuses whatever it outranks while yielding to
// whatever outranks it (P06, P08, R05, U01, U02). Pause is a state rather than a pending transition,
// so it blocks nothing but Unpause's precondition — which is why Reset and UpdateOptions are accepted
// against a paused activity and refused against a resetting one. Delete sits above the ranking and is
// never refused, being the one command that terminates as a means to its own end.
//
// Where a command is refused and a worker is still holding the attempt, the refusal must also be
// invisible to that worker: its next heartbeat reports the flags of the state it is already in, and
// not the one the refused command would have produced. R05 and R10 are the two entries that turn on
// this, and both are cells here.
//
// Currently failing, 21 cells of 77:
//   - WFA answers success where the table refuses — Unpause from Scheduled and Started (P05), Pause,
//     Unpause and UpdateOptions from ResetRequested (P08, P14, U02), and Pause, Unpause, Reset and
//     UpdateOptions from CancelRequested (P06, P07, R05, U01). WFA has no RESET_REQUESTED state, so
//     its whole ResetRequested row is really R01 showing through.
//   - SAA answers FailedPrecondition where the table accepts — a second Pause from Paused and from
//     PauseRequested, a second Reset from ResetRequested (R06), and a second RequestCancel from
//     CancelRequested. This is the equal-rank-supersedes rule, which neither surface implements for
//     Pause and which SAA implements for nothing.
//   - SAA answers FailedPrecondition rather than NotFound from a terminal status, in all six of its
//     Terminal cells (A02). WFA already answers NotFound there.
func (s *activityParityTestSuite) TestOperatorCommandMatrix() {
	t := s.T()
	env := newActivityParityEnv(t)

	deleteActivity := func(t *testing.T, a parityActivity) error {
		h, ok := a.(*saaHandle)
		require.True(t, ok, "delete is a standalone-activity command")
		_, err := env.FrontendClient().DeleteActivityExecution(h.testContext(),
			&workflowservice.DeleteActivityExecutionRequest{
				Namespace: env.Namespace().String(), ActivityId: h.activityID, RunId: h.runID,
			})
		return err
	}
	viaTrace := func(e model.Event) func(*testing.T, parityActivity) error {
		return func(t *testing.T, a parityActivity) error { return a.rpc(t, e) }
	}

	commands := []matrixCommand{
		{name: "Unpause", onWFA: true, issue: viaTrace(model.Unpause)},
		{name: "Pause", onWFA: true, issue: viaTrace(model.Pause)},
		{name: "Reset", onWFA: true, issue: viaTrace(model.Reset)},
		{name: "RequestCancel", issue: viaTrace(model.RequestCancel)},
		{name: "Terminate", issue: viaTrace(model.Terminate)},
		{name: "Delete", issue: deleteActivity},
		{name: "UpdateOptions", onWFA: true, issue: viaTrace(model.UpdateOptions)},
	}

	acc, ref, abs := matrixAccepted, matrixRefused, matrixAbsent
	states := []matrixState{
		//                                                       Unpause Pause Reset Cancel Term  Delete Update
		{
			name:    "Scheduled",
			answers: []matrixAnswer{ref, acc, acc, acc, acc, acc, acc},
		},
		{
			name: "Started", trace: []model.Event{model.Poll}, attemptRunning: true,
			answers: []matrixAnswer{ref, acc, acc, acc, acc, acc, acc},
		},
		{
			name: "Paused", trace: []model.Event{model.Pause},
			answers: []matrixAnswer{acc, acc, acc, acc, acc, acc, acc},
		},
		{
			name: "PauseRequested", trace: []model.Event{model.Poll, model.Pause}, attemptRunning: true,
			flags:   model.HeartbeatFlags{ActivityPaused: true},
			answers: []matrixAnswer{acc, acc, acc, acc, acc, acc, acc},
		},
		{
			name: "ResetRequested", trace: []model.Event{model.Poll, model.Reset}, attemptRunning: true,
			flags:   model.HeartbeatFlags{ActivityReset: true},
			answers: []matrixAnswer{ref, ref, acc, acc, acc, acc, ref},
		},
		{
			name: "CancelRequested", trace: []model.Event{model.Poll, model.RequestCancel}, attemptRunning: true,
			flags:   model.HeartbeatFlags{CancelRequested: true},
			answers: []matrixAnswer{ref, ref, ref, acc, acc, acc, ref},
		},
		{
			name: "Terminal", trace: []model.Event{model.Poll, model.Complete}, terminal: true,
			answers: []matrixAnswer{abs, abs, abs, abs, abs, acc, abs},
		},
	}

	// An activity with retries to spare and no deadline close enough to fire, so that nothing but the
	// command under test moves it out of the state the trace put it in.
	cfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}
	start := func(t *testing.T, wfa bool, st matrixState) parityActivity {
		if wfa {
			d := newWFADriver(t, env, cfg)
			// A closed workflow activity leaves the pending set with its workflow, and an operator
			// command answered about a vanished workflow says nothing about a closed activity.
			d.holdOpen = st.terminal
			return d.driveTrace(t, st.trace)
		}
		return newSAADriver(t, env, cfg).driveTrace(t, st.trace)
	}

	for _, st := range states {
		t.Run(st.name, func(t *testing.T) {
			require.Len(t, st.answers, len(commands), "every command needs an answer in every state")
			for i, cmd := range commands {
				want := st.answers[i]
				t.Run(cmd.name, func(t *testing.T) {
					run := func(t *testing.T, a parityActivity) {
						err := cmd.issue(t, a)
						switch want {
						case matrixAccepted:
							require.NoError(t, err)
							return
						case matrixRefused:
							requireFailedPrecondition(t, err)
						case matrixAbsent:
							var notFound *serviceerror.NotFound
							require.ErrorAs(t, err, &notFound)
						default:
							t.Fatalf("unhandled matrix answer %d", want)
						}
						if !st.attemptRunning {
							return
						}
						require.NoError(t, a.rpc(t, model.Heartbeat),
							"the worker still holds the attempt, so its heartbeat must still be answered")
						require.Equal(t, st.flags, a.heartbeatFlags(),
							"a refused command must not reach the worker as though it had been accepted")
					}
					if cmd.onWFA {
						t.Run("WorkflowActivity", func(t *testing.T) { run(t, start(t, true, st)) })
					}
					t.Run("StandaloneActivity", func(t *testing.T) { run(t, start(t, false, st)) })
				})
			}
		})
	}
}

// TestUpdateOptions covers the survey's update-options (U) entries.
func (s *activityParityTestSuite) TestUpdateOptions() {
	t := s.T()
	env := newActivityParityEnv(t)

	// U01: a pending cancellation is a decision to stop the activity, so options that may never be used
	// again cannot be updated: the call is refused, and the refusal must leave the cancellation in place.
	// TestPauseActivityExecution's UpdateWhileCancelRequestedFails (activity_parity_ported_test.go) already
	// covers this; this is the entry's own subtest.
	// Currently failing: WFA.
	t.Run("U01_UpdateWhileCancelRequested", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1}, []model.Event{model.Poll, model.RequestCancel},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.UpdateOptions))
				require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED, a.activityInfo(t).RunState,
					"a refused update must leave the pending cancellation in place")
			})
	})

	// U02: a pending reset will restart the activity, and may restore the options it was started with, so
	// an update landing between the request and its application has no well-defined effect and is refused.
	// The refusal must leave the reset pending: it is still applied when the attempt ends. Sibling of
	// TestPauseActivityExecution's PauseWhileResetRequested (activity_parity_ported_test.go), which refuses
	// a pause in the same state.
	// Currently failing: WFA.
	t.Run("U02_UpdateWhileResetRequested", func(t *testing.T) {
		// A long retry interval makes the applied reset legible: an ordinary retry would be attempt 2
		// waiting the interval out, whereas the reset is a dispatchable attempt 1.
		cfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}
		parityDrive(t, env, cfg, []model.Event{model.Poll, model.Reset},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.UpdateOptions))
				a.driveEvent(t, model.FailRetryably)
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 1},
					a.activityInfo(t), "a refused update must leave the pending reset to be applied")
			})
	})

	// U03: an accepted update to the non-retryable error types must be the list that decides the next
	// retry, exactly as if it had been given at start time — an update the server accepts but neither
	// reports nor obeys is worse than a refusal. The whole retry_policy is the field-mask path that
	// carries the list (see common/activityoptions.MergeActivityOptions), so every other policy field is
	// re-sent unchanged and the list is the only change.
	// Currently failing: WFA, on the SetByUpdate arm.
	t.Run("U03_UpdateNonRetryableErrorTypes", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}
		nonRetryable := []string{updateParityFailureType}
		updated := &activitypb.ActivityOptions{RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(cfg.retryInterval()),
			BackoffCoefficient:     1.0,
			MaximumInterval:        durationpb.New(cfg.retryInterval()),
			MaximumAttempts:        cfg.MaxAttempts,
			NonRetryableErrorTypes: nonRetryable,
		}}
		const decided = "a failure whose type is listed non-retryable must close the activity, not retry it"
		// The failure closes the activity in the same transaction that reports it, so closure is readable
		// at once. Reading it before the terminal outcome is what makes a surface that retried instead fail
		// here rather than block waiting for a close that is a retry interval away.
		assertClosedNonRetryably := func(t *testing.T, v updateParityView) {
			v.driveEvent(t, model.FailRetryably)
			require.True(t, v.closed(t), decided)
			require.Equal(t, updateParityNonRetryableTerminal, v.terminal(t), decided)
		}

		// The behavior the updated list has to reproduce, with the list given at start time instead.
		t.Run("SetAtStartTime", func(t *testing.T) {
			startCfg := cfg
			startCfg.NonRetryableErrorTypes = nonRetryable
			updateParityDrive(t, env, startCfg, []model.Event{model.Poll}, assertClosedNonRetryably)
		})

		t.Run("SetByUpdate", func(t *testing.T) {
			updateParityDrive(t, env, cfg, []model.Event{model.Poll}, func(t *testing.T, v updateParityView) {
				require.NoError(t, updateParitySend(v.testContext(), env, v.ids, updated, "retry_policy"))
				require.Equal(t, nonRetryable, v.policy(t).GetNonRetryableErrorTypes(),
					"an accepted update to the non-retryable error types must be reported back")
				assertClosedNonRetryably(t, v)
			})
		})
	})

	// U04: a worker-supplied next_retry_delay is that attempt's own instruction about when to retry, not a
	// value derived from the retry policy, so an options update must leave the pending dispatch where the
	// worker put it — including an update to the retry policy, whose new interval governs later attempts
	// rather than the one already scheduled.
	// Currently failing: WFA.
	t.Run("U04_UpdateWhileServingWorkerRetryDelay", func(t *testing.T) {
		cfg := activityConfig{
			MaxAttempts:    10,
			RetryInterval:  activityLongDuration,
			NextRetryDelay: updateParityWorkerRetryDelay,
		}
		trace := []model.Event{model.Poll, model.FailRetryably}
		updatedPolicy := &activitypb.ActivityOptions{RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(updateParityUpdatedRetryInterval),
		}}

		assertOverrideSurvives := func(t *testing.T, v updateParityView, update func(*testing.T, updateParityView)) {
			interval, dispatchTime := v.pending(t)
			require.Equal(t, updateParityWorkerRetryDelay, interval,
				"the worker's next_retry_delay must be the pending retry's interval")
			require.NotNil(t, dispatchTime, "a backing-off retry must report when it will be dispatched")
			update(t, v)
			updatedInterval, updatedDispatchTime := v.pending(t)
			require.Equal(t, updateParityWorkerRetryDelay, updatedInterval,
				"an options update must not replace the worker's next_retry_delay with a policy interval")
			require.NotNil(t, updatedDispatchTime, "the retry must still be pending after the update")
			require.Equal(t, dispatchTime.AsTime(), updatedDispatchTime.AsTime(),
				"the pending retry's dispatch time must survive an options update")
		}

		for _, tc := range []struct {
			name   string
			update func(*testing.T, updateParityView)
		}{
			{
				name:   "UnrelatedOption",
				update: func(t *testing.T, v updateParityView) { v.driveEvent(t, model.UpdateOptions) },
			},
			{
				name: "RetryPolicyInterval",
				update: func(t *testing.T, v updateParityView) {
					require.NoError(t, updateParitySend(v.testContext(), env, v.ids, updatedPolicy,
						"retry_policy.initial_interval"))
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				updateParityDrive(t, env, cfg, trace, func(t *testing.T, v updateParityView) {
					assertOverrideSurvives(t, v, tc.update)
				})
			})
		}
	})
}

// --- helpers ---------------------------------------------------------------------------------
//
// Everything below is machinery for the four tests above: each is prefixed with the section that
// uses it.

// coreParityWFAActivityType is the activity type wfaSingleActivityWorkflow schedules. A07 and A17 name
// it in a request, which is the only way to reach an activity by type.
const coreParityWFAActivityType = "testWFA"

// Metadata a standalone activity can be started with; A09 and A13 read it back. CustomKeywordField is
// one of the search attributes every test namespace registers.
var (
	coreParitySearchAttributes = &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
		"CustomKeywordField": sadefs.MustEncodeValue("core-parity", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
	}}
	coreParityHeader = &commonpb.Header{Fields: map[string]*commonpb.Payload{
		"core-parity": payload.EncodeString("header value"),
	}}
	coreParityUserMetadata = &sdkpb.UserMetadata{
		Summary: payload.EncodeString("core-parity summary"),
		Details: payload.EncodeString("core-parity details"),
	}
)

// coreParityLinks is a single link to an event of another execution, as a caller that started the
// activity on someone's behalf would attach.
func coreParityLinks(ns string) []*commonpb.Link {
	return []*commonpb.Link{{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: &commonpb.Link_WorkflowEvent{
		Namespace:  ns,
		WorkflowId: "core-parity-caller",
		RunId:      "core-parity-caller-run",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	}}}}
}

// coreParityCallbacks is one Nexus completion callback, for A22.
func coreParityCallbacks(url string) []*commonpb.Callback {
	return []*commonpb.Callback{
		{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: url}}},
	}
}

// coreParityAwaitVisible waits for query to return want activity executions, allowing for the delay
// before a new record is indexed.
func coreParityAwaitVisible(t *testing.T, env *testcore.TestEnv, query string, want int) {
	await.Require(testcontext.For(t), t, func(t *await.T) {
		resp, err := env.FrontendClient().ListActivityExecutions(t.Context(),
			&workflowservice.ListActivityExecutionsRequest{
				Namespace: env.Namespace().String(), PageSize: 10, Query: query,
			})
		t.Require().NoError(err)
		t.Require().Len(resp.GetExecutions(), want, "expected %d result(s) for query: %s", want, query)
	}, testcore.WaitForESToSettle, activityDriverPollInterval)
}

// coreParityActivityUnderWorkflow starts a wrapper workflow with the given start options, waits for it
// to schedule its activity, and returns that activity's task queue and a reader for its
// PendingActivityInfo. wfaDriver always starts its workflows with default options, whereas A19 and A20
// turn on an option of the enclosing workflow rather than of the activity.
func coreParityActivityUnderWorkflow(
	t *testing.T,
	env *testcore.TestEnv,
	cfg activityConfig,
	opts sdkclient.StartWorkflowOptions,
) (activityTaskQueue string, pending func(require.TestingT) *workflowpb.PendingActivityInfo) {
	const activityID = "act"
	d := newWFADriver(t, env, cfg)
	opts.ID = testcore.RandomizeStr("core-parity-wf")
	opts.TaskQueue = d.workflowTaskQueue()
	activityTaskQueue = testcore.RandomizeStr("core-parity-act")

	run, err := env.SdkClient().ExecuteWorkflow(d.testContext(), opts, wfaSingleActivityWorkflow,
		wfaActivityParams{Cfg: cfg, ActivityTQ: activityTaskQueue, ActivityID: activityID})
	require.NoError(t, err)

	pending = func(t require.TestingT) *workflowpb.PendingActivityInfo {
		resp, err := env.SdkClient().DescribeWorkflowExecution(d.testContext(), opts.ID, run.GetRunID())
		require.NoError(t, err)
		for _, pa := range resp.GetPendingActivities() {
			if pa.GetActivityId() == activityID {
				return pa
			}
		}
		return nil
	}
	await.Require(d.testContext(), t, func(t *await.T) {
		t.Require().NotNil(pending(t), "the wrapper workflow must schedule its activity")
	}, activityDriverTimeout, activityDriverPollInterval)
	return activityTaskQueue, pending
}

// awaitAbsorbedAttemptTimeout waits for a per-attempt timeout that a pending reset absorbs to land the
// activity in expected. The driver's timer-event wait cannot express this edge: it watches for the
// attempt counter advancing or the activity closing, and an absorbed timeout does neither, because the
// reset rewinds the counter to 1. Driving the timeout outside driveEvent leaves the driver's model
// cursor behind the activity, so nothing may be driven on it afterwards.
func awaitAbsorbedAttemptTimeout(t *testing.T, a parityActivity, expected activityInfo, window time.Duration) {
	await.Require(a.testContext(), t, func(t *await.T) {
		t.Require().Equal(expected, a.activityInfo(t))
	}, window+activityDriverTimerMargin, activityDriverPollInterval)
}

// resetParityRef identifies an activity to an RPC the drivers do not issue. workflowID is empty for a
// standalone activity.
type resetParityRef struct {
	testContext func() context.Context
	workflowID  string
	activityID  string
	runID       string
}

// resetParityUpdatePriority sets the activity's priority, which the trace vocabulary's UpdateOptions
// event cannot carry.
func resetParityUpdatePriority(t require.TestingT, env *testcore.TestEnv, ref resetParityRef, priorityKey int32) {
	_, err := env.FrontendClient().UpdateActivityExecutionOptions(ref.testContext(), &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace:       env.Namespace().String(),
		WorkflowId:      ref.workflowID,
		ActivityId:      ref.activityID,
		RunId:           ref.runID,
		Identity:        env.Tv().ClientIdentity(),
		ActivityOptions: &activitypb.ActivityOptions{Priority: &commonpb.Priority{PriorityKey: priorityKey}},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"priority"}},
	})
	require.NoError(t, err)
}

// operatorTarget names an activity for a raw operator RPC on either surface. workflowID is empty for
// a standalone activity, which is exactly what distinguishes the two on the wire: every operator
// command below is the same RPC either way.
type operatorTarget struct {
	env        *testcore.TestEnv
	workflowID string
	activityID string
	runID      string
}

func operatorPause(ctx context.Context, to operatorTarget, identity, reason string) error {
	_, err := to.env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
		Namespace: to.env.Namespace().String(), WorkflowId: to.workflowID, ActivityId: to.activityID, RunId: to.runID,
		Identity: identity, Reason: reason, RequestId: uuid.NewString(),
	})
	return err
}

func operatorUnpause(ctx context.Context, to operatorTarget, identity, reason string) error {
	_, err := to.env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
		Namespace: to.env.Namespace().String(), WorkflowId: to.workflowID, ActivityId: to.activityID, RunId: to.runID,
		Identity: identity, Reason: reason, RequestId: uuid.NewString(),
	})
	return err
}

// operatorReset and operatorUpdateOptions take a reason for a uniform signature; their requests have
// no reason field, so it is dropped rather than sent.
func operatorReset(ctx context.Context, to operatorTarget, identity, _ string) error {
	_, err := to.env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
		Namespace: to.env.Namespace().String(), WorkflowId: to.workflowID, ActivityId: to.activityID, RunId: to.runID,
		Identity: identity, RequestId: uuid.NewString(),
	})
	return err
}

func operatorUpdateOptions(ctx context.Context, to operatorTarget, identity, _ string) error {
	_, err := to.env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace: to.env.Namespace().String(), WorkflowId: to.workflowID, ActivityId: to.activityID, RunId: to.runID,
		Identity: identity, RequestId: uuid.NewString(),
		ActivityOptions: &activitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(time.Hour)},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
	})
	return err
}

// pauseParityUnpauseJitter is the unpause jitter P13 asks for: shorter than activityLongDuration, so a
// dispatch scheduled from the unpause time falls well before the backoff deadline being kept.
const pauseParityUnpauseJitter = time.Hour

// pauseParityUnpause is an UnpauseActivityExecution carrying a request id and a jitter, neither of
// which the trace vocabulary can send. It bypasses the driver's model check, so nothing may be driven
// on the handle after it.
type pauseParityUnpause func(requestID string, jitter time.Duration) error

func pauseParityUnpauseWFA(a *wfaHandle) pauseParityUnpause {
	return func(requestID string, jitter time.Duration) error {
		_, err := a.d.env.FrontendClient().UnpauseActivityExecution(a.testContext(), &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: a.d.env.Namespace().String(), WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID,
			Identity: a.d.env.Tv().ClientIdentity(), RequestId: requestID, Jitter: pauseParityDuration(jitter),
		})
		return err
	}
}

func pauseParityUnpauseSAA(a *saaHandle) pauseParityUnpause {
	return func(requestID string, jitter time.Duration) error {
		_, err := a.d.env.FrontendClient().UnpauseActivityExecution(a.testContext(), &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: a.d.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID,
			Identity: a.d.env.Tv().ClientIdentity(), RequestId: requestID, Jitter: pauseParityDuration(jitter),
		})
		return err
	}
}

func pauseParityDuration(d time.Duration) *durationpb.Duration {
	if d == 0 {
		return nil
	}
	return durationpb.New(d)
}

// updateParityFailureType is the application-failure type the drivers report; see respondFailedFailure.
const updateParityFailureType = "TestFailure"

// updateParityNonRetryableTerminal is what a failure whose type is listed in non_retryable_error_types
// must produce, whatever retry budget is left: the activity closes rather than retrying.
var updateParityNonRetryableTerminal = activityTerminalProjection{
	Status:      enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
	FailureType: updateParityFailureType,
	RetryState:  enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
}

// updateParityWorkerRetryDelay is the next_retry_delay a worker reports in U04, and
// updateParityUpdatedRetryInterval the interval U04's retry-policy update installs. Both differ from the
// configured retry interval and from each other, so a preserved dispatch time cannot be mistaken for a
// recomputed one.
const (
	updateParityWorkerRetryDelay     = 12 * time.Hour
	updateParityUpdatedRetryInterval = 6 * time.Hour
)

// updateParityView is a driven activity plus the per-surface reads these tests need beyond the shared
// activityInfo projection: the ids UpdateActivityExecutionOptions is addressed by, the retry policy the
// activity reports, whether it has stopped running, and its pending retry as an interval and an exact
// dispatch time.
type updateParityView struct {
	parityActivity
	ids     updateParityIDs
	policy  func(require.TestingT) *commonpb.RetryPolicy
	closed  func(require.TestingT) bool
	pending func(require.TestingT) (time.Duration, *timestamppb.Timestamp)
}

// updateParityDrive is parityDrive for a test that also needs the per-surface reads in updateParityView.
func updateParityDrive(
	t *testing.T,
	env *testcore.TestEnv,
	cfg activityConfig,
	trace []model.Event,
	check func(*testing.T, updateParityView),
) {
	t.Run("WorkflowActivity", func(t *testing.T) {
		a := newWFADriver(t, env, cfg).driveTrace(t, trace)
		check(t, updateParityView{
			parityActivity: a,
			ids:            updateParityIDs{WorkflowID: a.workflowID, ActivityID: a.activityID, RunID: a.runID},
			policy: func(t require.TestingT) *commonpb.RetryPolicy {
				return a.pendingActivityInfo(t).GetActivityOptions().GetRetryPolicy()
			},
			closed: func(t require.TestingT) bool { return a.pendingActivityInfo(t) == nil },
			pending: func(t require.TestingT) (time.Duration, *timestamppb.Timestamp) {
				pendingActivity := a.pendingActivityInfo(t)
				require.NotNil(t, pendingActivity, "the activity is no longer pending")
				return pendingActivity.GetCurrentRetryInterval().AsDuration().Round(time.Second),
					pendingActivity.GetNextAttemptScheduleTime()
			},
		})
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		a := newSAADriver(t, env, cfg).driveTrace(t, trace)
		check(t, updateParityView{
			parityActivity: a,
			ids:            updateParityIDs{ActivityID: a.activityID, RunID: a.runID},
			policy: func(t require.TestingT) *commonpb.RetryPolicy {
				return a.describe(t).GetInfo().GetRetryPolicy()
			},
			closed: func(t require.TestingT) bool {
				return a.describe(t).GetInfo().GetStatus() != enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING
			},
			pending: func(t require.TestingT) (time.Duration, *timestamppb.Timestamp) {
				info := a.describe(t).GetInfo()
				return info.GetCurrentRetryInterval().AsDuration().Round(time.Second),
					info.GetNextAttemptScheduleTime()
			},
		})
	})
}

// updateParityIDs identifies an activity to UpdateActivityExecutionOptions on either surface;
// WorkflowID is empty for a standalone activity.
type updateParityIDs struct {
	WorkflowID string
	ActivityID string
	RunID      string
}

// updateParitySend issues an update the drivers' UpdateOptions event cannot express, and returns the
// server's answer.
func updateParitySend(
	ctx context.Context,
	env *testcore.TestEnv,
	ids updateParityIDs,
	options *activitypb.ActivityOptions,
	paths ...string,
) error {
	_, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace:       env.Namespace().String(),
		WorkflowId:      ids.WorkflowID,
		ActivityId:      ids.ActivityID,
		RunId:           ids.RunID,
		Identity:        env.Tv().ClientIdentity(),
		ActivityOptions: options,
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: paths},
	})
	return err
}
