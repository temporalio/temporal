package matching

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/number"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type ScaleManagerSuite struct {
	suite.Suite
	protorequire.ProtoAssertions
	controller *gomock.Controller
	scaler     *MockPartitionScaler
	scaleDB    *MockscaleDB
	userData   *MockuserDataManager
	matching   *matchingservicemock.MockMatchingServiceClient
	timeSource *clock.EventTimeSource

	sm       *scaleManager
	settings dynamicconfig.PartitionScaleManagerSettings

	// metricsHandler is wired into the manager. It defaults to Noop (gauge
	// emission is enabled whenever the handler isn't the Noop handler, so gauges
	// are off by default). A test that asserts on emitted gauges sets
	// s.metricsHandler = s.capture before startManager.
	metricsHandler metrics.Handler
	capture        *metricstest.CaptureHandler

	// newTarget counts "new target" logs, which are also used for test synchronization
	newTarget *testlogger.Expectation
}

func TestScaleManagerSuite(t *testing.T) {
	suite.Run(t, new(ScaleManagerSuite))
}

func (s *ScaleManagerSuite) SetupTest() {
	s.ProtoAssertions = protorequire.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.scaler = NewMockPartitionScaler(s.controller)
	s.scaleDB = NewMockscaleDB(s.controller)
	s.userData = NewMockuserDataManager(s.controller)
	s.matching = matchingservicemock.NewMockMatchingServiceClient(s.controller)
	s.timeSource = clock.NewEventTimeSource()
	s.capture = metricstest.NewCaptureHandler()
	s.metricsHandler = metrics.NoopMetricsHandler

	// scaler.Stop is called from sm.Stop in TearDownTest, but only if the test
	// actually started the manager.
	s.scaler.EXPECT().Stop().AnyTimes()

	s.settings = dynamicconfig.PartitionScaleManagerSettings{
		MaxRate:            10,  // 100ms cooldown
		ShrinkRatio:        1.0, // no shrink limit by default
		ShrinkDelta:        100, // no shrink limit by default
		BatchSize:          1,
		BackgroundInterval: time.Hour, // disabled by default
		DrainBufferTime:    time.Hour, // disabled by default
	}
}

func (s *ScaleManagerSuite) TearDownTest() {
	if s.sm != nil {
		s.sm.Stop()
		// wait for the worker to actually exit, otherwise mock callbacks may
		// race with controller.Finish
		<-s.sm.background.Done()
		s.sm = nil
	}
}

// startManager builds and starts the scaleManager. Call this after configuring
// EXPECTs and tweaking s.settings. Pass initial=nil to start with no prior state.
func (s *ScaleManagerSuite) startManager(writePartitions int, initial *persistencespb.PartitionScaleState) {
	s.startManagerWithLogger(testlogger.NewTestLogger(s.T(), testlogger.FailOnExpectedErrorOnly), writePartitions, initial)
}

func (s *ScaleManagerSuite) startManagerWithLogger(
	logger log.Logger,
	writePartitions int,
	initial *persistencespb.PartitionScaleState,
) {
	if tl, ok := logger.(*testlogger.TestLogger); ok {
		s.newTarget = tl.Expect(testlogger.Info, "new target")
	}
	s.sm = newScaleManager(
		context.Background(),
		tqid.MustNormalPartitionFromRpcName("tq", "ns-id", enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		logger,
		s.metricsHandler,
		s.userData,
		s.matching,
		s.scaler,
		s.timeSource,
		dynamicconfig.GetTypedPropertyFn(s.settings),
		dynamicconfig.GetIntPropertyFn(writePartitions),
		func() bool { return s.metricsHandler != metrics.NoopMetricsHandler },
	)
	s.sm.Start(initial, s.scaleDB)
}

// fireBackgroundTimer waits for the worker to register its periodic timer and
// then advances the fake clock past the timer's deadline.
func (s *ScaleManagerSuite) fireBackgroundTimer() {
	await.RequireTruef(s.T(), func() bool { return s.timeSource.NumTimers() >= 1 },
		time.Second, time.Millisecond,
		"worker never registered a periodic timer")
	// BackgroundInterval has up to 5% jitter; advance comfortably past it.
	s.timeSource.Advance(s.settings.BackgroundInterval*2 + time.Millisecond)
}

// awaitDecisionApplied blocks until the worker has fully applied n decisions.
// This uses the "new target" log that's emitted at the end of callScaler, so we know that
// an update has been fully applied.
func (s *ScaleManagerSuite) awaitDecisionApplied(n int64) {
	s.T().Helper()
	waitLogMatches(s, s.newTarget, n, "decision not fully applied before clock advance")
}

// waitRecv blocks until ch yields a value, failing the test on timeout.
func waitRecv[T any](s *ScaleManagerSuite, ch <-chan T, msg string) T {
	s.T().Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		s.FailNow("timed out waiting", msg)
		var zero T
		return zero
	}
}

// assertNoRecv asserts that ch yields no value within d.
func assertNoRecv[T any](s *ScaleManagerSuite, ch <-chan T, d time.Duration, msg string) {
	s.T().Helper()
	select {
	case <-ch:
		s.FailNow("unexpected event", msg)
	case <-time.After(d):
	}
}

func waitLogMatches(s *ScaleManagerSuite, e *testlogger.Expectation, count int64, msg string) {
	s.T().Helper()
	await.RequireTruef(s.T(), func() bool { return e.MatchCount() == count },
		time.Second, time.Millisecond, "%s", msg)
}

// assertNoNewLogs asserts that no new matching log is emitted during d, i.e. the
// expectation's match count stays at whatever it is when this is called. Unlike a
// fixed expected count, this is robust to how many matches happened earlier in the test.
func assertNoNewLogs(s *ScaleManagerSuite, e *testlogger.Expectation, d time.Duration, msg string) {
	s.T().Helper()
	before := e.MatchCount()
	s.Require().Never(func() bool { return e.MatchCount() != before },
		d, time.Millisecond, msg)
}

// --- tests ---

// TestAddedTasksWakesScalerOnFullBatch verifies that the scaler is only called
// once cumulative batch reaches numTasks*BatchSize.
func (s *ScaleManagerSuite) TestAddedTasksWakesScalerOnFullBatch() {
	s.settings.BatchSize = 5

	inputs := make(chan PartitionScalerInput, 1)
	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Do(func(in PartitionScalerInput) { inputs <- in }).
		Return(PartitionScalerDecision{NoChange: true}).AnyTimes()

	s.startManager(4, nil)

	for range 4 {
		s.sm.AddedTasks(1)
	}
	assertNoRecv(s, inputs, 30*time.Millisecond, "scaler called below threshold")

	s.sm.AddedTasks(1) // cumulative 5 hits threshold
	in := waitRecv(s, inputs, "scaler never called")
	s.Equal(5, in.NumTasks, "all 5 tasks should accumulate before firing")
}

// TestPeriodicTimerCallsScaler verifies that the scaler is called periodically
// even with no AddedTasks activity (so we can scale down).
func (s *ScaleManagerSuite) TestPeriodicTimerCallsScaler() {
	s.settings.BackgroundInterval = 50 * time.Millisecond

	inputs := make(chan PartitionScalerInput, 4)
	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Do(func(in PartitionScalerInput) {
			select {
			case inputs <- in:
			default: // ignore the rest
			}
		}).
		Return(PartitionScalerDecision{NoChange: true}).
		MinTimes(1)

	s.startManager(4, nil)
	s.fireBackgroundTimer()

	in := waitRecv(s, inputs, "scaler never called")
	s.Zero(in.NumTasks)
}

// TestDecisionPersistsAndUpdatesEphemeralData verifies the full happy path.
// When prevTarget was 0, BacklogState should also cover all partitions from
// dynamic config (not just the new target).
func (s *ScaleManagerSuite) TestDecisionPersistsAndUpdatesEphemeralData() {
	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Return(PartitionScalerDecision{NewTarget: 2})

	dbWrites := make(chan *persistencespb.PartitionScaleState, 1)
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		Do(func(state *persistencespb.PartitionScaleState, _ bool) {
			dbWrites <- common.CloneProto(state)
		}).
		Return(nil)

	scaleInfos := make(chan *taskqueuespb.PartitionScaleInfo, 1)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).
		Do(func(info *taskqueuespb.PartitionScaleInfo) { scaleInfos <- info })

	s.startManager(4, nil) // 4 write partitions in dynamic config

	s.sm.AddedTasks(1)
	state := waitRecv(s, dbWrites, "no db write")
	s.Equal(int32(2), state.Target)
	s.Equal(int32(2), state.MaxTarget)
	s.Equal(s.timeSource.Now().UnixNano(), state.TargetVersion)
	s.Equal(int32(4), bitSet(state.BacklogState).len(),
		"BacklogState should cover all dynamic-config write partitions on initial scale-up")

	info := waitRecv(s, scaleInfos, "no ephemeral data update")
	s.Equal(int32(4), info.Read)
	s.Equal(int32(2), info.Write)
}

// TestEmitsGaugeMetrics verifies that when gauge emission is enabled, a scale
// decision records the read, write, and target gauges with the expected values.
func (s *ScaleManagerSuite) TestEmitsGaugeMetrics() {
	s.metricsHandler = s.capture
	capture := s.capture.StartCapture()
	defer s.capture.StopCapture(capture)

	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Return(PartitionScalerDecision{NewTarget: 2})

	dbWrites := make(chan *persistencespb.PartitionScaleState, 1)
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		Do(func(state *persistencespb.PartitionScaleState, _ bool) {
			dbWrites <- common.CloneProto(state)
		}).
		Return(nil)

	s.userData.EXPECT().SetPartitionScale(gomock.Any()).AnyTimes()

	s.startManager(4, nil) // 4 write partitions in dynamic config

	s.sm.AddedTasks(1)
	waitRecv(s, dbWrites, "no db write")

	// setState records the gauges after the ephemeral push, so poll the snapshot
	// until the decision's values land: Read=4 (dynamic config), Write=Target=2.
	lastValue := func(name string) (float64, bool) {
		recs := capture.Snapshot()[name]
		if len(recs) == 0 {
			return 0, false
		}
		v, ok := recs[len(recs)-1].Value.(float64)
		return v, ok
	}
	await.RequireTruef(s.T(), func() bool {
		v, ok := lastValue(metrics.PartitionScaleTarget.Name())
		return ok && v == 2
	}, time.Second, time.Millisecond, "target gauge never reached 2")

	read, _ := lastValue(metrics.PartitionScaleRead.Name())
	write, _ := lastValue(metrics.PartitionScaleWrite.Name())
	target, _ := lastValue(metrics.PartitionScaleTarget.Name())
	s.InDelta(float64(4), read, 0.001, "read gauge")
	s.InDelta(float64(2), write, 0.001, "write gauge")
	s.InDelta(float64(2), target, 0.001, "target gauge")
}

// TestNonPositiveShadowLogIntervalDisabled verifies that ShadowModeLogInterval
// <= 0 uses the normal apply path.
func (s *ScaleManagerSuite) TestNonPositiveShadowLogIntervalDisabled() {
	s.settings.ShadowModeLogInterval = -time.Second

	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Return(PartitionScalerDecision{NewTarget: 2})

	dbWrites := make(chan *persistencespb.PartitionScaleState, 1)
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		Do(func(state *persistencespb.PartitionScaleState, _ bool) {
			dbWrites <- common.CloneProto(state)
		}).
		Return(nil)

	scaleInfos := make(chan *taskqueuespb.PartitionScaleInfo, 1)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).
		Do(func(info *taskqueuespb.PartitionScaleInfo) { scaleInfos <- info })

	s.startManager(4, nil)

	s.sm.AddedTasks(1)
	state := waitRecv(s, dbWrites, "no db write")
	s.Equal(int32(2), state.Target)
	info := waitRecv(s, scaleInfos, "no ephemeral data update")
	s.Equal(int32(4), info.Read)
	s.Equal(int32(2), info.Write)
}

// TestShadowDecisionDoesNotPersistOrPush verifies that shadow mode observes the
// scaler decision without applying it. Entering shadow mode does release a prior
// managed target to baseline (one write, Target=0), but the scaler's hypothetical
// decision (2) is never persisted.
func (s *ScaleManagerSuite) TestShadowDecisionDoesNotPersistOrPush() {
	s.settings.ShadowModeLogInterval = time.Minute

	inputs := make(chan PartitionScalerInput, 1)
	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Do(func(in PartitionScalerInput) { inputs <- in }).
		Return(PartitionScalerDecision{NewTarget: 2})

	// Exactly one DB write: the one-time release to baseline. The scaler decision
	// (2) must never be persisted.
	dbWrites := make(chan *persistencespb.PartitionScaleState, 2)
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		Do(func(state *persistencespb.PartitionScaleState, _ bool) {
			dbWrites <- common.CloneProto(state)
		}).
		Return(nil).Times(1)

	// Start publishes the initial state; the release publishes once more.
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).Times(2)

	s.startManager(4, &persistencespb.PartitionScaleState{Target: 1})

	s.sm.AddedTasks(1)
	in := waitRecv(s, inputs, "shadow scaler call missing")
	// release happens before the scaler call, so it sees the baseline.
	s.Equal(0, in.CurrentTarget)

	w := waitRecv(s, dbWrites, "release write missing")
	s.Equal(int32(0), w.Target)
	s.Nil(w.PrivateScalerState)
	s.Equal(int32(0), s.sm.scaleState.GetTarget())

	// the scaler decision (2) must not be persisted
	assertNoRecv(s, dbWrites, 30*time.Millisecond, "scaler decision must not be persisted")
}

// TestShadowModeColdStartsScalerFromBaseline verifies that after releasing a
// prior managed target on entry, shadow mode feeds the scaler the baseline
// (Target 0, nil private state) on every call and never feeds hypothetical
// decisions back into the state.
func (s *ScaleManagerSuite) TestShadowModeColdStartsScalerFromBaseline() {
	s.settings.ShadowModeLogInterval = time.Minute
	realPriv := protoutils.MarshalAny(s.T(), wrapperspb.String("real-state"))
	priv1 := protoutils.MarshalAny(s.T(), wrapperspb.String("shadow-decision-1"))
	priv2 := protoutils.MarshalAny(s.T(), wrapperspb.String("shadow-decision-2"))

	inputs := make(chan PartitionScalerInput, 2)
	gomock.InOrder(
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 2, PrivateState: priv1}),
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 3, PrivateState: priv2}),
	)

	// One release write (Target=0) on entry; shadow decisions never write.
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	// Start push + one release push.
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).Times(2)

	s.startManager(4, &persistencespb.PartitionScaleState{Target: 1, PrivateScalerState: realPriv})

	s.sm.AddedTasks(1)
	in1 := waitRecv(s, inputs, "first shadow call missing")
	s.Equal(0, in1.CurrentTarget)
	s.Nil(in1.PrivateState)

	s.awaitDecisionApplied(1)                    // barrier: nextDecision set before we advance
	s.timeSource.Advance(110 * time.Millisecond) // past the 100ms cooldown
	s.sm.AddedTasks(1)
	in2 := waitRecv(s, inputs, "second shadow call missing")
	s.Equal(0, in2.CurrentTarget)
	s.Nil(in2.PrivateState)

	// scaler decisions are never applied back into the state
	s.Equal(int32(0), s.sm.scaleState.GetTarget())
	s.Nil(s.sm.scaleState.GetPrivateScalerState())
}

func (s *ScaleManagerSuite) TestShadowModeDoesNotLogNoChange() {
	s.settings.ShadowModeLogInterval = time.Minute

	logger := testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	inputs := make(chan PartitionScalerInput, 1)
	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Do(func(in PartitionScalerInput) { inputs <- in }).
		Return(PartitionScalerDecision{NoChange: true})

	s.startManagerWithLogger(logger, 4, nil)

	s.sm.AddedTasks(1)
	waitRecv(s, inputs, "shadow scaler call missing")
	assertNoNewLogs(s, s.newTarget, 30*time.Millisecond, "NoChange decision should not log")
}

// TestShadowLoggingCadence verifies that shadow decisions are logged no more
// frequently than the configured cadence and unchanged decisions are not logged.
func (s *ScaleManagerSuite) TestShadowLoggingCadence() {
	s.settings.ShadowModeLogInterval = time.Minute

	logger := testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	inputs := make(chan PartitionScalerInput, 4)
	gomock.InOrder(
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 2}),
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 2}),
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 2}),
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 3}),
	)

	s.startManagerWithLogger(logger, 4, nil)

	s.sm.AddedTasks(1)
	waitRecv(s, inputs, "first shadow call missing")
	waitLogMatches(s, s.newTarget, 1, "first shadow log missing")

	s.sm.AddedTasks(1)
	assertNoRecv(s, inputs, 30*time.Millisecond, "shadow scaler called inside cooldown")

	s.timeSource.Advance(110 * time.Millisecond) // past the 100ms cooldown
	s.sm.AddedTasks(1)
	waitRecv(s, inputs, "second shadow call missing")
	assertNoNewLogs(s, s.newTarget, 30*time.Millisecond, "shadow log repeated before cadence")

	s.timeSource.Advance(time.Minute)
	s.sm.AddedTasks(1)
	waitRecv(s, inputs, "third shadow call missing")
	assertNoNewLogs(s, s.newTarget, 30*time.Millisecond, "unchanged shadow decision logged after cadence")

	s.sm.AddedTasks(1)
	waitRecv(s, inputs, "fourth shadow call missing")
	waitLogMatches(s, s.newTarget, 2, "second shadow log missing")
}

func (s *ScaleManagerSuite) TestShadowModeDoesNotLogDisabledScaler() {
	s.settings.ShadowModeLogInterval = time.Minute

	logger := testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	inputs := make(chan PartitionScalerInput, 1)
	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Do(func(in PartitionScalerInput) { inputs <- in }).
		Return(PartitionScalerDecision{NewTarget: 0})

	// entering shadow with a managed target releases it to baseline (one write/push).
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).Times(2)

	s.startManagerWithLogger(logger, 4, &persistencespb.PartitionScaleState{Target: 2})
	s.sm.AddedTasks(1)
	waitRecv(s, inputs, "shadow scaler call missing")
	assertNoNewLogs(s, s.newTarget, 30*time.Millisecond, "disabled scaler should not log")
}

// TestShadowModeSkipsDrain verifies that shadow mode does not change real read
// partitions by clearing backlog bits.
func (s *ScaleManagerSuite) TestShadowModeSkipsDrain() {
	s.settings.ShadowModeLogInterval = time.Minute
	s.settings.BackgroundInterval = 50 * time.Millisecond
	s.settings.DrainBufferTime = 0

	inputs := make(chan PartitionScalerInput, 1)
	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Do(func(in PartitionScalerInput) { inputs <- in }).
		Return(PartitionScalerDecision{NoChange: true})

	// entering shadow releases the managed target to baseline (one write/push);
	// the drain path must not write or clear backlog bits.
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).Times(2)

	initial := &persistencespb.PartitionScaleState{
		Target:        2,
		MaxTarget:     4,
		TargetVersion: 0,
		BacklogState:  bitSet(nil).set(0).set(1).set(2).set(3),
	}
	drainedResp := &matchingservice.DescribeTaskQueuePartitionResponse{
		ScaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 4, Write: 2, Version: 0},
		VersionsInfoInternal: map[string]*taskqueuespb.TaskQueueVersionInfoInternal{
			"v1": {
				PhysicalTaskQueueInfo: &taskqueuespb.PhysicalTaskQueueInfo{
					InternalTaskQueueStatus: []*taskqueuespb.InternalTaskQueueStatus{
						{BacklogDrained: true},
					},
				},
			},
		},
	}
	describeCalls := make(chan struct{}, 4)
	s.matching.EXPECT().DescribeTaskQueuePartition(gomock.Any(), gomock.Any()).
		Do(func(context.Context, *matchingservice.DescribeTaskQueuePartitionRequest, ...grpc.CallOption) {
			describeCalls <- struct{}{}
		}).
		Return(drainedResp, nil).
		Times(4)

	s.startManager(4, initial)
	s.fireBackgroundTimer()
	waitRecv(s, inputs, "timer did not call scaler")
	for range 4 {
		waitRecv(s, describeCalls, "shadow drain did not describe read partition")
	}

	s.Equal(int32(4), bitSet(s.sm.scaleState.BacklogState).len())
}

// TestShadowModeReleasesManagedTargetToBaseline verifies that enabling shadow
// mode on top of an applied managed target zeroes the target and clears private
// state (one write), dropping the write side to baseline (Write=0) while
// preserving read partitions (BacklogState unchanged).
func (s *ScaleManagerSuite) TestShadowModeReleasesManagedTargetToBaseline() {
	s.settings.ShadowModeLogInterval = time.Minute
	priv := protoutils.MarshalAny(s.T(), wrapperspb.String("managed-state"))

	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Return(PartitionScalerDecision{NoChange: true}).AnyTimes()

	dbWrites := make(chan *persistencespb.PartitionScaleState, 1)
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		Do(func(state *persistencespb.PartitionScaleState, _ bool) {
			dbWrites <- common.CloneProto(state)
		}).
		Return(nil).Times(1)

	scaleInfos := make(chan *taskqueuespb.PartitionScaleInfo, 2)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).
		Do(func(info *taskqueuespb.PartitionScaleInfo) { scaleInfos <- info }).Times(2)

	initial := &persistencespb.PartitionScaleState{
		Target:             10,
		MaxTarget:          10,
		BacklogState:       bitSet(nil).set(0).set(1).set(2).set(3).set(4).set(5).set(6).set(7).set(8).set(9),
		PrivateScalerState: priv,
	}
	s.startManager(4, initial)

	s.sm.AddedTasks(1)

	w := waitRecv(s, dbWrites, "release write missing")
	s.Equal(int32(0), w.Target)
	s.Nil(w.PrivateScalerState)
	s.Equal(int32(10), bitSet(w.BacklogState).len(), "read partitions must be preserved")

	// Start push (Read=10/Write=10), then the release push (Read=10/Write=0).
	waitRecv(s, scaleInfos, "start push missing")
	released := waitRecv(s, scaleInfos, "release push missing")
	s.Equal(int32(10), released.Read)
	s.Equal(int32(0), released.Write)
}

// TestShadowModeLogsOscillationFromBaseline verifies that once a managed target
// is released to baseline (0), shadow decisions oscillating among positive values
// are all logged. Releasing removes the stale non-zero baseline that would
// otherwise short-circuit a decision returning to the prior real target.
func (s *ScaleManagerSuite) TestShadowModeLogsOscillationFromBaseline() {
	s.settings.ShadowModeLogInterval = time.Minute

	logger := testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	inputs := make(chan PartitionScalerInput, 3)
	gomock.InOrder(
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 3}),
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 2}),
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 3}),
	)

	// one release write/push on entry (managed Target=2 -> 0)
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).Times(2)

	s.startManagerWithLogger(logger, 4, &persistencespb.PartitionScaleState{Target: 2})

	s.sm.AddedTasks(1)
	waitRecv(s, inputs, "first shadow call missing")
	waitLogMatches(s, s.newTarget, 1, "shadow target 3 not logged")

	s.timeSource.Advance(time.Minute) // past cooldown and cadence
	s.sm.AddedTasks(1)
	waitRecv(s, inputs, "second shadow call missing")
	waitLogMatches(s, s.newTarget, 2, "shadow target 2 not logged")

	s.timeSource.Advance(time.Minute)
	s.sm.AddedTasks(1)
	waitRecv(s, inputs, "third shadow call missing")
	waitLogMatches(s, s.newTarget, 3, "shadow target 3 after oscillation not logged")
}

// TestNoChangeDecisionSkipsWrite covers two skip paths: explicit NoChange, and
// a decision whose NewTarget equals the current target. Neither should cause
// a DB write or an ephemeral data update.
func (s *ScaleManagerSuite) TestNoChangeDecisionSkipsWrite() {
	inputs := make(chan PartitionScalerInput, 2)
	gomock.InOrder(
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NoChange: true}),
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 3}), // same as current
	)

	var dbWrites, scalePushes atomic.Int32
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		DoAndReturn(func(*persistencespb.PartitionScaleState, bool) error {
			dbWrites.Add(1)
			return nil
		}).AnyTimes()
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).
		Do(func(*taskqueuespb.PartitionScaleInfo) { scalePushes.Add(1) }).AnyTimes()

	s.startManager(4, &persistencespb.PartitionScaleState{Target: 3})

	// Start pushes ephemeral data once (Read/Write change 0 → 3).
	await.RequireTrue(s.T(), func() bool { return scalePushes.Load() >= 1 },
		time.Second, time.Millisecond)
	baseline := scalePushes.Load()

	s.sm.AddedTasks(1)
	waitRecv(s, inputs, "first decision never delivered")
	s.sm.AddedTasks(1)
	waitRecv(s, inputs, "second decision never delivered")

	s.Require().Never(func() bool {
		return dbWrites.Load() > 0 || scalePushes.Load() > baseline
	}, 30*time.Millisecond, time.Millisecond, "unexpected DB write or ephemeral push")
}

// TestCooldown verifies that after a decision the scaler is not called again
// until 1/MaxRate of fake time has elapsed, and that batched tasks accumulate
// across the cooldown window rather than being dropped.
func (s *ScaleManagerSuite) TestCooldown() {
	// MaxRate=10 → 100ms cooldown.
	inputs := make(chan PartitionScalerInput, 2)
	gomock.InOrder(
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 2}),
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 4}),
	)

	dbWrites := make(chan *persistencespb.PartitionScaleState, 2)
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		Do(func(state *persistencespb.PartitionScaleState, _ bool) {
			dbWrites <- common.CloneProto(state)
		}).
		Return(nil).Times(2)

	s.userData.EXPECT().SetPartitionScale(gomock.Any()).AnyTimes()

	s.startManager(4, nil)

	// First decision lands immediately (lastDecision is zero, no cooldown).
	s.sm.AddedTasks(1)
	in1 := waitRecv(s, inputs, "first decision never delivered")
	s.Equal(1, in1.NumTasks)
	waitRecv(s, dbWrites, "first db write missing")

	// Within cooldown: another wakeup carrying 7 tasks. Scaler must NOT be
	// called, and tasks must accumulate (batch is only swapped when scaler runs).
	s.sm.AddedTasks(7)
	assertNoRecv(s, inputs, 30*time.Millisecond, "scaler called inside cooldown")

	// Advance fake time past cooldown. Trigger another wakeup; the scaler
	// should now be called with the accumulated 8 tasks.
	s.awaitDecisionApplied(1) // barrier: nextDecision set before we advance
	s.timeSource.Advance(110 * time.Millisecond)
	s.sm.AddedTasks(1)
	in2 := waitRecv(s, inputs, "second decision never delivered")
	s.Equal(8, in2.NumTasks)
	w2 := waitRecv(s, dbWrites, "second db write missing")
	s.Equal(int32(4), w2.Target)
}

// TestPrivateStatePropagation verifies that PrivateState produced by one
// decision is fed back to the scaler on the next call (and persisted).
func (s *ScaleManagerSuite) TestPrivateStatePropagation() {
	priv1 := protoutils.MarshalAny(s.T(), wrapperspb.String("after-decision-1"))
	priv2 := protoutils.MarshalAny(s.T(), wrapperspb.String("after-decision-2"))

	inputs := make(chan PartitionScalerInput, 2)
	gomock.InOrder(
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 2, PrivateState: priv1}),
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 4, PrivateState: priv2}),
	)

	dbWrites := make(chan *persistencespb.PartitionScaleState, 2)
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		Do(func(state *persistencespb.PartitionScaleState, _ bool) {
			dbWrites <- common.CloneProto(state)
		}).
		Return(nil).Times(2)

	s.userData.EXPECT().SetPartitionScale(gomock.Any()).AnyTimes()

	s.startManager(4, nil)

	s.sm.AddedTasks(1)
	in1 := waitRecv(s, inputs, "first call missing")
	s.Nil(in1.PrivateState, "first call should have no private state")
	waitRecv(s, dbWrites, "first db write missing")

	s.awaitDecisionApplied(1)                    // barrier: nextDecision set before we advance
	s.timeSource.Advance(110 * time.Millisecond) // past 100ms cooldown
	s.sm.AddedTasks(1)
	in2 := waitRecv(s, inputs, "second call missing")
	s.ProtoEqual(priv1, in2.PrivateState)
	w2 := waitRecv(s, dbWrites, "second db write missing")
	s.ProtoEqual(priv2, w2.PrivateScalerState)
}

// TestDrainClearsBacklogBits verifies that partitions which report as fully
// drained have their BacklogState bits cleared on the next periodic drain check.
func (s *ScaleManagerSuite) TestDrainClearsBacklogBits() {
	s.settings.BackgroundInterval = 50 * time.Millisecond
	s.settings.DrainBufferTime = 0

	initial := &persistencespb.PartitionScaleState{
		Target:        2,
		MaxTarget:     4,
		TargetVersion: 0, // fake clock starts at Unix zero
		BacklogState:  bitSet(nil).set(0).set(1).set(2).set(3),
	}
	// scaleStateToInfo for the initial state has Read=4, Write=2.
	drainedResp := &matchingservice.DescribeTaskQueuePartitionResponse{
		ScaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 4, Write: 2, Version: 0},
		VersionsInfoInternal: map[string]*taskqueuespb.TaskQueueVersionInfoInternal{
			"v1": {
				PhysicalTaskQueueInfo: &taskqueuespb.PhysicalTaskQueueInfo{
					InternalTaskQueueStatus: []*taskqueuespb.InternalTaskQueueStatus{
						{BacklogDrained: true},
					},
				},
			},
		},
	}
	s.matching.EXPECT().DescribeTaskQueuePartition(gomock.Any(), gomock.Any()).
		Return(drainedResp, nil).AnyTimes()

	// Timer-driven path also calls the scaler (with NumTasks=0). It returns
	// NoChange so no decision-driven DB write happens — only the drain write.
	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Return(PartitionScalerDecision{NoChange: true}).AnyTimes()

	dbWrites := make(chan *persistencespb.PartitionScaleState, 1)
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		Do(func(state *persistencespb.PartitionScaleState, _ bool) {
			dbWrites <- common.CloneProto(state)
		}).
		Return(nil)

	// One ephemeral push from Start (0 → Read=4/Write=2), one from the drain (Read drops to 2).
	scaleInfos := make(chan *taskqueuespb.PartitionScaleInfo, 2)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).
		Do(func(info *taskqueuespb.PartitionScaleInfo) { scaleInfos <- info }).Times(2)

	s.startManager(4, initial)

	// Start: Read=4 (backlog 0..3), Write=2 (target; shrink limit off by default).
	i0 := waitRecv(s, scaleInfos, "no start push")
	s.Equal(int32(4), i0.Read, "start read")
	s.Equal(int32(2), i0.Write, "start write")

	s.fireBackgroundTimer()

	state := waitRecv(s, dbWrites, "drain never wrote")
	s.Equal(int32(2), state.Target, "target should not change during drain")
	// Drained ids 2 and 3 should be cleared; ids 0 and 1 stay set (id < target).
	s.Equal(int32(2), bitSet(state.BacklogState).len())
	s.True(bitSet(state.BacklogState).get(0))
	s.True(bitSet(state.BacklogState).get(1))
	s.False(bitSet(state.BacklogState).get(2))
	s.False(bitSet(state.BacklogState).get(3))

	// Drain dropped Read to 2; Write stays at target 2.
	i1 := waitRecv(s, scaleInfos, "no drain push")
	s.Equal(int32(2), i1.Read, "drain read")
	s.Equal(int32(2), i1.Write, "drain write")
}

// TestDrainShrinkLimitedReadWrite is an end-to-end drain runthrough that checks
// the Read/Write counts pushed to ephemeral data as partitions drain, exercising
// the write-shrink limit across multiple rounds. With ShrinkDelta=2 and target=1:
// 10 partitions backlogged -> Read=10/Write=8; drain 7,8,9 -> Read=7/Write=5;
// drain the rest -> Read=1/Write=1 (partition 0 stays because id < target).
func (s *ScaleManagerSuite) TestDrainShrinkLimitedReadWrite() {
	s.settings.BackgroundInterval = 50 * time.Millisecond
	s.settings.DrainBufferTime = 0
	s.settings.ShrinkDelta = 2 // ShrinkRatio stays 1.0, so delta dominates

	initial := &persistencespb.PartitionScaleState{
		Target:        1,
		MaxTarget:     10,
		TargetVersion: 0, // fake clock starts at Unix zero; target never changes here
		BacklogState: bitSet(nil).
			set(0).set(1).set(2).set(3).set(4).set(5).set(6).set(7).set(8).set(9),
	}

	// drained holds the partition ids that currently report BacklogDrained. The
	// test advances it between drain rounds.
	var drained atomic.Pointer[map[int32]bool]
	setDrained := func(ids ...int32) {
		m := make(map[int32]bool, len(ids))
		for _, id := range ids {
			m[id] = true
		}
		drained.Store(&m)
	}
	setDrained() // nothing drained yet

	s.matching.EXPECT().DescribeTaskQueuePartition(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			req *matchingservice.DescribeTaskQueuePartitionRequest,
			_ ...grpc.CallOption,
		) (*matchingservice.DescribeTaskQueuePartitionResponse, error) {
			id := req.GetTaskQueuePartition().GetNormalPartitionId()
			// Echo the manager's current scale info so partitionIsFullyDrained's
			// version/read/write equality check passes at every round. This runs on
			// the background goroutine that owns scaleState, so the read is safe.
			info := scaleStateToInfo(s.sm.scaleState, s.settings)
			return &matchingservice.DescribeTaskQueuePartitionResponse{
				ScaleInfo: info,
				VersionsInfoInternal: map[string]*taskqueuespb.TaskQueueVersionInfoInternal{
					"v1": {
						PhysicalTaskQueueInfo: &taskqueuespb.PhysicalTaskQueueInfo{
							InternalTaskQueueStatus: []*taskqueuespb.InternalTaskQueueStatus{
								{BacklogDrained: (*drained.Load())[id]},
							},
						},
					},
				},
			}, nil
		}).AnyTimes()

	// The timer also calls the scaler; keep target stable with NoChange.
	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Return(PartitionScalerDecision{NoChange: true}).AnyTimes()

	dbWrites := make(chan *persistencespb.PartitionScaleState, 2)
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		Do(func(state *persistencespb.PartitionScaleState, _ bool) {
			dbWrites <- common.CloneProto(state)
		}).
		Return(nil).AnyTimes()

	scaleInfos := make(chan *taskqueuespb.PartitionScaleInfo, 3)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).
		Do(func(info *taskqueuespb.PartitionScaleInfo) { scaleInfos <- info }).AnyTimes()

	s.startManager(10, initial)

	// Start pushes the initial counts: Read=10, Write=max(1, 10-2)=8.
	i0 := waitRecv(s, scaleInfos, "no start push")
	s.Equal(int32(10), i0.Read, "initial read")
	s.Equal(int32(8), i0.Write, "initial write")

	// Round 1: partitions 7,8,9 fully drain.
	setDrained(7, 8, 9)
	s.fireBackgroundTimer()
	st1 := waitRecv(s, dbWrites, "round 1 drain never wrote")
	s.Equal(int32(7), bitSet(st1.BacklogState).len(), "round 1 backlog count")
	s.False(bitSet(st1.BacklogState).get(7))
	s.False(bitSet(st1.BacklogState).get(8))
	s.False(bitSet(st1.BacklogState).get(9))
	i1 := waitRecv(s, scaleInfos, "no round 1 push")
	s.Equal(int32(7), i1.Read, "round 1 read")   // backlog len 7
	s.Equal(int32(5), i1.Write, "round 1 write") // max(1, 7-2)

	// Round 2: everything else drains. Partition 0 (id < target) keeps its bit.
	setDrained(1, 2, 3, 4, 5, 6)
	s.fireBackgroundTimer()
	st2 := waitRecv(s, dbWrites, "round 2 drain never wrote")
	s.Equal(int32(1), bitSet(st2.BacklogState).len(), "round 2 backlog count")
	s.True(bitSet(st2.BacklogState).get(0), "partition below target stays set")
	i2 := waitRecv(s, scaleInfos, "no round 2 push")
	s.Equal(int32(1), i2.Read, "round 2 read")   // max(target 1, backlog len 1)
	s.Equal(int32(1), i2.Write, "round 2 write") // max(1, 1-1)
}

// TestNoDrainWithBacklog verifies that partitions still reporting backlog
// (BacklogDrained=false) keep their bits set.
func (s *ScaleManagerSuite) TestNoDrainWithBacklog() {
	s.settings.BackgroundInterval = 50 * time.Millisecond
	s.settings.DrainBufferTime = 0

	initial := &persistencespb.PartitionScaleState{
		Target:        2,
		MaxTarget:     4,
		TargetVersion: 0,
		BacklogState:  bitSet(nil).set(0).set(1).set(2).set(3),
	}
	notDrainedResp := &matchingservice.DescribeTaskQueuePartitionResponse{
		ScaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 4, Write: 2, Version: 0},
		VersionsInfoInternal: map[string]*taskqueuespb.TaskQueueVersionInfoInternal{
			"v1": {
				PhysicalTaskQueueInfo: &taskqueuespb.PhysicalTaskQueueInfo{
					InternalTaskQueueStatus: []*taskqueuespb.InternalTaskQueueStatus{
						{BacklogDrained: false},
					},
				},
			},
		},
	}
	s.matching.EXPECT().DescribeTaskQueuePartition(gomock.Any(), gomock.Any()).
		Return(notDrainedResp, nil).AnyTimes()

	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Return(PartitionScalerDecision{NoChange: true}).AnyTimes()

	// Only the push on Start (0 → Read=4/Write=2) is expected.
	scaleInfos := make(chan *taskqueuespb.PartitionScaleInfo, 2)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).
		Do(func(info *taskqueuespb.PartitionScaleInfo) { scaleInfos <- info }).AnyTimes()

	var dbWrites atomic.Int32
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		DoAndReturn(func(*persistencespb.PartitionScaleState, bool) error {
			dbWrites.Add(1)
			return nil
		}).AnyTimes()

	s.startManager(4, initial)

	// Start: Read=4 (backlog 0..3), Write=2 (target; shrink limit off by default).
	i0 := waitRecv(s, scaleInfos, "no start push")
	s.Equal(int32(4), i0.Read, "start read")
	s.Equal(int32(2), i0.Write, "start write")

	s.fireBackgroundTimer()

	s.Require().Never(func() bool { return dbWrites.Load() > 0 },
		30*time.Millisecond, time.Millisecond, "drain wrote despite backlog")
	// No backlog cleared, so no further ephemeral push beyond the one at Start.
	assertNoRecv(s, scaleInfos, 30*time.Millisecond, "unexpected ephemeral push without drain")
}

// TestDrainSkippedDuringBufferTime verifies that drain is not evaluated until
// DrainBufferTime has passed since the last target change.
func (s *ScaleManagerSuite) TestDrainSkippedDuringBufferTime() {
	s.settings.BackgroundInterval = 50 * time.Millisecond
	s.settings.DrainBufferTime = time.Hour // never elapses

	initial := &persistencespb.PartitionScaleState{
		Target:        2,
		MaxTarget:     4,
		TargetVersion: 0,
		BacklogState:  bitSet(nil).set(0).set(1).set(2).set(3),
	}
	s.matching.EXPECT().DescribeTaskQueuePartition(gomock.Any(), gomock.Any()).
		Return(&matchingservice.DescribeTaskQueuePartitionResponse{}, nil).AnyTimes()

	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Return(PartitionScalerDecision{NoChange: true}).AnyTimes()

	scaleInfos := make(chan *taskqueuespb.PartitionScaleInfo, 2)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).
		Do(func(info *taskqueuespb.PartitionScaleInfo) { scaleInfos <- info }).AnyTimes()

	var dbWrites atomic.Int32
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		DoAndReturn(func(*persistencespb.PartitionScaleState, bool) error {
			dbWrites.Add(1)
			return nil
		}).AnyTimes()

	s.startManager(4, initial)

	// Start: Read=4 (backlog 0..3), Write=2 (target; shrink limit off by default).
	i0 := waitRecv(s, scaleInfos, "no start push")
	s.Equal(int32(4), i0.Read, "start read")
	s.Equal(int32(2), i0.Write, "start write")

	s.fireBackgroundTimer()

	s.Require().Never(func() bool { return dbWrites.Load() > 0 },
		30*time.Millisecond, time.Millisecond, "drain wrote inside DrainBufferTime")
	// Drain skipped during buffer time, so no push beyond the one at Start.
	assertNoRecv(s, scaleInfos, 30*time.Millisecond, "unexpected ephemeral push during buffer time")
}

// TestScaleStateToInfoShrinkLimit exercises the write-shrink limiting in
// scaleStateToInfo. Read is max(target, backlog len); write is held up near
// read so it can't shrink faster than allowedShrink = max(1, min(read*ShrinkRatio,
// ShrinkDelta)) below read, with target as the floor. This matters during drain,
// when read exceeds target.
func (s *ScaleManagerSuite) TestScaleStateToInfoShrinkLimit() {
	// backlog builds a BacklogState with bits 0..n-1 set, giving a read count of n.
	backlog := func(n int32) bitSet {
		b := bitSet(nil)
		for i := range n {
			b = b.set(i)
		}
		return b
	}

	cases := []struct {
		name        string
		target      int32
		backlogLen  int32
		shrinkRatio float32
		shrinkDelta int32
		wantRead    int32
		wantWrite   int32
	}{
		{
			name:        "no drain: read==target, write==target regardless of shrink",
			target:      10,
			backlogLen:  10,
			shrinkRatio: 0.1,
			shrinkDelta: 8,
			wantRead:    10,
			wantWrite:   10,
		},
		{
			name:        "ratio dominates: read=10 ratio=0.1 -> shrink 1",
			target:      2,
			backlogLen:  10,
			shrinkRatio: 0.1,
			shrinkDelta: 8,
			wantRead:    10,
			wantWrite:   9, // 10 - max(1, min(1, 8))
		},
		{
			name:        "delta dominates: read=200 ratio=0.1 (=20) capped at delta 8",
			target:      2,
			backlogLen:  200,
			shrinkRatio: 0.1,
			shrinkDelta: 8,
			wantRead:    200,
			wantWrite:   192, // 200 - 8
		},
		{
			name:        "no shrink limit (suite defaults): write falls to target",
			target:      2,
			backlogLen:  10,
			shrinkRatio: 1.0,
			shrinkDelta: 100,
			wantRead:    10,
			wantWrite:   2, // 10 - min(10, 100) = 0, floored to target
		},
		{
			name:        "shrink floored to 1 when ratio rounds to 0",
			target:      2,
			backlogLen:  5,
			shrinkRatio: 0.0,
			shrinkDelta: 8,
			wantRead:    5,
			wantWrite:   4, // 5 - max(1, min(0, 8))
		},
		{
			name:        "target floor wins over read-allowedShrink",
			target:      9,
			backlogLen:  10,
			shrinkRatio: 0.1,
			shrinkDelta: 8,
			wantRead:    10,
			wantWrite:   9, // max(9, 10-1)
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			settings := s.settings
			settings.ShrinkRatio = tc.shrinkRatio
			settings.ShrinkDelta = tc.shrinkDelta

			state := &persistencespb.PartitionScaleState{
				Target:        tc.target,
				TargetVersion: 12345,
				BacklogState:  backlog(tc.backlogLen),
			}
			info := scaleStateToInfo(state, settings)
			s.Equal(tc.wantRead, info.Read, "read")
			s.Equal(tc.wantWrite, info.Write, "write")
			s.Equal(int64(12345), info.Version, "version")
		})
	}

	// nil state: read and write are both 0.
	info := scaleStateToInfo(nil, s.settings)
	s.Equal(int32(0), info.Read)
	s.Equal(int32(0), info.Write)
}

// TestDBWriteFailureKeepsState verifies that when persistence fails the
// in-memory state and ephemeral data are not advanced. A subsequent successful
// write should then update both.
func (s *ScaleManagerSuite) TestDBWriteFailureKeepsState() {
	inputs := make(chan PartitionScalerInput, 2)
	gomock.InOrder(
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 2}),
		s.scaler.EXPECT().OnTasks(gomock.Any()).
			Do(func(in PartitionScalerInput) { inputs <- in }).
			Return(PartitionScalerDecision{NewTarget: 3}),
	)

	dbWrites := make(chan *persistencespb.PartitionScaleState, 1)
	gomock.InOrder(
		s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
			Return(errors.New("boom")),
		s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
			Do(func(state *persistencespb.PartitionScaleState, _ bool) {
				dbWrites <- common.CloneProto(state)
			}).
			Return(nil),
	)

	// Only the successful write triggers an ephemeral push (first attempt is
	// dropped before setState).
	scaleInfos := make(chan *taskqueuespb.PartitionScaleInfo, 1)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).
		Do(func(info *taskqueuespb.PartitionScaleInfo) { scaleInfos <- info })

	s.startManager(4, nil)

	// First decision: scaler called, DB write fails. lastDecision is not set,
	// so cooldown is not engaged.
	s.sm.AddedTasks(1)
	waitRecv(s, inputs, "first decision missing")

	// No ephemeral push yet; the failed write must not have advanced state.
	assertNoRecv(s, scaleInfos, 30*time.Millisecond, "ephemeral data pushed after failed write")

	// Recovery: trigger another decision; this one persists.
	s.sm.AddedTasks(1)
	waitRecv(s, inputs, "second decision missing")
	state := waitRecv(s, dbWrites, "second db write missing")
	s.Equal(int32(3), state.Target, "second decision's target landed (first was dropped)")
	waitRecv(s, scaleInfos, "no ephemeral data push after recovery")
}

// dbWrite bundles the state and sync flag from an UpdateScaleState call so tests
// can assert on the sync argument (in-memory vs. persisted).
type dbWrite struct {
	state *persistencespb.PartitionScaleState
	sync  bool
}

// backlogDescribeResponse builds a describe response reporting the given
// approximate backlog count on a single unversioned physical queue.
func backlogDescribeResponse(count int64, drained bool) *matchingservice.DescribeTaskQueuePartitionResponse {
	return &matchingservice.DescribeTaskQueuePartitionResponse{
		ScaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 4, Write: 2, Version: 0},
		VersionsInfoInternal: map[string]*taskqueuespb.TaskQueueVersionInfoInternal{
			"": {
				PhysicalTaskQueueInfo: &taskqueuespb.PhysicalTaskQueueInfo{
					InternalTaskQueueStatus: []*taskqueuespb.InternalTaskQueueStatus{
						{ApproximateBacklogCount: count, BacklogDrained: drained},
					},
				},
			},
		},
	}
}

// TestBacklogCountUpdateInMemoryOnly verifies that when the periodic check finds
// only changed backlog counts (no drain), the new counts are recorded with a
// non-synchronous DB write (persisted lazily), the target is untouched, and read
// partitions are preserved.
func (s *ScaleManagerSuite) TestBacklogCountUpdateInMemoryOnly() {
	s.settings.BackgroundInterval = 50 * time.Millisecond
	s.settings.DrainBufferTime = time.Hour // disable drain; isolate the backlog-count path

	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Return(PartitionScalerDecision{NoChange: true}).AnyTimes()
	s.matching.EXPECT().DescribeTaskQueuePartition(gomock.Any(), gomock.Any()).
		Return(backlogDescribeResponse(500, false), nil).AnyTimes()

	writes := make(chan dbWrite, 1)
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		Do(func(state *persistencespb.PartitionScaleState, sync bool) {
			writes <- dbWrite{common.CloneProto(state), sync}
		}).
		Return(nil)
	pushes := make(chan *taskqueuespb.PartitionScaleInfo, 4)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).
		Do(func(info *taskqueuespb.PartitionScaleInfo) { pushes <- common.CloneProto(info) }).AnyTimes()

	initial := &persistencespb.PartitionScaleState{
		Target:       2,
		MaxTarget:    2,
		BacklogState: bitSet(nil).set(0).set(1),
	}
	s.startManager(4, initial)
	s.fireBackgroundTimer()

	w := waitRecv(s, writes, "backlog update never wrote")
	s.False(w.sync, "backlog-count-only update must not force a synchronous DB write")
	s.Equal(int32(2), w.state.Target, "target must not change on a backlog-count update")
	s.Equal([]byte{number.EncodeCompact8(500), number.EncodeCompact8(500)}, w.state.BacklogCounts)
	s.Equal(int32(2), bitSet(w.state.BacklogState).len(), "read partitions preserved (no drain)")

	// The DB write above happens before setState applies the state in memory and
	// pushes the new ephemeral data. Synchronize on that push (rather than reading
	// s.sm.scaleState, which would race the worker) to confirm the counts were
	// applied and propagated to clients via scaleStateToInfo.
	var info *taskqueuespb.PartitionScaleInfo
	for info.GetBacklogCounts() == nil {
		info = waitRecv(s, pushes, "no ephemeral push carrying the new backlog counts")
	}
	s.Equal([]byte{number.EncodeCompact8(500), number.EncodeCompact8(500)}, info.BacklogCounts)
	s.Equal(int32(2), info.Write, "target unchanged in pushed info")
}

// TestNoWriteWhenBacklogUnchanged verifies that when the sampled backlog counts
// match what's already stored and nothing drains, no DB write happens at all.
func (s *ScaleManagerSuite) TestNoWriteWhenBacklogUnchanged() {
	s.settings.BackgroundInterval = 50 * time.Millisecond
	s.settings.DrainBufferTime = time.Hour // disable drain

	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Return(PartitionScalerDecision{NoChange: true}).AnyTimes()
	s.matching.EXPECT().DescribeTaskQueuePartition(gomock.Any(), gomock.Any()).
		Return(backlogDescribeResponse(500, false), nil).AnyTimes()

	var dbWrites atomic.Int32
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		DoAndReturn(func(*persistencespb.PartitionScaleState, bool) error {
			dbWrites.Add(1)
			return nil
		}).AnyTimes()
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).AnyTimes()

	c8 := number.EncodeCompact8(500)
	initial := &persistencespb.PartitionScaleState{
		Target:        2,
		MaxTarget:     2,
		BacklogState:  bitSet(nil).set(0).set(1),
		BacklogCounts: []byte{c8, c8}, // already equal to what describe reports
	}
	s.startManager(4, initial)
	s.fireBackgroundTimer()

	s.Require().Never(func() bool { return dbWrites.Load() > 0 },
		30*time.Millisecond, time.Millisecond, "no write expected when backlog is unchanged and nothing drains")
}

// TestDrainWritePersistsBacklogCountsAndSyncs verifies the drain path: when
// partitions drain, the write is synchronous (sync=true), drained bits are
// cleared, and the freshly sampled backlog counts ride along on the same write.
func (s *ScaleManagerSuite) TestDrainWritePersistsBacklogCountsAndSyncs() {
	s.settings.BackgroundInterval = 50 * time.Millisecond
	s.settings.DrainBufferTime = 0

	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Return(PartitionScalerDecision{NoChange: true}).AnyTimes()

	// Partitions 0,1 (write side) still have backlog; 2,3 (draining side) are drained.
	s.matching.EXPECT().DescribeTaskQueuePartition(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			req *matchingservice.DescribeTaskQueuePartitionRequest,
			_ ...grpc.CallOption,
		) (*matchingservice.DescribeTaskQueuePartitionResponse, error) {
			if req.GetTaskQueuePartition().GetNormalPartitionId() >= 2 {
				return backlogDescribeResponse(0, true), nil
			}
			return backlogDescribeResponse(500, false), nil
		}).AnyTimes()

	writes := make(chan dbWrite, 1)
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		Do(func(state *persistencespb.PartitionScaleState, sync bool) {
			writes <- dbWrite{common.CloneProto(state), sync}
		}).
		Return(nil)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).AnyTimes()

	initial := &persistencespb.PartitionScaleState{
		Target:        2,
		MaxTarget:     4,
		TargetVersion: 0,
		BacklogState:  bitSet(nil).set(0).set(1).set(2).set(3),
	}
	s.startManager(4, initial)
	s.fireBackgroundTimer()

	w := waitRecv(s, writes, "drain never wrote")
	s.True(w.sync, "a drain-bit change must force a synchronous DB write")
	s.Equal(int32(2), w.state.Target, "target unchanged during drain")
	s.Equal([]byte{number.EncodeCompact8(500), number.EncodeCompact8(500), 0, 0}, w.state.BacklogCounts)
	s.True(bitSet(w.state.BacklogState).get(0))
	s.True(bitSet(w.state.BacklogState).get(1))
	s.False(bitSet(w.state.BacklogState).get(2), "drained partition bit cleared")
	s.False(bitSet(w.state.BacklogState).get(3), "drained partition bit cleared")
}

// TestBacklogCapChangePersists verifies that a decision that keeps the same
// target but changes the backlog cap is still applied (not short-circuited as
// "no change"), and that the cap is stored/pushed as its Compact8 encoding.
func (s *ScaleManagerSuite) TestBacklogCapChangePersists() {
	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Return(PartitionScalerDecision{NewTarget: 3, BacklogCap: 1000})

	dbWrites := make(chan *persistencespb.PartitionScaleState, 1)
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		Do(func(state *persistencespb.PartitionScaleState, _ bool) {
			dbWrites <- common.CloneProto(state)
		}).
		Return(nil)

	scaleInfos := make(chan *taskqueuespb.PartitionScaleInfo, 2)
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).
		Do(func(info *taskqueuespb.PartitionScaleInfo) { scaleInfos <- info }).AnyTimes()

	// Current target is already 3, so only the backlog cap differs from the decision.
	s.startManager(4, &persistencespb.PartitionScaleState{Target: 3, MaxTarget: 3})

	s.sm.AddedTasks(1)
	state := waitRecv(s, dbWrites, "cap-only change never persisted")
	s.Equal(int32(3), state.Target, "target unchanged; the cap change alone must trigger the write")
	s.Equal(int32(number.EncodeCompact8(1000)), state.BacklogCap)

	waitRecv(s, scaleInfos, "start push missing")
	info := waitRecv(s, scaleInfos, "cap push missing")
	s.Equal(int32(number.EncodeCompact8(1000)), info.BacklogCap, "cap must propagate to ephemeral data")
}

// TestSameTargetAndCapSkipsWrite verifies that a decision matching both the
// current target and the current backlog cap is short-circuited (no write), and
// that the stored backlog counts are fed into the scaler input.
func (s *ScaleManagerSuite) TestSameTargetAndCapSkipsWrite() {
	c8 := number.EncodeCompact8(500)

	inputs := make(chan PartitionScalerInput, 1)
	s.scaler.EXPECT().OnTasks(gomock.Any()).
		Do(func(in PartitionScalerInput) { inputs <- in }).
		Return(PartitionScalerDecision{NewTarget: 3, BacklogCap: 1000})

	var dbWrites atomic.Int32
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		DoAndReturn(func(*persistencespb.PartitionScaleState, bool) error {
			dbWrites.Add(1)
			return nil
		}).AnyTimes()
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).AnyTimes()

	initial := &persistencespb.PartitionScaleState{
		Target:        3,
		MaxTarget:     3,
		BacklogCap:    int32(number.EncodeCompact8(1000)),
		BacklogCounts: []byte{c8, c8, c8},
	}
	s.startManager(4, initial)

	s.sm.AddedTasks(1)
	in := waitRecv(s, inputs, "scaler never called")
	s.Equal([]byte{c8, c8, c8}, in.BacklogCounts, "stored backlog counts must be fed to the scaler")

	s.Require().Never(func() bool { return dbWrites.Load() > 0 },
		30*time.Millisecond, time.Millisecond, "no write when both target and cap are unchanged")
}
