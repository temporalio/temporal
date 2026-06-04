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
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
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

	// scaler.Stop is called from sm.Stop in TearDownTest, but only if the test
	// actually started the manager.
	s.scaler.EXPECT().Stop().AnyTimes()

	s.settings = dynamicconfig.PartitionScaleManagerSettings{
		MaxRate:            10, // 100ms cooldown
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
	s.sm = newScaleManager(
		context.Background(),
		tqid.MustNormalPartitionFromRpcName("tq", "ns-id", enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		s.userData,
		s.matching,
		s.scaler,
		s.timeSource,
		dynamicconfig.GetTypedPropertyFn(s.settings),
		dynamicconfig.GetIntPropertyFn(writePartitions),
		dynamicconfig.GetBoolPropertyFn(false),
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
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).Times(2)

	s.startManager(4, initial)
	s.fireBackgroundTimer()

	state := waitRecv(s, dbWrites, "drain never wrote")
	s.Equal(int32(2), state.Target, "target should not change during drain")
	// Drained ids 2 and 3 should be cleared; ids 0 and 1 stay set (id < target).
	s.Equal(int32(2), bitSet(state.BacklogState).len())
	s.True(bitSet(state.BacklogState).get(0))
	s.True(bitSet(state.BacklogState).get(1))
	s.False(bitSet(state.BacklogState).get(2))
	s.False(bitSet(state.BacklogState).get(3))
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
	s.userData.EXPECT().SetPartitionScale(gomock.Any()).AnyTimes()

	var dbWrites atomic.Int32
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		DoAndReturn(func(*persistencespb.PartitionScaleState, bool) error {
			dbWrites.Add(1)
			return nil
		}).AnyTimes()

	s.startManager(4, initial)
	s.fireBackgroundTimer()

	s.Require().Never(func() bool { return dbWrites.Load() > 0 },
		30*time.Millisecond, time.Millisecond, "drain wrote despite backlog")
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

	s.userData.EXPECT().SetPartitionScale(gomock.Any()).AnyTimes()

	var dbWrites atomic.Int32
	s.scaleDB.EXPECT().UpdateScaleState(gomock.Any(), gomock.Any()).
		DoAndReturn(func(*persistencespb.PartitionScaleState, bool) error {
			dbWrites.Add(1)
			return nil
		}).AnyTimes()

	s.startManager(4, initial)
	s.fireBackgroundTimer()

	s.Require().Never(func() bool { return dbWrites.Load() > 0 },
		30*time.Millisecond, time.Millisecond, "drain wrote inside DrainBufferTime")
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
