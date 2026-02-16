package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TimeSkippingTestSuite struct {
	testcore.FunctionalTestBase
}

// timeSkippingTaskProcessingDelta is the maximum expected wall-clock time
// between related events in a test (e.g., creating a timer and advancing past
// it). Every use of this constant means "the only error source here is real
// elapsed time during test execution."
const timeSkippingTaskProcessingDelta = 2 * time.Second

func TestTimeSkippingTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(TimeSkippingTestSuite))
}

func (s *TimeSkippingTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
}

func (s *TimeSkippingTestSuite) TestAdvanceTimePoint_SingleTimer() {
	tq := "ts-tq-" + uuid.NewString()
	identity := "worker1"

	we := s.startWorkflowWithTimeSkipping(tq)

	timerScheduled := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !timerScheduled {
			timerScheduled = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            "timer-1",
					StartToFireTimeout: durationpb.New(1 * time.Hour),
				}},
			}}, nil
		}
		// Timer was fired by time-skipping advance; complete the workflow.
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("done"),
			}},
		}}, nil
	}

	poller := s.newPoller(tq, identity, wtHandler)

	// Schedule the timer.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Get history to extract exact event times for subsequent assertions.
	// After the first workflow task, history is: Started, TaskScheduled, TaskStarted, TaskCompleted, TimerStarted.
	earlyHistory := s.GetHistory(s.Namespace().String(), we)
	s.GreaterOrEqual(len(earlyHistory), 5)

	startEventTime := earlyHistory[0].GetEventTime()                      // WorkflowExecutionStarted
	firstTaskStartedTime := earlyHistory[2].GetEventTime()                // WorkflowTaskStarted (3)
	firstTaskCompletedTime := earlyHistory[3].GetEventTime()              // WorkflowTaskCompleted (4)
	timerStartedTime := earlyHistory[4].GetEventTime()                    // TimerStarted (5)
	expectedTimerFireTime := timerStartedTime.AsTime().Add(1 * time.Hour) // ExpiryTime = TimerStarted.EventTime + duration

	// Describe before advance: verify time-skipping info and upcoming time points.
	descBefore := s.describeWorkflow(we)

	tsInfo := descBefore.GetTimeSkippingInfo()
	s.NotNil(tsInfo)
	s.True(tsInfo.GetConfig().GetEnabled())
	// Initial offset is zero (no advances yet).
	s.Equal(time.Duration(0), tsInfo.GetVirtualTimeOffset().AsDuration())

	// Find the timer in upcoming time points.
	upcomingBefore := descBefore.GetUpcomingTimePoints()
	s.Len(upcomingBefore, 1)
	timerPoint := upcomingBefore[0]
	s.NotNil(timerPoint.GetTimer())
	s.Equal("timer-1", timerPoint.GetTimer().GetTimerId())
	// Timer fire time == TimerStarted.EventTime + 1h exactly.
	s.Equal(expectedTimerFireTime, timerPoint.GetFireTime().AsTime())
	s.Equal(earlyHistory[4].GetEventId(), timerPoint.GetTimer().GetStartedEventId())

	// Advance time: fire the 1h timer.
	advResp := s.advanceTimePoint(we)
	s.NotEmpty(advResp.GetAdvancedTimePoints(), "should have fired at least one time point")
	// Verify the fired time point is our timer.
	s.Len(advResp.GetAdvancedTimePoints(), 1)
	firedTP := advResp.GetAdvancedTimePoints()[0]
	s.NotNil(firedTP.GetTimer())
	s.Equal("timer-1", firedTP.GetTimer().GetTimerId())
	s.Equal(expectedTimerFireTime, firedTP.GetFireTime().AsTime())
	// Offset ≈ 1h. The only error is wall-clock elapsed between TimerStarted
	// event and the advance call.
	advOffset := advResp.GetTimeSkippingInfo().GetVirtualTimeOffset().AsDuration()
	s.InDelta(float64(1*time.Hour), float64(advOffset), float64(timeSkippingTaskProcessingDelta))

	// Describe after advance: offset should match the advance response.
	descAfter := s.describeWorkflow(we)
	tsInfoAfter := descAfter.GetTimeSkippingInfo()
	s.NotNil(tsInfoAfter)
	s.Equal(advOffset, tsInfoAfter.GetVirtualTimeOffset().AsDuration())
	// timer-1 should no longer appear in upcoming time points.
	for _, pt := range descAfter.GetUpcomingTimePoints() {
		if pt.GetTimer() != nil {
			s.NotEqual("timer-1", pt.GetTimer().GetTimerId(), "timer-1 should have been fired and removed")
		}
	}

	// Complete the workflow.
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Verify full history event sequence.
	historyEvents := s.GetHistory(s.Namespace().String(), we)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 TimerStarted
  6 WorkflowExecutionTimePointAdvanced
  7 TimerFired
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowExecutionCompleted`, historyEvents)

	// Verify event times are monotonically non-decreasing.
	for i := 1; i < len(historyEvents); i++ {
		s.False(historyEvents[i].GetEventTime().AsTime().Before(historyEvents[i-1].GetEventTime().AsTime()),
			"event %d time should not be before event %d time", i+1, i)
	}

	// Verify all event times against known values.
	// Events 1-5 were verified above from earlyHistory. Now verify events 6-11.
	s.Equal(startEventTime.AsTime(), historyEvents[0].GetEventTime().AsTime())         // 1: WorkflowExecutionStarted
	s.Equal(firstTaskStartedTime.AsTime(), historyEvents[2].GetEventTime().AsTime())   // 3: WorkflowTaskStarted
	s.Equal(firstTaskCompletedTime.AsTime(), historyEvents[3].GetEventTime().AsTime()) // 4: WorkflowTaskCompleted
	s.Equal(timerStartedTime.AsTime(), historyEvents[4].GetEventTime().AsTime())       // 5: TimerStarted

	// TimePointAdvanced (6) is at old offset (0). TimerFired (7) is at new
	// offset. Since both share the same wall clock, timerFired event time
	// equals exactly the timer's fire time.
	timerFiredEventTime := historyEvents[6].GetEventTime()
	s.Equal(expectedTimerFireTime, timerFiredEventTime.AsTime())

	// WorkflowTaskStarted (3): before advance, wall clock only. Delta is
	// elapsed time from event creation to this assertion.
	s.WithinDuration(time.Now(), firstTaskStartedTime.AsTime(), timeSkippingTaskProcessingDelta)
	// WorkflowTaskStarted (9): after advance, wall clock + offset. Delta is
	// elapsed time from event creation to this assertion.
	secondTaskStartedTime := historyEvents[8].GetEventTime() // 9: WorkflowTaskStarted
	s.WithinDuration(time.Now().Add(advOffset), secondTaskStartedTime.AsTime(), timeSkippingTaskProcessingDelta)

	// TIME_POINT_ADVANCED event (6): verify all attributes.
	advancedEvent := historyEvents[5]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_POINT_ADVANCED, advancedEvent.GetEventType())
	s.True(advancedEvent.GetWorkerMayIgnore())
	attrs := advancedEvent.GetWorkflowExecutionTimePointAdvancedEventAttributes()
	s.NotNil(attrs)
	s.Equal("test-advancer", attrs.GetIdentity())
	s.NotEmpty(attrs.GetRequestId())
	// Exact invariant: advanceEvent.EventTime + DurationAdvanced == fireTime
	// (durationAdvanced is computed as fireTime - eventTime in the EventFactory).
	durationAdvanced := attrs.GetDurationAdvanced().AsDuration()
	s.True(durationAdvanced > 0, "duration_advanced should be positive")
	computedFireTime := advancedEvent.GetEventTime().AsTime().Add(durationAdvanced)
	s.Equal(expectedTimerFireTime, computedFireTime)

	// TimerFired event (7): verify timer ID and started event reference.
	timerFiredEvent := historyEvents[6]
	s.Equal(enumspb.EVENT_TYPE_TIMER_FIRED, timerFiredEvent.GetEventType())
	firedAttrs := timerFiredEvent.GetTimerFiredEventAttributes()
	s.Equal("timer-1", firedAttrs.GetTimerId())
	s.Equal(earlyHistory[4].GetEventId(), firedAttrs.GetStartedEventId())
}

func (s *TimeSkippingTestSuite) TestAutoSkip_TimerLoop() {
	tq := "ts-tq-" + uuid.NewString()
	identity := "worker1"
	timerDuration := 1 * time.Hour

	// Start workflow with time-skipping and auto-skip (default max_firings = 10).
	we := s.startWorkflowWithAutoSkip(tq, &workflowpb.TimeSkippingConfig_AutoSkipConfig{})

	wftCount := 0
	shouldComplete := false

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wftCount++
		if shouldComplete {
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				}},
			}}, nil
		}
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_START_TIMER,
			Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
				TimerId:            fmt.Sprintf("timer-%d", wftCount-1),
				StartToFireTimeout: durationpb.New(timerDuration),
			}},
		}}, nil
	}

	poller := s.newPoller(tq, identity, wtHandler)

	// History extraction helper.
	type timerEvents struct {
		started  []time.Time
		fired    []time.Time
		autoSkip int
		manual   int
	}
	getTimerEvents := func() timerEvents {
		var te timerEvents
		for _, event := range s.GetHistory(s.Namespace().String(), we) {
			switch event.GetEventType() {
			case enumspb.EVENT_TYPE_TIMER_STARTED:
				te.started = append(te.started, event.GetEventTime().AsTime())
			case enumspb.EVENT_TYPE_TIMER_FIRED:
				te.fired = append(te.fired, event.GetEventTime().AsTime())
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_POINT_ADVANCED:
				attrs := event.GetWorkflowExecutionTimePointAdvancedEventAttributes()
				if attrs.GetIdentity() == "auto-skip" {
					te.autoSkip++
				} else {
					te.manual++
				}
			}
		}
		return te
	}
	// Each TimerFired event time should be very close to TimerStarted + duration.
	// There is a sub-millisecond wall-clock delta between the TIME_POINT_ADVANCED
	// event and the TimerFired event (both call time.Now() independently).
	assertTimerFireTimes := func(te timerEvents) {
		for i := 0; i < len(te.fired); i++ {
			expected := te.started[i].Add(timerDuration)
			s.WithinDuration(expected, te.fired[i], 300*time.Millisecond,
				"timer-%d fire time should be within 300ms of start time + %v", i, timerDuration,
			)
		}
	}

	// ===== Phase 1: Default max_firings (10) =====
	// Each WFT: handler schedules 1h timer → respond → auto-skip fires → new WFT.
	// 11 WFTs: initial + 10 auto-skip firings. Auto-skip exhausts at firings_used=10.
	for i := 0; i < 11; i++ {
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)
	}
	s.Equal(11, wftCount)

	// Describe: auto-skip exhausted.
	desc1 := s.describeWorkflow(we)
	autoSkip1 := desc1.GetTimeSkippingInfo().GetAutoSkip()
	s.Equal(int32(10), autoSkip1.GetFiringsUsed())
	s.Equal(int32(0), autoSkip1.GetFiringsRemaining())
	s.False(autoSkip1.GetActive())
	offset1 := desc1.GetTimeSkippingInfo().GetVirtualTimeOffset().AsDuration()
	s.InDelta(float64(10*time.Hour), float64(offset1), float64(1*time.Minute))
	s.Len(desc1.GetUpcomingTimePoints(), 1, "1 pending timer (timer-10)")

	// History: 11 timers started (timer-0..10), 10 fired (timer-0..9), 10 auto-skip advances.
	te1 := getTimerEvents()
	s.Len(te1.started, 11)
	s.Len(te1.fired, 10)
	s.Equal(10, te1.autoSkip)
	s.Equal(0, te1.manual)
	assertTimerFireTimes(te1)

	// ===== Phase 2: "until" bound =====
	// until = virtual_now + 3h30min. With 1h timers, allows exactly 3 firings
	// (at +11h, +12h, +13h) before the deadline at ~+13h30min.
	untilTime := time.Now().Add(offset1).Add(3*time.Hour + 30*time.Minute)
	s.updateAutoSkipConfig(we, &workflowpb.TimeSkippingConfig_AutoSkipConfig{
		Bound: &workflowpb.TimeSkippingConfig_AutoSkipConfig_UntilTime{
			UntilTime: timestamppb.New(untilTime),
		},
		MaxFirings: 100,
	})
	// UpdateOptions fires timer-10 immediately (firings=1), then WFT loop fires 2 more.
	for i := 0; i < 3; i++ {
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)
	}
	s.Equal(14, wftCount)

	desc2 := s.describeWorkflow(we)
	autoSkip2 := desc2.GetTimeSkippingInfo().GetAutoSkip()
	s.Equal(int32(3), autoSkip2.GetFiringsUsed())
	s.False(autoSkip2.GetActive())
	offset2 := desc2.GetTimeSkippingInfo().GetVirtualTimeOffset().AsDuration()
	s.InDelta(float64(13*time.Hour), float64(offset2), float64(1*time.Minute))

	// History: cumulative 14 started, 13 fired, 13 auto-skip advances.
	te2 := getTimerEvents()
	s.Len(te2.started, 14)
	s.Len(te2.fired, 13)
	s.Equal(13, te2.autoSkip)
	s.Equal(0, te2.manual)
	assertTimerFireTimes(te2)

	// ===== Phase 3: "duration" bound =====
	// duration = 3h30min from current virtual time. Same pattern: 3 firings.
	s.updateAutoSkipConfig(we, &workflowpb.TimeSkippingConfig_AutoSkipConfig{
		Bound: &workflowpb.TimeSkippingConfig_AutoSkipConfig_UntilDuration{
			UntilDuration: durationpb.New(3*time.Hour + 30*time.Minute),
		},
		MaxFirings: 100,
	})
	for i := 0; i < 3; i++ {
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)
	}
	s.Equal(17, wftCount)

	desc3 := s.describeWorkflow(we)
	autoSkip3 := desc3.GetTimeSkippingInfo().GetAutoSkip()
	s.Equal(int32(3), autoSkip3.GetFiringsUsed())
	s.False(autoSkip3.GetActive())
	offset3 := desc3.GetTimeSkippingInfo().GetVirtualTimeOffset().AsDuration()
	s.InDelta(float64(16*time.Hour), float64(offset3), float64(1*time.Minute))

	// History: cumulative 17 started, 16 fired, 16 auto-skip advances.
	te3 := getTimerEvents()
	s.Len(te3.started, 17)
	s.Len(te3.fired, 16)
	s.Equal(16, te3.autoSkip)
	s.Equal(0, te3.manual)
	assertTimerFireTimes(te3)

	// ===== Cleanup: complete the workflow =====
	shouldComplete = true
	advResp := s.advanceTimePoint(we)
	s.NotEmpty(advResp.GetAdvancedTimePoints())
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.Equal(18, wftCount)

	// Final history: 17 started, 17 fired, 16 auto-skip + 1 manual advance.
	teFinal := getTimerEvents()
	s.Len(teFinal.started, 17)
	s.Len(teFinal.fired, 17)
	s.Equal(16, teFinal.autoSkip)
	s.Equal(1, teFinal.manual)
	assertTimerFireTimes(teFinal)
}

func (s *TimeSkippingTestSuite) TestAutoSkip_PausedDuringActivity() {
	tq := "ts-tq-" + uuid.NewString()
	identity := "worker1"
	timerDuration := 1 * time.Hour

	// Start workflow with auto-skip (default max_firings = 10).
	we := s.startWorkflowWithAutoSkip(tq, &workflowpb.TimeSkippingConfig_AutoSkipConfig{})

	wftCount := 0

	// hasEventType scans new events in a WFT for a specific event type.
	hasEventType := func(task *workflowservice.PollWorkflowTaskQueueResponse, et enumspb.EventType) bool {
		for _, event := range task.History.Events[task.PreviousStartedEventId:] {
			if event.GetEventType() == et {
				return true
			}
		}
		return false
	}

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wftCount++
		switch wftCount {
		case 1:
			// Schedule both a 1-hour timer and an activity.
			return []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_START_TIMER,
					Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
						TimerId:            "timer-1",
						StartToFireTimeout: durationpb.New(timerDuration),
					}},
				},
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             "activity-1",
						ActivityType:           &commonpb.ActivityType{Name: "test-activity"},
						TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						Input:                  payloads.EncodeString("input"),
						ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
						ScheduleToStartTimeout: durationpb.New(100 * time.Second),
						StartToCloseTimeout:    durationpb.New(100 * time.Second),
					}},
				},
			}, nil
		case 2:
			// Activity completed; verify the event is present.
			s.True(hasEventType(task, enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED),
				"WFT #2 should see ActivityTaskCompleted")
			// Return no commands. Auto-skip should fire the timer after this
			// WFT completes (no more pending activities).
			return nil, nil
		default:
			// Timer was fired by auto-skip; verify the event is present.
			s.True(hasEventType(task, enumspb.EVENT_TYPE_TIMER_FIRED),
				"WFT #3 should see TimerFired")
			// Complete the workflow.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				}},
			}}, nil
		}
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		return payloads.EncodeString("activity-result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// WFT #1: Schedule timer + activity.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.Equal(1, wftCount)

	// Auto-skip should NOT have fired (activity is pending).
	desc1 := s.describeWorkflow(we)
	autoSkip1 := desc1.GetTimeSkippingInfo().GetAutoSkip()
	s.NotNil(autoSkip1)
	s.False(autoSkip1.GetActive(), "auto-skip should be inactive while activity is pending")
	s.Equal(int32(0), autoSkip1.GetFiringsUsed())
	s.Equal(time.Duration(0), desc1.GetTimeSkippingInfo().GetVirtualTimeOffset().AsDuration(),
		"offset should be 0 — no time skipped yet")
	// Timer and activity timeouts should both be in upcoming time points.
	var foundTimer, foundActivityTimeout bool
	for _, pt := range desc1.GetUpcomingTimePoints() {
		if pt.GetTimer() != nil && pt.GetTimer().GetTimerId() == "timer-1" {
			foundTimer = true
		}
		if pt.GetActivityTimeout() != nil && pt.GetActivityTimeout().GetActivityId() == "activity-1" {
			foundActivityTimeout = true
		}
	}
	s.True(foundTimer, "timer-1 should appear in upcoming time points")
	s.True(foundActivityTimeout, "activity-1 timeout should appear in upcoming time points")

	// Complete the activity.
	err = poller.PollAndProcessActivityTask(false)
	s.NoError(err)

	// WFT #2: Acknowledge activity result (empty commands).
	// After this WFT completes, auto-skip fires the timer (no pending activities).
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.Equal(2, wftCount)

	// WFT #3: Timer was fired by auto-skip; complete workflow.
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.Equal(3, wftCount)

	// Verify auto-skip state after workflow completion.
	desc2 := s.describeWorkflow(we)
	autoSkip2 := desc2.GetTimeSkippingInfo().GetAutoSkip()
	s.Equal(int32(1), autoSkip2.GetFiringsUsed())
	s.False(autoSkip2.GetActive(), "auto-skip should be inactive after workflow completion")
	offset := desc2.GetTimeSkippingInfo().GetVirtualTimeOffset().AsDuration()
	s.InDelta(float64(timerDuration), float64(offset), float64(1*time.Minute))

	// Verify history.
	historyEvents := s.GetHistory(s.Namespace().String(), we)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 TimerStarted
  6 ActivityTaskScheduled
  7 ActivityTaskStarted
  8 ActivityTaskCompleted
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskCompleted
 12 WorkflowExecutionTimePointAdvanced
 13 TimerFired
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted
 16 WorkflowTaskCompleted
 17 WorkflowExecutionCompleted`, historyEvents)

	// Verify TIME_POINT_ADVANCED has auto-skip identity.
	advancedEvent := historyEvents[11]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_POINT_ADVANCED, advancedEvent.GetEventType())
	attrs := advancedEvent.GetWorkflowExecutionTimePointAdvancedEventAttributes()
	s.Equal("auto-skip", attrs.GetIdentity())
	s.True(attrs.GetDurationAdvanced().AsDuration() > 0)
}

func (s *TimeSkippingTestSuite) TestAdvanceTimePoint_MultipleTimers_EarliestFires() {
	tq := "ts-tq-" + uuid.NewString()
	identity := "worker1"

	we := s.startWorkflowWithTimeSkipping(tq)

	wftCount := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wftCount++
		switch wftCount {
		case 1:
			// Schedule 3 timers: 30m, 1h, 2h.
			return []*commandpb.Command{
				s.timerCommand("timer-30m", 30*time.Minute),
				s.timerCommand("timer-1h", 1*time.Hour),
				s.timerCommand("timer-2h", 2*time.Hour),
			}, nil
		case 2:
			// First advance fired timer-30m only.
			return nil, nil
		case 3:
			// Second advance fired timer-1h only.
			return nil, nil
		default:
			// Third advance fired timer-2h; complete.
			return s.completeWorkflowCommands(), nil
		}
	}

	poller := s.newPoller(tq, identity, wtHandler)

	// WFT 1: schedule all 3 timers.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Advance #1: should fire only the 30m timer (earliest).
	resp1 := s.advanceTimePoint(we)
	s.Len(resp1.GetAdvancedTimePoints(), 1)
	s.Equal("timer-30m", resp1.GetAdvancedTimePoints()[0].GetTimer().GetTimerId())
	s.Len(resp1.GetUpcomingTimePoints(), 2, "2 timers remaining")
	offset1 := resp1.GetTimeSkippingInfo().GetVirtualTimeOffset().AsDuration()
	s.InDelta(float64(30*time.Minute), float64(offset1), float64(timeSkippingTaskProcessingDelta))

	// Process the WFT from the first advance.
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Advance #2: should fire the 1h timer (next earliest).
	resp2 := s.advanceTimePoint(we)
	s.Len(resp2.GetAdvancedTimePoints(), 1)
	s.Equal("timer-1h", resp2.GetAdvancedTimePoints()[0].GetTimer().GetTimerId())
	s.Len(resp2.GetUpcomingTimePoints(), 1, "1 timer remaining")
	offset2 := resp2.GetTimeSkippingInfo().GetVirtualTimeOffset().AsDuration()
	s.InDelta(float64(1*time.Hour), float64(offset2), float64(timeSkippingTaskProcessingDelta))

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Advance #3: should fire the 2h timer (last one).
	resp3 := s.advanceTimePoint(we)
	s.Len(resp3.GetAdvancedTimePoints(), 1)
	s.Equal("timer-2h", resp3.GetAdvancedTimePoints()[0].GetTimer().GetTimerId())
	s.Empty(resp3.GetUpcomingTimePoints(), "no timers remaining")
	offset3 := resp3.GetTimeSkippingInfo().GetVirtualTimeOffset().AsDuration()
	s.InDelta(float64(2*time.Hour), float64(offset3), float64(timeSkippingTaskProcessingDelta))

	// Complete workflow.
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// History: 3 advances, 3 timer fired, each advance before its fired event.
	historyEvents := s.GetHistory(s.Namespace().String(), we)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 TimerStarted
  6 TimerStarted
  7 TimerStarted
  8 WorkflowExecutionTimePointAdvanced
  9 TimerFired
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionTimePointAdvanced
 14 TimerFired
 15 WorkflowTaskScheduled
 16 WorkflowTaskStarted
 17 WorkflowTaskCompleted
 18 WorkflowExecutionTimePointAdvanced
 19 TimerFired
 20 WorkflowTaskScheduled
 21 WorkflowTaskStarted
 22 WorkflowTaskCompleted
 23 WorkflowExecutionCompleted`, historyEvents)
}

func (s *TimeSkippingTestSuite) TestPropagateToNewChildren() {
	parentTQ := "ts-tq-parent-" + uuid.NewString()
	childTQ := "ts-tq-child-" + uuid.NewString()
	identity := "worker1"
	childWorkflowID := "ts-child-" + uuid.NewString()

	// Start parent with time-skipping and propagate_to_new_children.
	parentWE := s.startWorkflowWithConfig(parentTQ, &workflowpb.TimeSkippingConfig{
		Enabled:                true,
		PropagateToNewChildren: true,
	})

	childStarted := false
	parentHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !childStarted {
			childStarted = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						WorkflowId:          childWorkflowID,
						WorkflowType:        &commonpb.WorkflowType{Name: "child-wf"},
						TaskQueue:           &taskqueuepb.TaskQueue{Name: childTQ, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						WorkflowTaskTimeout: durationpb.New(10 * time.Second),
					},
				},
			}}, nil
		}
		// Child has completed; complete parent.
		return s.completeWorkflowCommands(), nil
	}

	childHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return s.completeWorkflowCommands(), nil
	}

	parentPoller := s.newPoller(parentTQ, identity, parentHandler)
	childPoller := s.newPoller(childTQ, identity, childHandler)

	// Parent WFT 1: issue StartChildWorkflowExecution command.
	_, err := parentPoller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Child WFT: complete child immediately.
	_, err = childPoller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Describe child: verify time-skipping config was inherited.
	childDesc := s.describeWorkflow(&commonpb.WorkflowExecution{WorkflowId: childWorkflowID})
	tsInfo := childDesc.GetTimeSkippingInfo()
	s.NotNil(tsInfo, "child should have time-skipping info")
	s.True(tsInfo.GetConfig().GetEnabled(), "child should have time-skipping enabled")
	s.True(tsInfo.GetConfig().GetPropagateToNewChildren(), "child should inherit propagate_to_new_children")

	// Parent WFT 2: sees ChildStarted + ChildCompleted, completes parent.
	_, err = parentPoller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Verify parent completed.
	parentDesc := s.describeWorkflow(parentWE)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, parentDesc.GetWorkflowExecutionInfo().GetStatus())
}

func (s *TimeSkippingTestSuite) TestPropagateOnContinueAsNew() {
	tq := "ts-tq-" + uuid.NewString()
	identity := "worker1"

	// Start with time-skipping + propagate_on_continue_as_new.
	we := s.startWorkflowWithConfig(tq, &workflowpb.TimeSkippingConfig{
		Enabled:                  true,
		PropagateOnContinueAsNew: true,
	})

	wftCount := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wftCount++
		switch wftCount {
		case 1:
			// First run: schedule a 1h timer.
			return []*commandpb.Command{s.timerCommand("timer-1", 1*time.Hour)}, nil
		case 2:
			// First run: timer was fired; issue continue-as-new.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
					ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						WorkflowType:        &commonpb.WorkflowType{Name: "time-skipping-wf"},
						TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						WorkflowTaskTimeout: durationpb.New(10 * time.Second),
					},
				},
			}}, nil
		case 3:
			// New run: schedule a timer.
			return []*commandpb.Command{s.timerCommand("timer-2", 1*time.Hour)}, nil
		default:
			return s.completeWorkflowCommands(), nil
		}
	}

	poller := s.newPoller(tq, identity, wtHandler)

	// WFT 1: schedule timer.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Advance: fire the timer.
	advResp := s.advanceTimePoint(we)
	s.Len(advResp.GetAdvancedTimePoints(), 1)

	// WFT 2: timer fired → CAN.
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Old run should be continued-as-new.
	oldDesc := s.describeWorkflow(we)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, oldDesc.GetWorkflowExecutionInfo().GetStatus())

	// New run should have inherited time-skipping config.
	newRunWE := &commonpb.WorkflowExecution{WorkflowId: we.WorkflowId}
	newDesc := s.describeWorkflow(newRunWE)
	s.NotNil(newDesc.GetTimeSkippingInfo().GetConfig())
	s.True(newDesc.GetTimeSkippingInfo().GetConfig().GetEnabled())
	s.True(newDesc.GetTimeSkippingInfo().GetConfig().GetPropagateOnContinueAsNew())

	// WFT 3: new run schedules timer-2.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithRetries(1))
	s.NoError(err)

	// Advance the new run's timer.
	newRunAdvResp := s.advanceTimePoint(newRunWE)
	s.Len(newRunAdvResp.GetAdvancedTimePoints(), 1)

	// WFT 4: timer-2 fired → complete workflow.
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	finalDesc := s.describeWorkflow(newRunWE)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, finalDesc.GetWorkflowExecutionInfo().GetStatus())
}

// Helpers

func (s *TimeSkippingTestSuite) startWorkflowWithConfig(tq string, config *workflowpb.TimeSkippingConfig) *commonpb.WorkflowExecution {
	id := "ts-" + uuid.NewString()
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "time-skipping-wf"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            "test-worker",
		TimeSkippingConfig:  config,
	}
	resp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)
	return &commonpb.WorkflowExecution{WorkflowId: id, RunId: resp.GetRunId()}
}

func (s *TimeSkippingTestSuite) startWorkflowWithTimeSkipping(tq string) *commonpb.WorkflowExecution {
	id := "ts-" + uuid.NewString()
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "time-skipping-wf"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            "test-worker",
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	}
	resp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)
	return &commonpb.WorkflowExecution{WorkflowId: id, RunId: resp.GetRunId()}
}

func (s *TimeSkippingTestSuite) newPoller(tq, identity string, handler func(*workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error)) *testcore.TaskPoller {
	return &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: handler,
		Logger:              s.Logger,
		T:                   s.T(),
	}
}

func (s *TimeSkippingTestSuite) advanceTimePoint(we *commonpb.WorkflowExecution) *workflowservice.AdvanceWorkflowExecutionTimePointResponse {
	resp, err := s.FrontendClient().AdvanceWorkflowExecutionTimePoint(testcore.NewContext(), &workflowservice.AdvanceWorkflowExecutionTimePointRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Identity:          "test-advancer",
		RequestId:         uuid.NewString(),
	})
	s.NoError(err)
	return resp
}

func (s *TimeSkippingTestSuite) describeWorkflow(we *commonpb.WorkflowExecution) *workflowservice.DescribeWorkflowExecutionResponse {
	resp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: we,
	})
	s.NoError(err)
	return resp
}

func (s *TimeSkippingTestSuite) startWorkflowWithAutoSkip(tq string, autoSkip *workflowpb.TimeSkippingConfig_AutoSkipConfig) *commonpb.WorkflowExecution {
	id := "ts-" + uuid.NewString()
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "time-skipping-wf"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            "test-worker",
		TimeSkippingConfig: &workflowpb.TimeSkippingConfig{
			Enabled:  true,
			AutoSkip: autoSkip,
		},
	}
	resp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)
	return &commonpb.WorkflowExecution{WorkflowId: id, RunId: resp.GetRunId()}
}

func (s *TimeSkippingTestSuite) timerCommand(timerID string, duration time.Duration) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_TIMER,
		Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
			TimerId:            timerID,
			StartToFireTimeout: durationpb.New(duration),
		}},
	}
}

func (s *TimeSkippingTestSuite) completeWorkflowCommands() []*commandpb.Command {
	return []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
			Result: payloads.EncodeString("done"),
		}},
	}}
}

func (s *TimeSkippingTestSuite) updateAutoSkipConfig(we *commonpb.WorkflowExecution, autoSkip *workflowpb.TimeSkippingConfig_AutoSkipConfig) {
	_, err := s.FrontendClient().UpdateWorkflowExecutionOptions(testcore.NewContext(), &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
			TimeSkippingConfig: &workflowpb.TimeSkippingConfig{
				AutoSkip: autoSkip,
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config.auto_skip"}},
	})
	s.NoError(err)
}
