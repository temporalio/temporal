// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package workflow

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	commonclock "go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	BackoffIntervalCalculator = func(
		time.Time,
		int32,
		int32,
		*durationpb.Duration,
		*durationpb.Duration,
		*timestamppb.Timestamp,
		float64,
		*failurepb.Failure,
		[]string,
	) (time.Duration, enumspb.RetryState)

	snapshot struct {
		mutableStateApproximateSize int
		activitySize                int
	}

	retrySuite struct {
		suite.Suite

		controller       *gomock.Controller
		mockConfig       *configs.Config
		mockShard        *shard.ContextTest
		mockEventsCache  *events.MockCache
		onActivityCreate *snapshot

		mutableState               *MutableStateImpl
		logger                     log.Logger
		testScope                  tally.TestScope
		originalGetBackoffInterval BackoffIntervalCalculator
		backoffCalculatorStub      getBackoffIntervalStub
		activity                   *persistencespb.ActivityInfo
		failure                    *failurepb.Failure
		timeSource                 TimeSourceStub
	}
)

func TestMutableStateRetryActivitySuite(t *testing.T) {
	s := new(retrySuite)

	suite.Run(t, s)
}

func (s *retrySuite) SetupSuite() {
	s.mockConfig = tests.NewDynamicConfig()
	// set the checksum probabilities to 100% for exercising during test
	s.mockConfig.MutableStateChecksumGenProbability = func(namespace string) int { return 100 }
	s.mockConfig.MutableStateChecksumVerifyProbability = func(namespace string) int { return 100 }
	s.mockConfig.MutableStateActivityFailureSizeLimitWarn = func(namespace string) int { return 1 * 1024 }
	s.mockConfig.MutableStateActivityFailureSizeLimitError = func(namespace string) int { return 2 * 1024 }

	s.originalGetBackoffInterval = getBackoffInterval
	getBackoffInterval = s.backoffCalculatorStub.getBackoffInterval
}

func (s *retrySuite) TearDownSuite() {
	getBackoffInterval = s.originalGetBackoffInterval
}

func (s *retrySuite) SetupTest() {

	s.controller = gomock.NewController(s.T())
	s.mockEventsCache = events.NewMockCache(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		s.mockConfig,
	)
	s.mockShard.SetEventsCacheForTesting(s.mockEventsCache)

	s.testScope = s.mockShard.Resource.MetricsScope.(tally.TestScope)
	s.logger = s.mockShard.GetLogger()

	s.mutableState = NewMutableState(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, time.Now().UTC())
	s.activity = s.makeActivityAndPutIntoFailingState()
	s.failure = s.activityFailure()
	s.backoffCalculatorStub.onNextCallExpect(
		s.timeSource.Now(),
		s.activity.Attempt,
		s.activity.RetryMaximumAttempts,
		s.activity.RetryInitialInterval,
		s.activity.RetryMaximumInterval,
		s.activity.RetryExpirationTime,
		s.activity.RetryBackoffCoefficient,
		s.failure,
		s.activity.RetryNonRetryableErrorTypes,
	)
}

func (s *retrySuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *retrySuite) TestRetryActivity_when_activity_has_no_retry_policy_should_fail() {
	s.activity.HasRetryPolicy = false
	s.onActivityCreate.activitySize = s.activity.Size()

	state, err := s.mutableState.RetryActivity(s.activity, s.activityFailure())

	s.NoError(err, "activity which has no retry policy should not be retried but it failed")
	s.Equal(enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET, state)
	s.assertActivityWasNotScheduled(s.activity, "with no retry policy")
	s.assertNoChange(s.activity, "activity should not change if it is not restarted")
}

func (s *retrySuite) TestRetryActivity_when_activity_has_pending_cancel_request_should_fail() {
	s.activity.CancelRequested = true
	s.onActivityCreate.activitySize = s.activity.Size()

	state, err := s.mutableState.RetryActivity(s.activity, s.activityFailure())

	s.NoError(err, "activity which has no retry policy should not be retried but it failed")
	s.Equal(enumspb.RETRY_STATE_CANCEL_REQUESTED, state)
	s.assertActivityWasNotScheduled(s.activity, "with pending cancellation")
	s.assertNoChange(s.activity, "activity should not change if it is not restarted")
}

func (s *retrySuite) TestRetryActivity_should_be_scheduled_when_next_backoff_interval_can_be_calculated() {
	taskGeneratorMock := NewMockTaskGenerator(s.controller)
	taskGeneratorMock.EXPECT().GenerateActivityRetryTasks(s.activity.ScheduledEventId)
	s.mutableState.taskGenerator = taskGeneratorMock

	ts := TimeSourceStub{}
	s.mutableState.timeSource = ts

	s.backoffCalculatorStub.onNextCallReturn(time.Second, enumspb.RETRY_STATE_IN_PROGRESS)
	_, err := s.mutableState.RetryActivity(s.activity, s.activityFailure())
	s.NoError(err)
	s.Equal(s.onActivityCreate.mutableStateApproximateSize-s.onActivityCreate.activitySize+s.activity.Size(), s.mutableState.approximateSize)
	s.Equal(s.activity.Version, s.mutableState.currentVersion)

	s.Equal(ts.Now().Add(1*time.Second).UTC(), s.activity.ScheduledTime.AsTime(), "Activity scheduled time is incorrect")
	s.Equal(s.backoffCalculatorStub.expected, s.backoffCalculatorStub.recorded)
	s.assertTruncateFailureCalled()
}

func (s *retrySuite) TestRetryActivity_should_fail_when_no_next_backoff_interval() {
	taskGeneratorMock := NewMockTaskGenerator(s.controller)
	s.mutableState.taskGenerator = taskGeneratorMock

	s.backoffCalculatorStub.onNextCallReturn(backoff.NoBackoff, enumspb.RETRY_STATE_TIMEOUT)

	state, err := s.mutableState.RetryActivity(s.activity, s.activityFailure())
	s.NoError(err)
	s.Equal(enumspb.RETRY_STATE_TIMEOUT, state)
	s.assertActivityWasNotScheduled(s.activity, "which retries for too long")
	s.assertNoChange(s.activity, "activity should not change if it is not restarted")
}

func (s *retrySuite) TestRetryActivity_should_fail_when_task_can_not_be_generated() {
	e := errors.New("can't generate task")
	taskGeneratorMock := NewMockTaskGenerator(s.controller)
	taskGeneratorMock.EXPECT().GenerateActivityRetryTasks(s.activity.ScheduledEventId).Return(e)
	s.mutableState.taskGenerator = taskGeneratorMock

	ts := TimeSourceStub{}
	s.mutableState.timeSource = ts

	s.backoffCalculatorStub.onNextCallReturn(time.Second, enumspb.RETRY_STATE_IN_PROGRESS)
	state, err := s.mutableState.RetryActivity(s.activity, s.activityFailure())
	s.Error(err, e.Error())
	s.Equal(
		enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR,
		state,
		"failure to generate task should produce RETRY_STATE_INTERNAL_SERVER_ERROR got %v",
		state,
	)
	s.assertActivityWasNotScheduled(s.activity, "with failing task generator")
}

func (s *retrySuite) TestRetryActivity_when_workflow_is_not_mutable_should_fail() {
	s.mutableState.executionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED

	state, err := s.mutableState.RetryActivity(s.activity, s.activityFailure())

	s.Error(ErrWorkflowFinished, err.Error(), "when workflow finished should get error stating it")
	s.Equal(enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR, state)
	s.assertActivityWasNotScheduled(s.activity, "for non-mutable workflow")
	s.assertNoChange(s.activity, "activity should not change if it is not restarted")
}

const getBackoffIntervalParametersFormat = "now(%v):currentAttempt(%v):maxAttempts(%v):initInterval(%v):maxInterval(%v):expirationTime(%v):backoffCoefficient(%v):failure(%v):nonRetryable(%v)"

type getBackoffIntervalStub struct {
	expected string
	recorded string
	duration time.Duration
	state    enumspb.RetryState
}

func (nbis *getBackoffIntervalStub) getBackoffInterval(
	now time.Time,
	currentAttempt int32,
	maxAttempts int32,
	initInterval *durationpb.Duration,
	maxInterval *durationpb.Duration,
	expirationTime *timestamppb.Timestamp,
	backoffCoefficient float64,
	failure *failurepb.Failure,
	nonRetryableTypes []string,
) (time.Duration, enumspb.RetryState) {
	nbis.recorded = fmt.Sprintf(
		getBackoffIntervalParametersFormat,
		now,
		currentAttempt,
		maxAttempts,
		initInterval,
		maxInterval,
		expirationTime,
		backoffCoefficient,
		failure,
		nonRetryableTypes)
	return nbis.duration, nbis.state
}

func (nbis *getBackoffIntervalStub) onNextCallExpect(
	now time.Time,
	currentAttempt int32,
	maxAttempts int32,
	initInterval *durationpb.Duration,
	maxInterval *durationpb.Duration,
	expirationTime *timestamppb.Timestamp,
	backoffCoefficient float64,
	failure *failurepb.Failure,
	nonRetryableTypes []string,
) {
	nbis.expected = fmt.Sprintf(
		getBackoffIntervalParametersFormat,
		now,
		currentAttempt,
		maxAttempts,
		initInterval,
		maxInterval,
		expirationTime,
		backoffCoefficient,
		failure,
		nonRetryableTypes,
	)
}

func (nbis *getBackoffIntervalStub) onNextCallReturn(duration time.Duration, state enumspb.RetryState) {
	nbis.duration = duration
	nbis.state = state
}

type TimeSourceStub struct {
}

func (ts TimeSourceStub) Now() time.Time {
	const layout = "Jan 2, 2006 at 3:04pm (MST)"
	const timeString = "Nov 30, 2023 at 12:07pm (GMT)"
	result, _ := time.Parse(layout, timeString)
	return result
}

func (ts TimeSourceStub) AfterFunc(_ time.Duration, _ func()) commonclock.Timer {
	return nil
}

func (s *retrySuite) assertActivityWasNotScheduled(ai *persistencespb.ActivityInfo, kind string) {
	s.T().Helper()
	s.Equal(s.onActivityCreate.mutableStateApproximateSize, s.mutableState.approximateSize, "mutable state size should not change when activity not restarted")
	s.NotContains(s.mutableState.syncActivityTasks, ai.ScheduledEventId, "activity %s was scheduled", kind)
	s.NotContains(s.mutableState.updateActivityInfos, ai.ScheduledEventId, "activity with no restart policy was marked for update")
}

func (s *retrySuite) assertNoChange(ai *persistencespb.ActivityInfo, msg string) {
	s.T().Helper()
	s.Equal(s.onActivityCreate.activitySize, ai.Size(), msg)
}

func (s *retrySuite) assertTruncateFailureCalled() {
	s.T().Helper()
	s.IsType(&failurepb.Failure{}, s.failure, "original failure should be of type Failure")
	s.IsType(&failurepb.Failure_ServerFailureInfo{}, s.activity.RetryLastFailure.FailureInfo, "after truncation failure should be of type Failure_ServerFailureInfo")
}

func (s *retrySuite) makeActivityAndPutIntoFailingState() *persistencespb.ActivityInfo {
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	workflowTaskCompletedEventID := int64(4)
	_, activityInfo, err := s.mutableState.AddActivityTaskScheduledEvent(
		workflowTaskCompletedEventID,
		&commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:   "5",
			ActivityType: &commonpb.ActivityType{Name: "activity-type"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "task-queue"},
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: timestamp.DurationFromSeconds(1),
			},
		},
		false,
	)
	s.NoError(err)

	_, err = s.mutableState.AddActivityTaskStartedEvent(
		activityInfo,
		activityInfo.ScheduledEventId,
		uuid.New(),
		"worker-identity",
	)
	s.NoError(err)

	delete(s.mutableState.syncActivityTasks, activityInfo.ScheduledEventId)
	delete(s.mutableState.updateActivityInfos, activityInfo.ScheduledEventId)
	s.onActivityCreate = &snapshot{
		mutableStateApproximateSize: s.mutableState.approximateSize,
		activitySize:                activityInfo.Size(),
	}
	return activityInfo
}

func (s *retrySuite) activityFailure() *failurepb.Failure {
	failureSizeErrorLimit := s.mockConfig.MutableStateActivityFailureSizeLimitError(
		s.mutableState.namespaceEntry.Name().String(),
	)

	failure := &failurepb.Failure{
		Message: "activity failure with large details",
		Source:  "application",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         "application-failure-type",
			NonRetryable: false,
			Details: &commonpb.Payloads{
				Payloads: []*commonpb.Payload{
					{
						Data: make([]byte, failureSizeErrorLimit*2),
					},
				},
			},
		}},
	}
	s.Greater(failure.Size(), failureSizeErrorLimit)
	return failure
}
