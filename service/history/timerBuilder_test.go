// Copyright (c) 2017 Uber Technologies, Inc.
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

package history

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

type (
	timerBuilderProcessorSuite struct {
		suite.Suite
		tb              *timerBuilder
		mockShard       *shardContextImpl
		mockEventsCache *MockEventsCache
		config          *Config
		logger          log.Logger
	}
)

type mockTimeSource struct {
	currTime time.Time
}

func (ts *mockTimeSource) Now() time.Time {
	return ts.currTime
}

func TestTimerBuilderSuite(t *testing.T) {
	s := new(timerBuilderProcessorSuite)
	suite.Run(t, s)
}

func (s *timerBuilderProcessorSuite) SetupSuite() {
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.config = NewDynamicConfigForTest()
}

func (s *timerBuilderProcessorSuite) SetupTest() {
	s.mockShard = &shardContextImpl{
		shardInfo:                 &persistence.ShardInfo{ShardID: 0, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		timeSource:                clock.NewRealTimeSource(),
	}
	s.mockEventsCache = &MockEventsCache{}
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
}

func (s *timerBuilderProcessorSuite) TestTimerBuilderSingleUserTimer() {
	tb := newTimerBuilder(&mockTimeSource{currTime: time.Now()})

	// Add one timer.
	msb := newMutableStateBuilder(s.mockShard, s.mockEventsCache, s.logger, testLocalDomainEntry)
	msb.Load(&persistence.WorkflowMutableState{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{NextEventID: int64(201)},
		TimerInfos:    make(map[string]*persistence.TimerInfo),
	})
	_, ti1, err := msb.AddTimerStartedEvent(int64(3), &workflow.StartTimerDecisionAttributes{
		TimerId:                   common.StringPtr("tid1"),
		StartToFireTimeoutSeconds: common.Int64Ptr(1),
	})
	s.Nil(err)

	t1 := tb.GetUserTimerTaskIfNeeded(msb)

	s.NotNil(t1)
	s.Equal(int64(201), t1.(*persistence.UserTimerTask).EventID)
	s.Equal(ti1.ExpiryTime.Unix(), t1.(*persistence.UserTimerTask).VisibilityTimestamp.Unix())

	ti, ok := msb.GetUserTimer("tid1")
	s.True(ok)
	s.NotNil(ti)
	s.Equal(int64(201), ti.StartedID)
	s.Equal(int64(1), ti.TaskID)
	s.Equal("tid1", ti.TimerID)
	s.Equal(ti1.ExpiryTime.Unix(), ti.ExpiryTime.Unix())
}

func (s *timerBuilderProcessorSuite) TestTimerBuilderMulitpleUserTimer() {
	tb := newTimerBuilder(&mockTimeSource{currTime: time.Now()})

	// Add two timers. (before and after)
	tp := &persistence.TimerInfo{TimerID: "tid1", StartedID: 201, TaskID: 101, ExpiryTime: time.Now().Add(10 * time.Second)}
	timerInfos := map[string]*persistence.TimerInfo{"tid1": tp}
	msb := newMutableStateBuilder(s.mockShard, s.mockEventsCache, s.logger, testLocalDomainEntry)
	msb.Load(&persistence.WorkflowMutableState{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{NextEventID: int64(202)},
		TimerInfos:    timerInfos,
	})

	_, tiBefore, err := msb.AddTimerStartedEvent(int64(3), &workflow.StartTimerDecisionAttributes{
		TimerId:                   common.StringPtr("tid-before"),
		StartToFireTimeoutSeconds: common.Int64Ptr(1),
	})
	s.Nil(err)

	_, _, err = msb.AddTimerStartedEvent(int64(3), &workflow.StartTimerDecisionAttributes{
		TimerId:                   common.StringPtr("tid-after"),
		StartToFireTimeoutSeconds: common.Int64Ptr(15),
	})
	s.Nil(err)

	t1 := tb.GetUserTimerTaskIfNeeded(msb)
	s.NotNil(t1)
	s.Equal(tiBefore.StartedID, t1.(*persistence.UserTimerTask).EventID)
	s.Equal(tiBefore.ExpiryTime.Unix(), t1.(*persistence.UserTimerTask).VisibilityTimestamp.Unix())

	// Mutable state with out a timer task.
	tb = newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	tp2 := &persistence.TimerInfo{TimerID: "tid1", StartedID: 201, TaskID: TimerTaskStatusNone, ExpiryTime: time.Now().Add(10 * time.Second)}
	timerInfos = map[string]*persistence.TimerInfo{"tid1": tp2}
	msb = newMutableStateBuilder(s.mockShard, s.mockEventsCache, s.logger, testLocalDomainEntry)
	msb.Load(&persistence.WorkflowMutableState{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{NextEventID: int64(203)},
		TimerInfos:    timerInfos,
	})

	_, _, err = msb.AddTimerStartedEvent(int64(3), &workflow.StartTimerDecisionAttributes{
		TimerId:                   common.StringPtr("tid-after"),
		StartToFireTimeoutSeconds: common.Int64Ptr(15),
	})
	s.Nil(err)

	t1 = tb.GetUserTimerTaskIfNeeded(msb)
	s.NotNil(t1)
	s.Equal(int64(201), t1.(*persistence.UserTimerTask).EventID)
	s.Equal(tp2.ExpiryTime.Unix(), t1.(*persistence.UserTimerTask).VisibilityTimestamp.Unix())

	ti, ok := msb.GetUserTimer("tid-after")
	s.True(ok)
	s.NotNil(ti)
	s.Equal(int64(TimerTaskStatusNone), ti.TaskID)
	s.Equal(int64(203), ti.StartedID)
}

func (s *timerBuilderProcessorSuite) TestTimerBuilderDuplicateTimerID() {
	tp := &persistence.TimerInfo{TimerID: "tid-exist", StartedID: 201, TaskID: 101, ExpiryTime: time.Now().Add(10 * time.Second)}
	timerInfos := map[string]*persistence.TimerInfo{"tid-exist": tp}
	msb := newMutableStateBuilder(s.mockShard, s.mockEventsCache, s.logger, testLocalDomainEntry)
	msb.Load(&persistence.WorkflowMutableState{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{NextEventID: int64(203)},
		TimerInfos:    timerInfos,
	})

	_, ti, err := msb.AddTimerStartedEvent(int64(3), &workflow.StartTimerDecisionAttributes{
		TimerId:                   common.StringPtr("tid-exist"),
		StartToFireTimeoutSeconds: common.Int64Ptr(15),
	})
	s.IsType(&workflow.BadRequestError{}, err)
	s.Nil(ti)
}

func (s *timerBuilderProcessorSuite) TestTimerBuilder_GetActivityTimer() {
	// ScheduleToStart being more than HB.
	builder := newMutableStateBuilder(s.mockShard, s.mockEventsCache, s.logger, testLocalDomainEntry)
	ase, ai, err := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("test-id"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(2),
			HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(3),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.Nil(err)
	// create a schedule to start timeout
	tb := newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	tt := tb.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	s.Equal(workflow.TimeoutTypeScheduleToStart, workflow.TimeoutType(tt.(*persistence.ActivityTimeoutTask).TimeoutType))

	builder.AddActivityTaskStartedEvent(ai, *ase.EventId, uuid.New(), "")

	// create a heart beat timeout
	tb = newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	tt = tb.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	s.Equal(workflow.TimeoutTypeHeartbeat, workflow.TimeoutType(tt.(*persistence.ActivityTimeoutTask).TimeoutType))
}

func (s *timerBuilderProcessorSuite) TestDecodeHistory() {
	historyString := "5b7b226576656e744964223a312c2274696d657374616d70223a313438383332353631383735333431373433312c226576656e7454797065223a22576f726b666c6f77457865637574696f6e53746172746564222c22776f726b666c6f77457865637574696f6e537461727465644576656e7441747472696275746573223a7b22776f726b666c6f7754797065223a7b226e616d65223a22696e7465726174696f6e2d73657175656e7469616c2d757365722d74696d6572732d746573742d74797065227d2c227461736b4c697374223a7b226e616d65223a22696e7465726174696f6e2d73657175656e7469616c2d757365722d74696d6572732d746573742d7461736b6c697374227d2c22657865637574696f6e5374617274546f436c6f736554696d656f75745365636f6e6473223a3130302c227461736b5374617274546f436c6f736554696d656f75745365636f6e6473223a312c226964656e74697479223a22776f726b657231227d7d2c7b226576656e744964223a322c2274696d657374616d70223a313438383332353631383735333435333137312c226576656e7454797065223a224465636973696f6e5461736b5363686564756c6564222c226465636973696f6e5461736b5363686564756c65644576656e7441747472696275746573223a7b227461736b4c697374223a7b226e616d65223a22696e7465726174696f6e2d73657175656e7469616c2d757365722d74696d6572732d746573742d7461736b6c697374227d2c227374617274546f436c6f736554696d656f75745365636f6e6473223a317d7d2c7b226576656e744964223a332c2274696d657374616d70223a313438383332353632333938383637373536302c226576656e7454797065223a224465636973696f6e5461736b53746172746564222c226465636973696f6e5461736b537461727465644576656e7441747472696275746573223a7b227363686564756c65644576656e744964223a322c226964656e74697479223a22776f726b657231222c22726571756573744964223a2235383364326164652d663363332d343862322d383366352d323936636238393931646433227d7d2c7b226576656e744964223a342c2274696d657374616d70223a313438383332353632333939373138303336362c226576656e7454797065223a224465636973696f6e5461736b436f6d706c65746564222c226465636973696f6e5461736b436f6d706c657465644576656e7441747472696275746573223a7b22657865637574696f6e436f6e74657874223a224d513d3d222c227363686564756c65644576656e744964223a322c22737461727465644576656e744964223a332c226964656e74697479223a22776f726b657231227d7d2c7b226576656e744964223a352c2274696d657374616d70223a313438383332353632333939373138343436332c226576656e7454797065223a2254696d657253746172746564222c2274696d6572537461727465644576656e7441747472696275746573223a7b2274696d65724964223a2274696d65722d69642d31222c227374617274546f4669726554696d656f75745365636f6e6473223a312c226465636973696f6e5461736b436f6d706c657465644576656e744964223a347d7d2c7b226576656e744964223a362c2274696d657374616d70223a313438383332353632343939363835383639382c226576656e7454797065223a2254696d65724669726564222c2274696d657246697265644576656e7441747472696275746573223a7b2274696d65724964223a2274696d65722d69642d31222c22737461727465644576656e744964223a357d7d2c7b226576656e744964223a372c2274696d657374616d70223a313438383332353632343939363837333438302c226576656e7454797065223a224465636973696f6e5461736b5363686564756c6564222c226465636973696f6e5461736b5363686564756c65644576656e7441747472696275746573223a7b227461736b4c697374223a7b226e616d65223a22696e7465726174696f6e2d73657175656e7469616c2d757365722d74696d6572732d746573742d7461736b6c697374227d2c227374617274546f436c6f736554696d656f75745365636f6e6473223a317d7d2c7b226576656e744964223a382c2274696d657374616d70223a313438383332353632353238313139373232312c226576656e7454797065223a224465636973696f6e5461736b53746172746564222c226465636973696f6e5461736b537461727465644576656e7441747472696275746573223a7b227363686564756c65644576656e744964223a372c226964656e74697479223a22776f726b657231222c22726571756573744964223a2233646361663661642d663639382d343436342d386363612d333366663431353838393363227d7d2c7b226576656e744964223a392c2274696d657374616d70223a313438383332353632353238343137353337372c226576656e7454797065223a224465636973696f6e5461736b436f6d706c65746564222c226465636973696f6e5461736b436f6d706c657465644576656e7441747472696275746573223a7b22657865637574696f6e436f6e74657874223a224d673d3d222c227363686564756c65644576656e744964223a372c22737461727465644576656e744964223a382c226964656e74697479223a22776f726b657231227d7d2c7b226576656e744964223a31302c2274696d657374616d70223a313438383332353632353238343137373732342c226576656e7454797065223a2254696d657253746172746564222c2274696d6572537461727465644576656e7441747472696275746573223a7b2274696d65724964223a2274696d65722d69642d32222c227374617274546f4669726554696d656f75745365636f6e6473223a312c226465636973696f6e5461736b436f6d706c657465644576656e744964223a397d7d5d"
	data, err := hex.DecodeString(historyString)
	var historyEvents []*workflow.HistoryEvent
	err = json.Unmarshal(data, &historyEvents)
	if err != nil {
		s.logger.Error("DecodeString failed with error: %+v", tag.Error(err))
		panic("Failed deserialization of history")
	}

	history := &workflow.History{}
	history.Events = historyEvents
}

func (s *timerBuilderProcessorSuite) TestDecodeKey() {
	taskID := TimerSequenceID{VisibilityTimestamp: time.Unix(0, 0), TaskID: 1}
	s.logger.Info(fmt.Sprintf("Timer: %s, expiry: %v", TimerSequenceID(taskID), taskID.VisibilityTimestamp))
}
