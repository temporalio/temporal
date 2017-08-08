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
	"os"
	"testing"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"

	"encoding/hex"

	"encoding/json"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
)

type (
	timerBuilderProcessorSuite struct {
		suite.Suite
		tb     *timerBuilder
		config *Config
		logger bark.Logger
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
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
	logger := log.New()
	//logger.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(logger)
	s.config = NewConfig(1)
	s.tb = newTimerBuilder(s.config, s.logger, &mockTimeSource{currTime: time.Now()})
}

func (s *timerBuilderProcessorSuite) TestTimerBuilderSingleUserTimer() {
	tb := newTimerBuilder(s.config, s.logger, &mockTimeSource{currTime: time.Now()})

	// Add one timer.
	msb := newMutableStateBuilder(s.config, s.logger)
	msb.Load(&persistence.WorkflowMutableState{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{NextEventID: int64(201)},
		TimerInfos:    make(map[string]*persistence.TimerInfo),
	})
	_, ti1 := msb.AddTimerStartedEvent(int64(3), &workflow.StartTimerDecisionAttributes{
		TimerId:                   common.StringPtr("tid1"),
		StartToFireTimeoutSeconds: common.Int64Ptr(1),
	})

	tb.AddUserTimer(ti1, msb)
	t1 := tb.GetUserTimerTaskIfNeeded(msb)

	s.NotNil(t1)
	s.Equal(int64(201), t1.(*persistence.UserTimerTask).EventID)
	s.Equal(ti1.ExpiryTime.Unix(), t1.(*persistence.UserTimerTask).VisibilityTimestamp.Unix())

	isRunning, ti := msb.GetUserTimer("tid1")
	s.True(isRunning)
	s.NotNil(ti)
	s.Equal(int64(201), ti.StartedID)
	s.Equal(int64(1), ti.TaskID)
	s.Equal("tid1", ti.TimerID)
	s.Equal(ti1.ExpiryTime.Unix(), ti.ExpiryTime.Unix())
}

func (s *timerBuilderProcessorSuite) TestTimerBuilderMulitpleUserTimer() {
	tb := newTimerBuilder(s.config, s.logger, &mockTimeSource{currTime: time.Now()})

	// Add two timers. (before and after)
	tp := &persistence.TimerInfo{TimerID: "tid1", StartedID: 201, TaskID: 101, ExpiryTime: time.Now().Add(10 * time.Second)}
	timerInfos := map[string]*persistence.TimerInfo{"tid1": tp}
	msb := newMutableStateBuilder(s.config, s.logger)
	msb.Load(&persistence.WorkflowMutableState{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{NextEventID: int64(202)},
		TimerInfos:    timerInfos,
	})

	_, tiBefore := msb.AddTimerStartedEvent(int64(3), &workflow.StartTimerDecisionAttributes{
		TimerId:                   common.StringPtr("tid-before"),
		StartToFireTimeoutSeconds: common.Int64Ptr(1),
	})
	tb.AddUserTimer(tiBefore, msb)

	_, tiAfter := msb.AddTimerStartedEvent(int64(3), &workflow.StartTimerDecisionAttributes{
		TimerId:                   common.StringPtr("tid-after"),
		StartToFireTimeoutSeconds: common.Int64Ptr(15),
	})
	tb.AddUserTimer(tiAfter, msb)

	t1 := tb.GetUserTimerTaskIfNeeded(msb)
	s.NotNil(t1)
	s.Equal(tiBefore.StartedID, t1.(*persistence.UserTimerTask).EventID)
	s.Equal(tiBefore.ExpiryTime.Unix(), t1.(*persistence.UserTimerTask).VisibilityTimestamp.Unix())

	// Mutable state with out a timer task.
	tb = newTimerBuilder(s.config, s.logger, &mockTimeSource{currTime: time.Now()})
	tp2 := &persistence.TimerInfo{TimerID: "tid1", StartedID: 201, TaskID: TimerTaskStatusNone, ExpiryTime: time.Now().Add(10 * time.Second)}
	timerInfos = map[string]*persistence.TimerInfo{"tid1": tp2}
	msb = newMutableStateBuilder(s.config, s.logger)
	msb.Load(&persistence.WorkflowMutableState{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{NextEventID: int64(203)},
		TimerInfos:    timerInfos,
	})

	_, ti3 := msb.AddTimerStartedEvent(int64(3), &workflow.StartTimerDecisionAttributes{
		TimerId:                   common.StringPtr("tid-after"),
		StartToFireTimeoutSeconds: common.Int64Ptr(15),
	})
	tb.AddUserTimer(ti3, msb)

	t1 = tb.GetUserTimerTaskIfNeeded(msb)
	s.NotNil(t1)
	s.Equal(int64(201), t1.(*persistence.UserTimerTask).EventID)
	s.Equal(tp2.ExpiryTime.Unix(), t1.(*persistence.UserTimerTask).VisibilityTimestamp.Unix())

	isRunning, ti := msb.GetUserTimer("tid-after")
	s.True(isRunning)
	s.NotNil(ti)
	s.Equal(int64(TimerTaskStatusNone), ti.TaskID)
	s.Equal(int64(203), ti.StartedID)
}

func (s *timerBuilderProcessorSuite) TestTimerBuilderDuplicateTimerID() {
	tp := &persistence.TimerInfo{TimerID: "tid-exist", StartedID: 201, TaskID: 101, ExpiryTime: time.Now().Add(10 * time.Second)}
	timerInfos := map[string]*persistence.TimerInfo{"tid-exist": tp}
	msb := newMutableStateBuilder(s.config, s.logger)
	msb.Load(&persistence.WorkflowMutableState{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{NextEventID: int64(203)},
		TimerInfos:    timerInfos,
	})

	_, ti := msb.AddTimerStartedEvent(int64(3), &workflow.StartTimerDecisionAttributes{
		TimerId:                   common.StringPtr("tid-exist"),
		StartToFireTimeoutSeconds: common.Int64Ptr(15),
	})

	s.Nil(ti)
}

func (s *timerBuilderProcessorSuite) TestTimerBuilder_GetActivityTimer() {
	// ScheduleToStart being more than HB.
	builder := newMutableStateBuilder(s.config, s.logger)
	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(2),
			HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
		})
	// create a schedule to start timeout
	tb := newTimerBuilder(s.config, s.logger, &mockTimeSource{currTime: time.Now()})
	tt := tb.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	s.Equal(workflow.TimeoutType_SCHEDULE_TO_START, workflow.TimeoutType(tt.(*persistence.ActivityTimeoutTask).TimeoutType))

	builder.AddActivityTaskStartedEvent(ai, ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	// create a heart beat timeout
	tb = newTimerBuilder(s.config, s.logger, &mockTimeSource{currTime: time.Now()})
	tt = tb.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	s.Equal(workflow.TimeoutType_HEARTBEAT, workflow.TimeoutType(tt.(*persistence.ActivityTimeoutTask).TimeoutType))
}

func (s *timerBuilderProcessorSuite) TestDecodeHistory() {
	historyString := "5b7b226576656e744964223a312c2274696d657374616d70223a313438383332353631383735333431373433312c226576656e7454797065223a22576f726b666c6f77457865637574696f6e53746172746564222c22776f726b666c6f77457865637574696f6e537461727465644576656e7441747472696275746573223a7b22776f726b666c6f7754797065223a7b226e616d65223a22696e7465726174696f6e2d73657175656e7469616c2d757365722d74696d6572732d746573742d74797065227d2c227461736b4c697374223a7b226e616d65223a22696e7465726174696f6e2d73657175656e7469616c2d757365722d74696d6572732d746573742d7461736b6c697374227d2c22657865637574696f6e5374617274546f436c6f736554696d656f75745365636f6e6473223a3130302c227461736b5374617274546f436c6f736554696d656f75745365636f6e6473223a312c226964656e74697479223a22776f726b657231227d7d2c7b226576656e744964223a322c2274696d657374616d70223a313438383332353631383735333435333137312c226576656e7454797065223a224465636973696f6e5461736b5363686564756c6564222c226465636973696f6e5461736b5363686564756c65644576656e7441747472696275746573223a7b227461736b4c697374223a7b226e616d65223a22696e7465726174696f6e2d73657175656e7469616c2d757365722d74696d6572732d746573742d7461736b6c697374227d2c227374617274546f436c6f736554696d656f75745365636f6e6473223a317d7d2c7b226576656e744964223a332c2274696d657374616d70223a313438383332353632333938383637373536302c226576656e7454797065223a224465636973696f6e5461736b53746172746564222c226465636973696f6e5461736b537461727465644576656e7441747472696275746573223a7b227363686564756c65644576656e744964223a322c226964656e74697479223a22776f726b657231222c22726571756573744964223a2235383364326164652d663363332d343862322d383366352d323936636238393931646433227d7d2c7b226576656e744964223a342c2274696d657374616d70223a313438383332353632333939373138303336362c226576656e7454797065223a224465636973696f6e5461736b436f6d706c65746564222c226465636973696f6e5461736b436f6d706c657465644576656e7441747472696275746573223a7b22657865637574696f6e436f6e74657874223a224d513d3d222c227363686564756c65644576656e744964223a322c22737461727465644576656e744964223a332c226964656e74697479223a22776f726b657231227d7d2c7b226576656e744964223a352c2274696d657374616d70223a313438383332353632333939373138343436332c226576656e7454797065223a2254696d657253746172746564222c2274696d6572537461727465644576656e7441747472696275746573223a7b2274696d65724964223a2274696d65722d69642d31222c227374617274546f4669726554696d656f75745365636f6e6473223a312c226465636973696f6e5461736b436f6d706c657465644576656e744964223a347d7d2c7b226576656e744964223a362c2274696d657374616d70223a313438383332353632343939363835383639382c226576656e7454797065223a2254696d65724669726564222c2274696d657246697265644576656e7441747472696275746573223a7b2274696d65724964223a2274696d65722d69642d31222c22737461727465644576656e744964223a357d7d2c7b226576656e744964223a372c2274696d657374616d70223a313438383332353632343939363837333438302c226576656e7454797065223a224465636973696f6e5461736b5363686564756c6564222c226465636973696f6e5461736b5363686564756c65644576656e7441747472696275746573223a7b227461736b4c697374223a7b226e616d65223a22696e7465726174696f6e2d73657175656e7469616c2d757365722d74696d6572732d746573742d7461736b6c697374227d2c227374617274546f436c6f736554696d656f75745365636f6e6473223a317d7d2c7b226576656e744964223a382c2274696d657374616d70223a313438383332353632353238313139373232312c226576656e7454797065223a224465636973696f6e5461736b53746172746564222c226465636973696f6e5461736b537461727465644576656e7441747472696275746573223a7b227363686564756c65644576656e744964223a372c226964656e74697479223a22776f726b657231222c22726571756573744964223a2233646361663661642d663639382d343436342d386363612d333366663431353838393363227d7d2c7b226576656e744964223a392c2274696d657374616d70223a313438383332353632353238343137353337372c226576656e7454797065223a224465636973696f6e5461736b436f6d706c65746564222c226465636973696f6e5461736b436f6d706c657465644576656e7441747472696275746573223a7b22657865637574696f6e436f6e74657874223a224d673d3d222c227363686564756c65644576656e744964223a372c22737461727465644576656e744964223a382c226964656e74697479223a22776f726b657231227d7d2c7b226576656e744964223a31302c2274696d657374616d70223a313438383332353632353238343137373732342c226576656e7454797065223a2254696d657253746172746564222c2274696d6572537461727465644576656e7441747472696275746573223a7b2274696d65724964223a2274696d65722d69642d32222c227374617274546f4669726554696d656f75745365636f6e6473223a312c226465636973696f6e5461736b436f6d706c657465644576656e744964223a397d7d5d"
	data, err := hex.DecodeString(historyString)
	var historyEvents []*workflow.HistoryEvent
	err = json.Unmarshal(data, &historyEvents)
	if err != nil {
		s.logger.Errorf("DecodeString failed with error: %+v", err)
		panic("Failed deserialization of history")
	}

	history := workflow.NewHistory()
	history.Events = historyEvents

	common.PrettyPrintHistory(history, s.logger)
}

func (s *timerBuilderProcessorSuite) TestDecodeKey() {
	taskID := SequenceID{VisibilityTimestamp: time.Unix(0, 0), TaskID: 1}
	s.logger.Infof("Timer: %s, expiry: %v", SequenceID(taskID), taskID.VisibilityTimestamp)
}
