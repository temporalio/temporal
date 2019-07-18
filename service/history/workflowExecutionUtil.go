// Copyright (c) 2019 Uber Technologies, Inc.
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
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
)

func failDecision(
	mutableState mutableState,
	di *decisionInfo,
	decisionFailureCause workflow.DecisionTaskFailedCause,
) error {

	_, err := mutableState.AddDecisionTaskFailedEvent(
		di.ScheduleID,
		di.StartedID,
		decisionFailureCause,
		nil,
		identityHistoryService,
		"",
		"",
		"",
		0,
	)
	return err
}

func scheduleDecision(
	mutableState mutableState,
	timeSource clock.TimeSource,
	logger log.Logger,
) error {

	if mutableState.HasPendingDecisionTask() {
		return nil
	}

	di, err := mutableState.AddDecisionTaskScheduledEvent()
	if err != nil {
		return &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
	}

	executionInfo := mutableState.GetExecutionInfo()
	transferTask := &persistence.DecisionTask{
		DomainID:   executionInfo.DomainID,
		TaskList:   di.TaskList,
		ScheduleID: di.ScheduleID,
	}
	mutableState.AddTransferTasks(transferTask)

	if mutableState.IsStickyTaskListEnabled() {
		tBuilder := newTimerBuilder(logger, timeSource)
		timerTask := tBuilder.AddScheduleToStartDecisionTimoutTask(
			di.ScheduleID,
			di.Attempt,
			executionInfo.StickyScheduleToStartTimeout,
		)
		mutableState.AddTimerTasks(timerTask)
	}

	return nil
}
