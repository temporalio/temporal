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

package xdc

import (
	"fmt"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/.gen/go/shared"
	"testing"
)

var (
	notPendingDecisionTask = func(pastEvents []Vertex) bool {
		count := 0
		for _, e := range pastEvents {
			switch {
			case e.GetName() == shared.EventTypeDecisionTaskScheduled.String():
				count++
			case e.GetName() == shared.EventTypeDecisionTaskCompleted.String(),
				e.GetName() == shared.EventTypeDecisionTaskFailed.String(),
				e.GetName() == shared.EventTypeDecisionTaskTimedOut.String():
				count--
			}
		}
		return count <= 0
	}

	containActivityComplete = func(pastEvents []Vertex) bool {
		for _, e := range pastEvents {
			if e.GetName() == shared.EventTypeActivityTaskCompleted.String() {
				return true
			}
		}
		return false
	}
)

type (
	historyEventTestSuit struct {
		suite.Suite
		generator Generator
	}
)

func TestHistoryEventTestSuite(t *testing.T) {
	suite.Run(t, new(historyEventTestSuit))
}

func (s *historyEventTestSuit) SetupSuite() {
	//Setup decision task model
	decisionModel := NewHistoryEventModel()
	decisionSchedule := NewHistoryEvent(shared.EventTypeDecisionTaskScheduled.String())
	decisionStart := NewHistoryEvent(shared.EventTypeDecisionTaskStarted.String())
	decisionStart.SetIsStrictOnNextVertex(true)
	decisionFail := NewHistoryEvent(shared.EventTypeDecisionTaskFailed.String())
	decisionTimedOut := NewHistoryEvent(shared.EventTypeDecisionTaskTimedOut.String())
	decisionComplete := NewHistoryEvent(shared.EventTypeDecisionTaskCompleted.String())
	decisionComplete.SetIsStrictOnNextVertex(true)
	decisionComplete.SetMaxNextVertex(2)
	decisionScheduleToStart := NewConnection(decisionSchedule, decisionStart)
	decisionStartToComplete := NewConnection(decisionStart, decisionComplete)
	decisionStartToFail := NewConnection(decisionStart, decisionFail)
	decisionStartToTimedOut := NewConnection(decisionStart, decisionTimedOut)
	decisionFailToSchedule := NewConnection(decisionFail, decisionSchedule)
	decisionFailToSchedule.SetCondition(notPendingDecisionTask)
	decisionTimedOutToSchedule := NewConnection(decisionTimedOut, decisionSchedule)
	decisionTimedOutToSchedule.SetCondition(notPendingDecisionTask)
	decisionModel.AddEdge(decisionScheduleToStart, decisionStartToComplete, decisionStartToFail, decisionStartToTimedOut,
		decisionFailToSchedule, decisionTimedOutToSchedule)

	//Setup workflow model
	workflowModel := NewHistoryEventModel()
	workflowStart := NewHistoryEvent(shared.EventTypeWorkflowExecutionStarted.String())
	workflowSignal := NewHistoryEvent(shared.EventTypeWorkflowExecutionSignaled.String())
	workflowComplete := NewHistoryEvent(shared.EventTypeWorkflowExecutionCompleted.String())
	continueAsNew := NewHistoryEvent(shared.EventTypeWorkflowExecutionContinuedAsNew.String())
	workflowFail := NewHistoryEvent(shared.EventTypeWorkflowExecutionFailed.String())
	workflowCancel := NewHistoryEvent(shared.EventTypeWorkflowExecutionCanceled.String())
	workflowCancelRequest := NewHistoryEvent(shared.EventTypeWorkflowExecutionCancelRequested.String()) //?
	workflowTerminate := NewHistoryEvent(shared.EventTypeWorkflowExecutionTerminated.String())
	workflowTimedOut := NewHistoryEvent(shared.EventTypeWorkflowExecutionTimedOut.String())
	workflowStartToSignal := NewConnection(workflowStart, workflowSignal)
	workflowStartToDecisionSchedule := NewConnection(workflowStart, decisionSchedule)
	workflowStartToDecisionSchedule.SetCondition(notPendingDecisionTask)
	workflowSignalToDecisionSchedule := NewConnection(workflowSignal, decisionSchedule)
	workflowSignalToDecisionSchedule.SetCondition(notPendingDecisionTask)
	decisionCompleteToWorkflowComplete := NewConnection(decisionComplete, workflowComplete)
	decisionCompleteToWorkflowComplete.SetCondition(containActivityComplete)
	decisionCompleteToWorkflowFailed := NewConnection(decisionComplete, workflowFail)
	decisionCompleteToWorkflowFailed.SetCondition(containActivityComplete)
	decisionCompleteToCAN := NewConnection(decisionComplete, continueAsNew)
	decisionCompleteToCAN.SetCondition(containActivityComplete)
	workflowCancelRequestToCancel := NewConnection(workflowCancelRequest, workflowCancel)
	workflowModel.AddEdge(workflowStartToSignal, workflowStartToDecisionSchedule, workflowSignalToDecisionSchedule,
		decisionCompleteToCAN, decisionCompleteToWorkflowComplete, decisionCompleteToWorkflowFailed, workflowCancelRequestToCancel)

	//Setup activity model
	activityModel := NewHistoryEventModel()
	activitySchedule := NewHistoryEvent(shared.EventTypeActivityTaskScheduled.String())
	activityStart := NewHistoryEvent(shared.EventTypeActivityTaskStarted.String())
	activityComplete := NewHistoryEvent(shared.EventTypeActivityTaskCompleted.String())
	activityFail := NewHistoryEvent(shared.EventTypeActivityTaskFailed.String())
	activityTimedOut := NewHistoryEvent(shared.EventTypeActivityTaskTimedOut.String())
	activityCancelRequest := NewHistoryEvent(shared.EventTypeActivityTaskCancelRequested.String()) //?
	activityCancel := NewHistoryEvent(shared.EventTypeActivityTaskCanceled.String())
	activityCancelRequestFail := NewHistoryEvent(shared.EventTypeRequestCancelActivityTaskFailed.String())
	decisionCompleteToATSchedule := NewConnection(decisionComplete, activitySchedule)
	activityScheduleToStart := NewConnection(activitySchedule, activityStart)
	activityStartToComplete := NewConnection(activityStart, activityComplete)
	activityStartToFail := NewConnection(activityStart, activityFail)
	activityStartToTimedOut := NewConnection(activityStart, activityTimedOut)
	activityCompleteToDecisionSchedule := NewConnection(activityComplete, decisionSchedule)
	activityCompleteToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityFailToDecisionSchedule := NewConnection(activityFail, decisionSchedule)
	activityFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityTimedOutToDecisionSchedule := NewConnection(activityTimedOut, decisionSchedule)
	activityTimedOutToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityCancelReqToCancel := NewConnection(activityCancelRequest, activityCancel)
	activityCancelReqToCancelFail := NewConnection(activityCancelRequest, activityCancelRequestFail)
	activityCancelToDecisionSchedule := NewConnection(activityCancel, decisionSchedule)
	activityCancelToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityCancelRequestFailToDecisionSchedule := NewConnection(activityCancelRequestFail, decisionSchedule)
	activityCancelRequestFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityStartToCancalReq := NewConnection(activityStart, activityCancelRequest)
	activityModel.AddEdge(decisionCompleteToATSchedule, activityScheduleToStart, activityStartToComplete,
		activityStartToFail, activityStartToTimedOut, decisionCompleteToATSchedule, activityCompleteToDecisionSchedule,
		activityFailToDecisionSchedule, activityTimedOutToDecisionSchedule, activityCancelReqToCancel, activityCancelReqToCancelFail,
		activityCancelToDecisionSchedule, activityStartToCancalReq, activityCancelRequestFailToDecisionSchedule)

	//Setup timer model
	timerModel := NewHistoryEventModel()
	timerStart := NewHistoryEvent(shared.EventTypeTimerStarted.String())
	timerFired := NewHistoryEvent(shared.EventTypeTimerFired.String())
	timerCancel := NewHistoryEvent(shared.EventTypeTimerCanceled.String())
	timerStartToFire := NewConnection(timerStart, timerFired)
	timerStartToCancel := NewConnection(timerStart, timerCancel)
	decisionCompleteToTimerStart := NewConnection(decisionComplete, timerStart)
	timerFiredToDecisionSchedule := NewConnection(timerFired, decisionSchedule)
	timerFiredToDecisionSchedule.SetCondition(notPendingDecisionTask)
	timerCancelToDecisionSchedule := NewConnection(timerCancel, decisionSchedule)
	timerCancelToDecisionSchedule.SetCondition(notPendingDecisionTask)
	timerModel.AddEdge(timerStartToFire, timerStartToCancel, decisionCompleteToTimerStart, timerFiredToDecisionSchedule, timerCancelToDecisionSchedule)

	//Setup child workflow model
	childWorkflowModel := NewHistoryEventModel()
	childWorkflowInitial := NewHistoryEvent(shared.EventTypeStartChildWorkflowExecutionInitiated.String())
	childWorkflowInitialFail := NewHistoryEvent(shared.EventTypeStartChildWorkflowExecutionFailed.String())
	childWorkflowStart := NewHistoryEvent(shared.EventTypeChildWorkflowExecutionStarted.String())
	childWorkflowCancel := NewHistoryEvent(shared.EventTypeChildWorkflowExecutionCanceled.String())
	childWorkflowComplete := NewHistoryEvent(shared.EventTypeChildWorkflowExecutionCompleted.String())
	childWorkflowFail := NewHistoryEvent(shared.EventTypeChildWorkflowExecutionFailed.String())
	childWorkflowTerminate := NewHistoryEvent(shared.EventTypeChildWorkflowExecutionTerminated.String())
	childWorkflowTimedOut := NewHistoryEvent(shared.EventTypeChildWorkflowExecutionTimedOut.String())
	decisionCompleteToChildWorkflowInitial := NewConnection(decisionComplete, childWorkflowInitial)
	childWorkflowInitialToFail := NewConnection(childWorkflowInitial, childWorkflowInitialFail)
	childWorkflowInitialToStart := NewConnection(childWorkflowInitial, childWorkflowStart)
	childWorkflowStartToCancel := NewConnection(childWorkflowStart, childWorkflowCancel)
	childWorkflowStartToFail := NewConnection(childWorkflowStart, childWorkflowFail)
	childWorkflowStartToComplete := NewConnection(childWorkflowStart, childWorkflowComplete)
	childWorkflowStartToTerminate := NewConnection(childWorkflowStart, childWorkflowTerminate)
	childWorkflowStartToTimedOut := NewConnection(childWorkflowStart, childWorkflowTimedOut)
	childWorkflowCancelToDecisionSchedule := NewConnection(childWorkflowCancel, decisionSchedule)
	childWorkflowCancelToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowFailToDecisionSchedule := NewConnection(childWorkflowFail, decisionSchedule)
	childWorkflowFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowCompleteToDecisionSchedule := NewConnection(childWorkflowComplete, decisionSchedule)
	childWorkflowCompleteToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowTerminateToDecisionSchedule := NewConnection(childWorkflowTerminate, decisionSchedule)
	childWorkflowTerminateToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowTimedOutToDecisionSchedule := NewConnection(childWorkflowTimedOut, decisionSchedule)
	childWorkflowTimedOutToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowInitialFailToDecisionSchedule := NewConnection(childWorkflowInitialFail, decisionSchedule)
	childWorkflowInitialFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowModel.AddEdge(decisionCompleteToChildWorkflowInitial, childWorkflowInitialToFail, childWorkflowInitialToStart,
		childWorkflowStartToCancel, childWorkflowStartToFail, childWorkflowStartToComplete, childWorkflowStartToTerminate,
		childWorkflowStartToTimedOut, childWorkflowCancelToDecisionSchedule, childWorkflowFailToDecisionSchedule,
		childWorkflowCompleteToDecisionSchedule, childWorkflowTerminateToDecisionSchedule, childWorkflowTimedOutToDecisionSchedule,
		childWorkflowInitialFailToDecisionSchedule)

	//Setup external workflow model
	externalWorkflowModel := NewHistoryEventModel()
	externalWorkflowSignal := NewHistoryEvent(shared.EventTypeSignalExternalWorkflowExecutionInitiated.String())
	externalWorkflowSignalFailed := NewHistoryEvent(shared.EventTypeSignalExternalWorkflowExecutionFailed.String())
	externalWorkflowSignaled := NewHistoryEvent(shared.EventTypeExternalWorkflowExecutionSignaled.String())
	externalWorkflowCancel := NewHistoryEvent(shared.EventTypeRequestCancelExternalWorkflowExecutionInitiated.String())
	externalWorkflowCancelFail := NewHistoryEvent(shared.EventTypeRequestCancelExternalWorkflowExecutionFailed.String())
	externalWorkflowCanceled := NewHistoryEvent(shared.EventTypeExternalWorkflowExecutionCancelRequested.String())
	decisionCompleteToExternalWorkflowSignal := NewConnection(decisionComplete, externalWorkflowSignal)
	decisionCompleteToExternalWorkflowCancel := NewConnection(decisionComplete, externalWorkflowCancel)
	externalWorkflowSignalToFail := NewConnection(externalWorkflowSignal, externalWorkflowSignalFailed)
	externalWorkflowSignalToSignaled := NewConnection(externalWorkflowSignal, externalWorkflowSignaled)
	externalWorkflowCancelToFail := NewConnection(externalWorkflowCancel, externalWorkflowCancelFail)
	externalWorkflowCancelToCanceled := NewConnection(externalWorkflowCancel, externalWorkflowCanceled)
	externalWorkflowSignaledToDecisionSchedule := NewConnection(externalWorkflowSignaled, decisionSchedule)
	externalWorkflowSignaledToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowSignalFailedToDecisionSchedule := NewConnection(externalWorkflowSignalFailed, decisionSchedule)
	externalWorkflowSignalFailedToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowCanceledToDecisionSchedule := NewConnection(externalWorkflowCanceled, decisionSchedule)
	externalWorkflowCanceledToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowCancelFailToDecisionSchedule := NewConnection(externalWorkflowCancelFail, decisionSchedule)
	externalWorkflowCancelFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowModel.AddEdge(decisionCompleteToExternalWorkflowSignal, decisionCompleteToExternalWorkflowCancel,
		externalWorkflowSignalToFail, externalWorkflowSignalToSignaled, externalWorkflowCancelToFail, externalWorkflowCancelToCanceled,
		externalWorkflowSignaledToDecisionSchedule, externalWorkflowSignalFailedToDecisionSchedule,
		externalWorkflowCanceledToDecisionSchedule, externalWorkflowCancelFailToDecisionSchedule)

	//Initial event generator
	generator := NewEventGenerator()
	generator.AddInitialEntryVertex(workflowStart)
	generator.AddExitVertex(workflowComplete, workflowFail, continueAsNew, workflowTerminate, workflowTimedOut)
	generator.AddRandomEntryVertex(workflowSignal, workflowTerminate, workflowTimedOut)
	generator.AddModel(decisionModel)
	generator.AddModel(workflowModel)
	generator.AddModel(activityModel)
	generator.AddModel(timerModel)
	generator.AddModel(childWorkflowModel)
	generator.AddModel(externalWorkflowModel)
	s.generator = generator
}

func (s *historyEventTestSuit) SetupTest() {
	s.generator.Reset()
}

func (s *historyEventTestSuit) Test_HistoryEvent_Generator() {
	for s.generator.HasNextVertex() {
		v := s.generator.GetNextVertex()
		for _, e := range v {
			fmt.Println(e.GetName())
		}
	}
	s.NotEmpty(s.generator.ListGeneratedVertex())
}
