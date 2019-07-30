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
	"runtime/debug"
	"testing"
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
	generator := NewEventGenerator()

	//Function
	notPendingDecisionTask := func() bool {
		count := 0
		for _, e := range generator.ListGeneratedVertices() {
			switch e.GetName() {
			case shared.EventTypeDecisionTaskScheduled.String():
				count++
			case shared.EventTypeDecisionTaskCompleted.String(),
				shared.EventTypeDecisionTaskFailed.String(),
				shared.EventTypeDecisionTaskTimedOut.String():
				count--
			}
		}
		return count <= 0
	}

	containActivityComplete := func() bool {
		for _, e := range generator.ListGeneratedVertices() {
			if e.GetName() == shared.EventTypeActivityTaskCompleted.String() {
				return true
			}
		}
		return false
	}

	hasPendingTimer := func() bool {
		count := 0
		for _, e := range generator.ListGeneratedVertices() {
			switch e.GetName() {
			case shared.EventTypeTimerStarted.String():
				count++
			case shared.EventTypeTimerFired.String(),
				shared.EventTypeTimerCanceled.String():
				count--
			}
		}
		return count > 0
	}

	hasPendingActivity := func() bool {
		count := 0
		for _, e := range generator.ListGeneratedVertices() {
			switch e.GetName() {
			case shared.EventTypeActivityTaskScheduled.String():
				count++
			case shared.EventTypeActivityTaskCanceled.String(),
				shared.EventTypeActivityTaskFailed.String(),
				shared.EventTypeActivityTaskTimedOut.String(),
				shared.EventTypeActivityTaskCompleted.String():
				count--
			}
		}
		return count > 0
	}

	hasPendingStartActivity := func() bool {
		count := 0
		for _, e := range generator.ListGeneratedVertices() {
			switch e.GetName() {
			case shared.EventTypeActivityTaskStarted.String():
				count++
			case shared.EventTypeActivityTaskCanceled.String(),
				shared.EventTypeActivityTaskFailed.String(),
				shared.EventTypeActivityTaskTimedOut.String(),
				shared.EventTypeActivityTaskCompleted.String():
				count--
			}
		}
		return count > 0
	}

	canDoBatch := func(history []Vertex) bool {
		if len(history) == 0 {
			return true
		}

		hasPendingDecisionTask := false
		for _, event := range generator.ListGeneratedVertices() {
			switch event.GetName() {
			case shared.EventTypeDecisionTaskScheduled.String():
				hasPendingDecisionTask = true
			case shared.EventTypeDecisionTaskCompleted.String(),
				shared.EventTypeDecisionTaskFailed.String(),
				shared.EventTypeDecisionTaskTimedOut.String():
				hasPendingDecisionTask = false
			}
		}
		if hasPendingDecisionTask {
			return false
		}
		if history[len(history)-1].GetName() == shared.EventTypeDecisionTaskScheduled.String() {
			return false
		}
		if history[0].GetName() == shared.EventTypeDecisionTaskCompleted.String() {
			return len(history) == 1
		}
		return true
	}

	//Setup decision task model
	decisionModel := NewHistoryEventModel()
	decisionSchedule := NewHistoryEventVertex(shared.EventTypeDecisionTaskScheduled.String())
	decisionStart := NewHistoryEventVertex(shared.EventTypeDecisionTaskStarted.String())
	decisionStart.SetIsStrictOnNextVertex(true)
	decisionFail := NewHistoryEventVertex(shared.EventTypeDecisionTaskFailed.String())
	decisionTimedOut := NewHistoryEventVertex(shared.EventTypeDecisionTaskTimedOut.String())
	decisionComplete := NewHistoryEventVertex(shared.EventTypeDecisionTaskCompleted.String())
	decisionComplete.SetIsStrictOnNextVertex(true)
	decisionComplete.SetMaxNextVertex(2)
	decisionScheduleToStart := NewHistoryEventEdge(decisionSchedule, decisionStart)
	decisionStartToComplete := NewHistoryEventEdge(decisionStart, decisionComplete)
	decisionStartToFail := NewHistoryEventEdge(decisionStart, decisionFail)
	decisionStartToTimedOut := NewHistoryEventEdge(decisionStart, decisionTimedOut)
	decisionFailToSchedule := NewHistoryEventEdge(decisionFail, decisionSchedule)
	decisionFailToSchedule.SetCondition(notPendingDecisionTask)
	decisionTimedOutToSchedule := NewHistoryEventEdge(decisionTimedOut, decisionSchedule)
	decisionTimedOutToSchedule.SetCondition(notPendingDecisionTask)
	decisionModel.AddEdge(decisionScheduleToStart, decisionStartToComplete, decisionStartToFail, decisionStartToTimedOut,
		decisionFailToSchedule, decisionTimedOutToSchedule)

	//Setup workflow model
	workflowModel := NewHistoryEventModel()
	workflowStart := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionStarted.String())
	workflowSignal := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionSignaled.String())
	workflowComplete := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionCompleted.String())
	continueAsNew := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionContinuedAsNew.String())
	workflowFail := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionFailed.String())
	workflowCancel := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionCanceled.String())
	workflowCancelRequest := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionCancelRequested.String()) //?
	workflowTerminate := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionTerminated.String())
	workflowTimedOut := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionTimedOut.String())
	workflowStartToSignal := NewHistoryEventEdge(workflowStart, workflowSignal)
	workflowStartToDecisionSchedule := NewHistoryEventEdge(workflowStart, decisionSchedule)
	workflowStartToDecisionSchedule.SetCondition(notPendingDecisionTask)
	workflowSignalToDecisionSchedule := NewHistoryEventEdge(workflowSignal, decisionSchedule)
	workflowSignalToDecisionSchedule.SetCondition(notPendingDecisionTask)
	decisionCompleteToWorkflowComplete := NewHistoryEventEdge(decisionComplete, workflowComplete)
	decisionCompleteToWorkflowComplete.SetCondition(containActivityComplete)
	decisionCompleteToWorkflowFailed := NewHistoryEventEdge(decisionComplete, workflowFail)
	decisionCompleteToWorkflowFailed.SetCondition(containActivityComplete)
	decisionCompleteToCAN := NewHistoryEventEdge(decisionComplete, continueAsNew)
	decisionCompleteToCAN.SetCondition(containActivityComplete)
	workflowCancelRequestToCancel := NewHistoryEventEdge(workflowCancelRequest, workflowCancel)
	workflowModel.AddEdge(workflowStartToSignal, workflowStartToDecisionSchedule, workflowSignalToDecisionSchedule,
		decisionCompleteToCAN, decisionCompleteToWorkflowComplete, decisionCompleteToWorkflowFailed, workflowCancelRequestToCancel)

	//Setup activity model
	activityModel := NewHistoryEventModel()
	activitySchedule := NewHistoryEventVertex(shared.EventTypeActivityTaskScheduled.String())
	activityStart := NewHistoryEventVertex(shared.EventTypeActivityTaskStarted.String())
	activityComplete := NewHistoryEventVertex(shared.EventTypeActivityTaskCompleted.String())
	activityFail := NewHistoryEventVertex(shared.EventTypeActivityTaskFailed.String())
	activityTimedOut := NewHistoryEventVertex(shared.EventTypeActivityTaskTimedOut.String())
	activityCancelRequest := NewHistoryEventVertex(shared.EventTypeActivityTaskCancelRequested.String()) //?
	activityCancel := NewHistoryEventVertex(shared.EventTypeActivityTaskCanceled.String())
	activityCancelRequestFail := NewHistoryEventVertex(shared.EventTypeRequestCancelActivityTaskFailed.String())
	decisionCompleteToATSchedule := NewHistoryEventEdge(decisionComplete, activitySchedule)

	activityScheduleToStart := NewHistoryEventEdge(activitySchedule, activityStart)
	activityScheduleToStart.SetCondition(hasPendingActivity)

	activityStartToComplete := NewHistoryEventEdge(activityStart, activityComplete)
	activityStartToComplete.SetCondition(hasPendingActivity)

	activityStartToFail := NewHistoryEventEdge(activityStart, activityFail)
	activityStartToFail.SetCondition(hasPendingActivity)

	activityStartToTimedOut := NewHistoryEventEdge(activityStart, activityTimedOut)
	activityStartToTimedOut.SetCondition(hasPendingActivity)

	activityCompleteToDecisionSchedule := NewHistoryEventEdge(activityComplete, decisionSchedule)
	activityCompleteToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityFailToDecisionSchedule := NewHistoryEventEdge(activityFail, decisionSchedule)
	activityFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityTimedOutToDecisionSchedule := NewHistoryEventEdge(activityTimedOut, decisionSchedule)
	activityTimedOutToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityCancelToDecisionSchedule := NewHistoryEventEdge(activityCancel, decisionSchedule)
	activityCancelToDecisionSchedule.SetCondition(notPendingDecisionTask)

	activityCancelReqToCancel := NewHistoryEventEdge(activityCancelRequest, activityCancel)
	activityCancelReqToCancel.SetCondition(hasPendingActivity)

	activityCancelReqToCancelFail := NewHistoryEventEdge(activityCancelRequest, activityCancelRequestFail)
	activityCancelRequestFailToDecisionSchedule := NewHistoryEventEdge(activityCancelRequestFail, decisionSchedule)
	activityCancelRequestFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	decisionCompleteToActivityCancelRequest := NewHistoryEventEdge(decisionComplete, activityCancelRequest)
	decisionCompleteToActivityCancelRequest.SetCondition(hasPendingStartActivity)

	activityModel.AddEdge(decisionCompleteToATSchedule, activityScheduleToStart, activityStartToComplete,
		activityStartToFail, activityStartToTimedOut, decisionCompleteToATSchedule, activityCompleteToDecisionSchedule,
		activityFailToDecisionSchedule, activityTimedOutToDecisionSchedule, activityCancelReqToCancel, activityCancelReqToCancelFail,
		activityCancelToDecisionSchedule, decisionCompleteToActivityCancelRequest, activityCancelRequestFailToDecisionSchedule)

	//Setup timer model
	timerModel := NewHistoryEventModel()
	timerStart := NewHistoryEventVertex(shared.EventTypeTimerStarted.String())
	timerFired := NewHistoryEventVertex(shared.EventTypeTimerFired.String())
	timerCancel := NewHistoryEventVertex(shared.EventTypeTimerCanceled.String())
	timerStartToFire := NewHistoryEventEdge(timerStart, timerFired)
	timerStartToFire.SetCondition(hasPendingTimer)
	decisionCompleteToCancel := NewHistoryEventEdge(decisionComplete, timerCancel)
	decisionCompleteToCancel.SetCondition(hasPendingTimer)

	decisionCompleteToTimerStart := NewHistoryEventEdge(decisionComplete, timerStart)
	timerFiredToDecisionSchedule := NewHistoryEventEdge(timerFired, decisionSchedule)
	timerFiredToDecisionSchedule.SetCondition(notPendingDecisionTask)
	timerCancelToDecisionSchedule := NewHistoryEventEdge(timerCancel, decisionSchedule)
	timerCancelToDecisionSchedule.SetCondition(notPendingDecisionTask)
	timerModel.AddEdge(timerStartToFire, decisionCompleteToCancel, decisionCompleteToTimerStart, timerFiredToDecisionSchedule, timerCancelToDecisionSchedule)

	//Setup child workflow model
	childWorkflowModel := NewHistoryEventModel()
	childWorkflowInitial := NewHistoryEventVertex(shared.EventTypeStartChildWorkflowExecutionInitiated.String())
	childWorkflowInitialFail := NewHistoryEventVertex(shared.EventTypeStartChildWorkflowExecutionFailed.String())
	childWorkflowStart := NewHistoryEventVertex(shared.EventTypeChildWorkflowExecutionStarted.String())
	childWorkflowCancel := NewHistoryEventVertex(shared.EventTypeChildWorkflowExecutionCanceled.String())
	childWorkflowComplete := NewHistoryEventVertex(shared.EventTypeChildWorkflowExecutionCompleted.String())
	childWorkflowFail := NewHistoryEventVertex(shared.EventTypeChildWorkflowExecutionFailed.String())
	childWorkflowTerminate := NewHistoryEventVertex(shared.EventTypeChildWorkflowExecutionTerminated.String())
	childWorkflowTimedOut := NewHistoryEventVertex(shared.EventTypeChildWorkflowExecutionTimedOut.String())
	decisionCompleteToChildWorkflowInitial := NewHistoryEventEdge(decisionComplete, childWorkflowInitial)
	childWorkflowInitialToFail := NewHistoryEventEdge(childWorkflowInitial, childWorkflowInitialFail)
	childWorkflowInitialToStart := NewHistoryEventEdge(childWorkflowInitial, childWorkflowStart)
	childWorkflowStartToCancel := NewHistoryEventEdge(childWorkflowStart, childWorkflowCancel)
	childWorkflowStartToFail := NewHistoryEventEdge(childWorkflowStart, childWorkflowFail)
	childWorkflowStartToComplete := NewHistoryEventEdge(childWorkflowStart, childWorkflowComplete)
	childWorkflowStartToTerminate := NewHistoryEventEdge(childWorkflowStart, childWorkflowTerminate)
	childWorkflowStartToTimedOut := NewHistoryEventEdge(childWorkflowStart, childWorkflowTimedOut)
	childWorkflowCancelToDecisionSchedule := NewHistoryEventEdge(childWorkflowCancel, decisionSchedule)
	childWorkflowCancelToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowFailToDecisionSchedule := NewHistoryEventEdge(childWorkflowFail, decisionSchedule)
	childWorkflowFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowCompleteToDecisionSchedule := NewHistoryEventEdge(childWorkflowComplete, decisionSchedule)
	childWorkflowCompleteToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowTerminateToDecisionSchedule := NewHistoryEventEdge(childWorkflowTerminate, decisionSchedule)
	childWorkflowTerminateToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowTimedOutToDecisionSchedule := NewHistoryEventEdge(childWorkflowTimedOut, decisionSchedule)
	childWorkflowTimedOutToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowInitialFailToDecisionSchedule := NewHistoryEventEdge(childWorkflowInitialFail, decisionSchedule)
	childWorkflowInitialFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowModel.AddEdge(decisionCompleteToChildWorkflowInitial, childWorkflowInitialToFail, childWorkflowInitialToStart,
		childWorkflowStartToCancel, childWorkflowStartToFail, childWorkflowStartToComplete, childWorkflowStartToTerminate,
		childWorkflowStartToTimedOut, childWorkflowCancelToDecisionSchedule, childWorkflowFailToDecisionSchedule,
		childWorkflowCompleteToDecisionSchedule, childWorkflowTerminateToDecisionSchedule, childWorkflowTimedOutToDecisionSchedule,
		childWorkflowInitialFailToDecisionSchedule)

	//Setup external workflow model
	externalWorkflowModel := NewHistoryEventModel()
	externalWorkflowSignal := NewHistoryEventVertex(shared.EventTypeSignalExternalWorkflowExecutionInitiated.String())
	externalWorkflowSignalFailed := NewHistoryEventVertex(shared.EventTypeSignalExternalWorkflowExecutionFailed.String())
	externalWorkflowSignaled := NewHistoryEventVertex(shared.EventTypeExternalWorkflowExecutionSignaled.String())
	externalWorkflowCancel := NewHistoryEventVertex(shared.EventTypeRequestCancelExternalWorkflowExecutionInitiated.String())
	externalWorkflowCancelFail := NewHistoryEventVertex(shared.EventTypeRequestCancelExternalWorkflowExecutionFailed.String())
	externalWorkflowCanceled := NewHistoryEventVertex(shared.EventTypeExternalWorkflowExecutionCancelRequested.String())
	decisionCompleteToExternalWorkflowSignal := NewHistoryEventEdge(decisionComplete, externalWorkflowSignal)
	decisionCompleteToExternalWorkflowCancel := NewHistoryEventEdge(decisionComplete, externalWorkflowCancel)
	externalWorkflowSignalToFail := NewHistoryEventEdge(externalWorkflowSignal, externalWorkflowSignalFailed)
	externalWorkflowSignalToSignaled := NewHistoryEventEdge(externalWorkflowSignal, externalWorkflowSignaled)
	externalWorkflowCancelToFail := NewHistoryEventEdge(externalWorkflowCancel, externalWorkflowCancelFail)
	externalWorkflowCancelToCanceled := NewHistoryEventEdge(externalWorkflowCancel, externalWorkflowCanceled)
	externalWorkflowSignaledToDecisionSchedule := NewHistoryEventEdge(externalWorkflowSignaled, decisionSchedule)
	externalWorkflowSignaledToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowSignalFailedToDecisionSchedule := NewHistoryEventEdge(externalWorkflowSignalFailed, decisionSchedule)
	externalWorkflowSignalFailedToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowCanceledToDecisionSchedule := NewHistoryEventEdge(externalWorkflowCanceled, decisionSchedule)
	externalWorkflowCanceledToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowCancelFailToDecisionSchedule := NewHistoryEventEdge(externalWorkflowCancelFail, decisionSchedule)
	externalWorkflowCancelFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowModel.AddEdge(decisionCompleteToExternalWorkflowSignal, decisionCompleteToExternalWorkflowCancel,
		externalWorkflowSignalToFail, externalWorkflowSignalToSignaled, externalWorkflowCancelToFail, externalWorkflowCancelToCanceled,
		externalWorkflowSignaledToDecisionSchedule, externalWorkflowSignalFailedToDecisionSchedule,
		externalWorkflowCanceledToDecisionSchedule, externalWorkflowCancelFailToDecisionSchedule)

	//Config event generator
	generator.SetBatchGenerationRule(canDoBatch)
	generator.AddInitialEntryVertex(workflowStart)
	generator.AddExitVertex(workflowComplete, workflowFail, continueAsNew, workflowTerminate, workflowTimedOut)
	//generator.AddRandomEntryVertex(workflowSignal, workflowTerminate, workflowTimedOut)
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

// This is a sample about how to use the generator
func (s *historyEventTestSuit) Test_HistoryEvent_Generator() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("stacktrace from panic: \n" + string(debug.Stack()))
		}
	}()

	totalBranchNumber := 2
	currentBranch := totalBranchNumber
	root := &branch{
		batches: make([]batch, 0),
	}
	curr := root
	//eventRanches := make([][]Vertex, 0, totalBranchNumber)
	for currentBranch > 0 {
		for s.generator.HasNextVertex() {
			events := s.generator.GetNextVertices()
			newBatch := batch{
				events: events,
			}
			curr.batches = append(curr.batches, newBatch)
			for _, e := range events {
				fmt.Println(e.GetName())
			}
		}
		currentBranch--
		if currentBranch > 0 {
			resetIdx := s.generator.RandomResetToResetPoint()
			curr = root.split(resetIdx)
		}
	}
	s.NotEmpty(s.generator.ListGeneratedVertices())
	queue := []*branch{root}
	for len(queue) > 0 {
		b := queue[0]
		queue = queue[1:]
		for _, batch := range b.batches {
			for _, event := range batch.events {
				fmt.Println(event.GetName())
			}
		}
		queue = append(queue, b.next...)
	}

	// Generator one branch of history events
	batches := []batch{}
	batches = append(batches, root.batches...)
	batches = append(batches, root.next[0].batches...)
	attributeGenerator := NewHistoryAttributesGenerator()
	history := attributeGenerator.GenerateHistoryEvents(batches)
	s.NotEmpty(history)
}
