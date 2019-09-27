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

package canary

import (
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

// sanityChildWFList is the list of tests / child workflows invoked by the sanity canary
var sanityChildWFList = []string{
	wfTypeEcho,
	wfTypeSignal,
	wfTypeVisibility,
	wfTypeSearchAttributes,
	wfTypeConcurrentExec,
	wfTypeQuery,
	wfTypeTimeout,
	wfTypeLocalActivity,
	wfTypeCancellation,
	wfTypeRetry,
	wfTypeReset,
	wfTypeArchival,
	wfTypeBatch,
}

func init() {
	registerWorkflow(sanityWorkflow, wfTypeSanity)
}

// sanityWorkflow represents a canary implementation that tests the
// sanity of a cadence cluster - it performs probes / tests that
// exercises the frontend APIs and the basic functionality of the
// client library - each probe / test MUST be implemented as a
// child workflow of this workflow
func sanityWorkflow(ctx workflow.Context, scheduledTimeNanos int64, domain string) error {
	var err error
	profile, err := beginWorkflow(ctx, wfTypeSanity, scheduledTimeNanos)
	if err != nil {
		return profile.end(err)
	}

	childNames, err := getChildWorkflowNames(ctx)
	if err != nil {
		return profile.end(err)
	}

	selector, resultC := forkChildWorkflows(ctx, domain, childNames)
	err = joinChildWorkflows(ctx, childNames, selector, resultC)
	if err != nil {
		workflow.GetLogger(ctx).Error("sanity workflow failed", zap.Error(err))
		return profile.end(err)
	}

	workflow.GetLogger(ctx).Info("sanity workflow finished successfully")
	return profile.end(err)
}

// forkChildWorkflows spawns child workflows with the given names
// this method assumes that all child workflows have the same method signature
func forkChildWorkflows(ctx workflow.Context, domain string, names []string) (workflow.Selector, workflow.Channel) {
	now := workflow.Now(ctx).UnixNano()
	selector := workflow.NewSelector(ctx)
	resultC := workflow.NewBufferedChannel(ctx, len(names))

	myID := workflow.GetInfo(ctx).WorkflowExecution.ID
	for _, childName := range names {
		cwo := newChildWorkflowOptions(domain, concat(myID, childName))
		childCtx := workflow.WithChildOptions(ctx, cwo)
		future := workflow.ExecuteChildWorkflow(childCtx, childName, now, domain)
		selector.AddFuture(future, func(f workflow.Future) {
			if err := f.Get(ctx, nil); err != nil {
				workflow.GetLogger(ctx).Error("child workflow failed", zap.Error(err))
				resultC.Send(ctx, err)
				return
			}
			resultC.Send(ctx, nil)
		})
	}

	return selector, resultC
}

// joinChildWorkflows waits for all the given child workflows to complete
// returns error even if atleast one of the child workflow returns an error
func joinChildWorkflows(ctx workflow.Context, names []string, selector workflow.Selector, resultC workflow.Channel) error {
	var err error
	for i := 0; i < len(names); i++ {
		selector.Select(ctx)
		var err1 error
		resultC.Receive(ctx, &err1)
		if err1 != nil {
			err = err1
		}
	}
	return err
}

// getChildWorkflowNames exist mainly to make sure replays result in
// deterministic behavior for the workflow - the set of child workflows
// to be spawned are recorded in history as a side effect and result
// from the side effect is returned as a result
func getChildWorkflowNames(ctx workflow.Context) ([]string, error) {
	var names []string
	err := workflow.SideEffect(ctx, func(workflow.Context) interface{} { return sanityChildWFList }).Get(&names)
	return names, err
}
