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
	"context"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

var (
	cronSleepTime = time.Second * 5
)

func init() {
	workflow.RegisterWithOptions(cronWorkflow, workflow.RegisterOptions{Name: wfTypeCron})
	activity.Register(cronActivity)
}

// cronWorkflow is a generic cron workflow implementation that takes as input
// a tuple - (jobName, JobFrequency) where jobName refers to a workflow name
// and job frequency refers to the rate at which the job must be scheduled
// there are several assumptions that this cron makes:
// - jobFrequency is any value between 1-59 seconds
// - jobName refers to a workflow function with a well-defined function signature
// - every instance of job completes within 10 mins
func cronWorkflow(
	ctx workflow.Context,
	domain string,
	jobName string) error {

	profile, err := beginWorkflow(ctx, wfTypeCron, workflow.Now(ctx).UnixNano())
	aCtx := workflow.WithActivityOptions(ctx, newActivityOptions())

	startTime := workflow.Now(ctx).UnixNano()
	workflow.ExecuteActivity(aCtx, cronActivity, startTime, domain, jobName)

	workflow.Sleep(ctx, cronSleepTime)
	elapsed := time.Duration(workflow.Now(ctx).UnixNano() - startTime)
	profile.scope.Timer(timerDriftLatency).Record(absDurationDiff(elapsed, cronSleepTime))

	return err
}

// cronActivity starts root canary workflows at a pre-defined frequency
// this activity exits automatically a minute after it is scheduled
func cronActivity(
	ctx context.Context,
	scheduledTimeNanos int64,
	domain string,
	jobName string) error {

	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeCron, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	cadenceClient := getActivityContext(ctx).cadence
	logger := activity.GetLogger(ctx)
	parentID := activity.GetInfo(ctx).WorkflowExecution.ID

	jobID := fmt.Sprintf("%s-%s-%v", parentID, jobName, time.Now().Format(time.RFC3339))
	wf, err := startJob(&cadenceClient, scope, jobID, jobName, domain)
	if err != nil {
		logger.Error("cronActivity: failed to start job", zap.Error(err))
		if isDomainNotActiveErr(err) {
			return err
		}
	} else {
		logger.Info("cronActivity: started new job",
			zap.String("wfID", jobID), zap.String("runID", wf.RunID))
	}

	return nil
}

func startJob(
	cadenceClient *cadenceClient,
	scope tally.Scope,
	jobID string,
	jobName string,
	domain string) (*workflow.Execution, error) {

	scope.Counter(startWorkflowCount).Inc(1)
	sw := scope.Timer(startWorkflowLatency).Start()
	defer sw.Stop()

	// start off a workflow span
	ctx := context.Background()
	span := opentracing.StartSpan(fmt.Sprintf("start-%v", jobName))
	defer span.Finish()
	ctx = opentracing.ContextWithSpan(ctx, span)

	opts := newWorkflowOptions(jobID, cronJobTimeout)
	wf, err := cadenceClient.StartWorkflow(ctx, opts, jobName, time.Now().UnixNano(), domain)
	if err != nil {
		scope.Counter(startWorkflowFailureCount).Inc(1)
		switch err.(type) {
		case *shared.WorkflowExecutionAlreadyStartedError:
			scope.Counter(startWorkflowAlreadyStartedCount).Inc(1)
		case *shared.DomainNotActiveError:
			scope.Counter(startWorkflowDomainNotActiveCount).Inc(1)
		}
		return nil, err
	}
	scope.Counter(startWorkflowSuccessCount).Inc(1)
	return wf, err
}

func isDomainNotActiveErr(err error) bool {
	_, ok := err.(*shared.DomainNotActiveError)
	return ok
}
