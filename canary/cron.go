package canary

import (
	"context"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal/activity"
	"go.temporal.io/temporal/client"
	"go.temporal.io/temporal/workflow"
	"go.uber.org/zap"
)

var (
	cronSleepTime = time.Second * 5
)

func registerCron(r registrar) {
	registerWorkflow(r, cronWorkflow, wfTypeCron)
	registerActivity(r, cronActivity, "")
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
	namespace string,
	jobName string) error {

	profile, err := beginWorkflow(ctx, wfTypeCron, workflow.Now(ctx).UnixNano())
	aCtx := workflow.WithActivityOptions(ctx, newActivityOptions())

	startTime := workflow.Now(ctx).UnixNano()
	workflow.ExecuteActivity(aCtx, cronActivity, startTime, namespace, jobName)

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
	namespace string,
	jobName string) error {

	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeCron, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	cadenceClient := getActivityContext(ctx).cadence
	logger := activity.GetLogger(ctx)
	parentID := activity.GetInfo(ctx).WorkflowExecution.ID

	jobID := fmt.Sprintf("%s-%s-%v", parentID, jobName, time.Now().Format(time.RFC3339))
	wf, err := startJob(&cadenceClient, scope, jobID, jobName, namespace)
	if err != nil {
		logger.Error("cronActivity: failed to start job", zap.Error(err))
		if _, ok := err.(*serviceerror.NamespaceNotActive); ok {
			return err
		}
	} else {
		logger.Info("cronActivity: started new job",
			zap.String("wfID", jobID), zap.String("runID", wf.GetRunID()))
	}

	return nil
}

func startJob(
	cadenceClient *cadenceClient,
	scope tally.Scope,
	jobID string,
	jobName string,
	namespace string) (client.WorkflowRun, error) {

	scope.Counter(startWorkflowCount).Inc(1)
	sw := scope.Timer(startWorkflowLatency).Start()
	defer sw.Stop()

	// start off a workflow span
	ctx := context.Background()
	span := opentracing.StartSpan(fmt.Sprintf("start-%v", jobName))
	defer span.Finish()
	ctx = opentracing.ContextWithSpan(ctx, span)

	opts := newWorkflowOptions(jobID, cronJobTimeout)
	wf, err := cadenceClient.ExecuteWorkflow(ctx, opts, jobName, time.Now().UnixNano(), namespace)
	if err != nil {
		scope.Counter(startWorkflowFailureCount).Inc(1)
		switch err.(type) {
		case *serviceerror.WorkflowExecutionAlreadyStarted:
			scope.Counter(startWorkflowAlreadyStartedCount).Inc(1)
		case *serviceerror.NamespaceNotActive:
			scope.Counter(startWorkflowNamespaceNotActiveCount).Inc(1)
		}

		return nil, err
	}
	scope.Counter(startWorkflowSuccessCount).Inc(1)
	return wf, err
}
