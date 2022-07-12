package batcher

import (
	"context"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"golang.org/x/time/rate"
)

type activities struct {
	activityDeps
	namespace   namespace.Name
	namespaceID namespace.ID
}

// BatchActivity is activity for processing batch operation
func (a *activities) BatchActivity(ctx context.Context, batchParams BatchParams) (HeartBeatDetails, error) {
	logger := a.getActivityLogger(ctx)
	hbd := HeartBeatDetails{}

	sdkClient, err := a.ClientFactory.NewClient(batchParams.Namespace, logger)
	if err != nil {
		logger.Error("Unable to create SDK client for namespace.", tag.Error(err), tag.WorkflowNamespace(batchParams.Namespace))
		return hbd, err
	}

	startOver := true
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &hbd); err == nil {
			startOver = false
		} else {
			a.MetricsClient.IncCounter(metrics.BatcherScope, metrics.BatcherProcessorFailures)
			logger.Error("Failed to recover from last heartbeat, start over from beginning", tag.Error(err))
		}
	}

	if startOver {
		resp, err := sdkClient.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Query: batchParams.Query,
		})
		if err != nil {
			return HeartBeatDetails{}, err
		}
		hbd.TotalEstimate = resp.GetCount()
	}
	rateLimiter := rate.NewLimiter(rate.Limit(batchParams.RPS), batchParams.RPS)
	taskCh := make(chan taskDetail, pageSize)
	respCh := make(chan error, pageSize)
	for i := 0; i < batchParams.Concurrency; i++ {
		go startTaskProcessor(ctx, batchParams, taskCh, respCh, rateLimiter, sdkClient, a.MetricsClient, logger)
	}

	for {
		resp, err := sdkClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			PageSize:      int32(pageSize),
			NextPageToken: hbd.PageToken,
			Query:         batchParams.Query,
		})
		if err != nil {
			return HeartBeatDetails{}, err
		}
		batchCount := len(resp.Executions)
		if batchCount <= 0 {
			break
		}

		// send all tasks
		for _, wf := range resp.Executions {
			taskCh <- taskDetail{
				execution: *wf.Execution,
				attempts:  1,
				hbd:       hbd,
			}
		}

		succCount := 0
		errCount := 0
		// wait for counters indicate this batch is done
	Loop:
		for {
			select {
			case err := <-respCh:
				if err == nil {
					succCount++
				} else {
					errCount++
				}
				if succCount+errCount == batchCount {
					break Loop
				}
			case <-ctx.Done():
				return HeartBeatDetails{}, ctx.Err()
			}
		}

		hbd.CurrentPage++
		hbd.PageToken = resp.NextPageToken
		hbd.SuccessCount += succCount
		hbd.ErrorCount += errCount
		activity.RecordHeartbeat(ctx, hbd)

		if len(hbd.PageToken) == 0 {
			break
		}
	}

	return hbd, nil
}

func (a *activities) getActivityLogger(ctx context.Context) log.Logger {
	wfInfo := activity.GetInfo(ctx)
	return log.With(
		a.Logger,
		tag.WorkflowID(wfInfo.WorkflowExecution.ID),
		tag.WorkflowRunID(wfInfo.WorkflowExecution.RunID),
		tag.WorkflowNamespace(wfInfo.WorkflowNamespace),
	)
}
