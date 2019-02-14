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

package sysworkflow

import (
	"context"
	"encoding/json"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/blobstore/blob"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"time"
)

var (
	errArchivalUploadActivityGetDomain           = cadence.NewCustomError(errArchivalUploadActivityGetDomainStr)
	errArchivalUploadActivityNextBlob            = cadence.NewCustomError(errArchivalUploadActivityNextBlobStr)
	errArchivalUploadActivityConstructKey        = cadence.NewCustomError(errArchivalUploadActivityConstructKeyStr)
	errArchivalUploadActivityBlobExists          = cadence.NewCustomError(errArchivalUploadActivityBlobExistsStr)
	errArchivalUploadActivityMarshalBlob         = cadence.NewCustomError(errArchivalUploadActivityMarshalBlobStr)
	errArchivalUploadActivityConvertHeaderToTags = cadence.NewCustomError(errArchivalUploadActivityConvertHeaderToTagsStr)
	errArchivalUploadActivityWrapBlob            = cadence.NewCustomError(errArchivalUploadActivityWrapBlobStr)
	errArchivalUploadActivityUploadBlob          = cadence.NewCustomError(errArchivalUploadActivityUploadBlobStr)
	errDeleteHistoryActivityDeleteFromV2         = cadence.NewCustomError(errDeleteHistoryActivityDeleteFromV2Str)
	errDeleteHistoryActivityDeleteFromV1         = cadence.NewCustomError(errDeleteHistoryActivityDeleteFromV1Str)
)

// ArchiveSystemWorkflow is the system workflow which archives and deletes history
func ArchiveSystemWorkflow(ctx workflow.Context, carryoverRequests []ArchiveRequest) error {
	sysWorkflowInfo := workflow.GetInfo(ctx)
	logger := NewReplayBarkLogger(globalLogger.WithFields(bark.Fields{
		logging.TagWorkflowExecutionID: sysWorkflowInfo.WorkflowExecution.ID,
		logging.TagWorkflowRunID:       sysWorkflowInfo.WorkflowExecution.RunID,
	}), ctx, false)
	logger.Info("started system workflow")
	metricsClient := NewReplayMetricsClient(globalMetricsClient, ctx)
	metricsClient.IncCounter(metrics.SystemWorkflowScope, metrics.SysWorkerWorkflowStarted)
	sw := metricsClient.StartTimer(metrics.SystemWorkflowScope, metrics.SysWorkerContinueAsNewLatency)
	requestsHandled := 0

	// step 1: start workers to process archival requests in parallel
	workQueue := workflow.NewChannel(ctx)
	finishedWorkQueue := workflow.NewBufferedChannel(ctx, signalsUntilContinueAsNew*10) // make large enough that never blocks on send
	for i := 0; i < numWorkers; i++ {
		workflow.Go(ctx, func(ctx workflow.Context) {
			for {
				var request ArchiveRequest
				workQueue.Receive(ctx, &request)
				handleRequest(request, ctx, logger, metricsClient)
				finishedWorkQueue.Send(ctx, nil)
			}
		})
	}

	// step 2: pump carryover requests into worker queue
	for _, request := range carryoverRequests {
		requestsHandled++
		workQueue.Send(ctx, request)
	}

	// step 3: pump current iterations workload into worker queue
	ch := workflow.GetSignalChannel(ctx, signalName)
	for requestsHandled < signalsUntilContinueAsNew {
		var request ArchiveRequest
		if more := ch.Receive(ctx, &request); !more {
			break
		}
		metricsClient.IncCounter(metrics.SystemWorkflowScope, metrics.SysWorkerReceivedSignal)
		requestsHandled++
		workQueue.Send(ctx, request)
	}

	// step 4: wait for all in progress work to finish
	for i := 0; i < requestsHandled; i++ {
		finishedWorkQueue.Receive(ctx, nil)
	}

	// step 5: drain signal channel to get next run's carryover
	var co []ArchiveRequest
	for {
		var request ArchiveRequest
		if ok := ch.ReceiveAsync(&request); !ok {
			break
		}
		metricsClient.IncCounter(metrics.SystemWorkflowScope, metrics.SysWorkerReceivedSignal)
		co = append(co, request)
	}

	// step 6: schedule new run
	ctx = workflow.WithExecutionStartToCloseTimeout(ctx, workflowStartToCloseTimeout)
	ctx = workflow.WithWorkflowTaskStartToCloseTimeout(ctx, decisionTaskStartToCloseTimeout)
	metricsClient.IncCounter(metrics.SystemWorkflowScope, metrics.SysWorkerContinueAsNew)
	sw.Stop()
	logger.WithFields(bark.Fields{
		logging.TagNumberOfSignalsUntilContinueAsNew: requestsHandled,
	}).Info("system workflow is continuing as new")
	return workflow.NewContinueAsNewError(ctx, archiveSystemWorkflowFnName, co)
}

func handleRequest(request ArchiveRequest, ctx workflow.Context, logger bark.Logger, metricsClient metrics.Client) {
	uploadActOpts := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    5 * time.Minute,
		HeartbeatTimeout:       heartbeatTimeout,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			ExpirationInterval: time.Hour * 24 * 30,
			NonRetriableErrorReasons: []string{
				errArchivalUploadActivityGetDomainStr,
				errArchivalUploadActivityNextBlobStr,
				errArchivalUploadActivityConstructKeyStr,
				errArchivalUploadActivityBlobExistsStr,
				errArchivalUploadActivityMarshalBlobStr,
				errArchivalUploadActivityConvertHeaderToTagsStr,
				errArchivalUploadActivityWrapBlobStr,
				errArchivalUploadActivityUploadBlobStr,
			},
		},
	}
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, uploadActOpts),
		archivalUploadActivityFnName,
		request,
	).Get(ctx, nil); err != nil {
		logger.WithFields(bark.Fields{
			logging.TagErr:                             err,
			logging.TagArchiveRequestDomainID:          request.DomainID,
			logging.TagArchiveRequestWorkflowID:        request.WorkflowID,
			logging.TagArchiveRequestRunID:             request.RunID,
			logging.TagArchiveRequestEventStoreVersion: request.EventStoreVersion,
			logging.TagArchiveRequestLastFirstEventID:  request.LastFirstEventID,
		}).Error("ArchivalUploadActivity encountered non-retryable error")
		metricsClient.IncCounter(metrics.SystemWorkflowScope, metrics.SysWorkerArchivalUploadActivityNonRetryableFailures)
	} else {
		metricsClient.IncCounter(metrics.SystemWorkflowScope, metrics.SysWorkerArchivalUploadSuccessful)
	}
	lao := workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: 10 * time.Second,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    10,
			NonRetriableErrorReasons: []string{
				errDeleteHistoryActivityDeleteFromV1Str,
				errDeleteHistoryActivityDeleteFromV2Str,
			},
		},
	}
	err := workflow.ExecuteLocalActivity(workflow.WithLocalActivityOptions(ctx, lao), ArchivalDeleteHistoryActivity, request).Get(ctx, nil)
	if err == nil {
		metricsClient.IncCounter(metrics.SystemWorkflowScope, metrics.SysWorkerArchivalDeleteHistorySuccessful)
		return
	}
	logger.WithFields(bark.Fields{
		logging.TagErr:                             err,
		logging.TagArchiveRequestDomainID:          request.DomainID,
		logging.TagArchiveRequestWorkflowID:        request.WorkflowID,
		logging.TagArchiveRequestRunID:             request.RunID,
		logging.TagArchiveRequestEventStoreVersion: request.EventStoreVersion,
		logging.TagArchiveRequestLastFirstEventID:  request.LastFirstEventID,
	}).Warn("ArchivalDeleteHistoryActivity could not be completed as a local activity, attempting to run as normal activity")
	deleteActOpts := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    5 * time.Minute,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			ExpirationInterval: time.Hour * 24 * 30,
			NonRetriableErrorReasons: []string{
				errDeleteHistoryActivityDeleteFromV1Str,
				errDeleteHistoryActivityDeleteFromV2Str,
			},
		},
	}
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, deleteActOpts),
		archivalDeleteHistoryActivityFnName,
		request,
	).Get(ctx, nil); err != nil {
		logger.WithFields(bark.Fields{
			logging.TagErr:                             err,
			logging.TagArchiveRequestDomainID:          request.DomainID,
			logging.TagArchiveRequestWorkflowID:        request.WorkflowID,
			logging.TagArchiveRequestRunID:             request.RunID,
			logging.TagArchiveRequestEventStoreVersion: request.EventStoreVersion,
			logging.TagArchiveRequestLastFirstEventID:  request.LastFirstEventID,
		}).Error("ArchivalDeleteHistoryActivity encountered non-retryable error")
		metricsClient.IncCounter(metrics.SystemWorkflowScope, metrics.SysWorkerArchivalDeleteHistoryActivityNonRetryableFailures)
	} else {
		metricsClient.IncCounter(metrics.SystemWorkflowScope, metrics.SysWorkerArchivalDeleteHistorySuccessful)
	}
}

// ArchivalUploadActivity does the following three things:
// 1. Read history from persistence
// 2. Construct blobs
// 3. Upload blobs
// It is assumed that history is immutable when this activity is running. Under this assumption this activity is idempotent.
// If an error is returned it will be of type archivalActivityNonRetryableErr. All retryable errors are retried forever.
func ArchivalUploadActivity(ctx context.Context, request ArchiveRequest) error {
	go func() {
		for {
			<-time.After(heartbeatTimeout / 2)
			activity.RecordHeartbeat(ctx)
		}
	}()
	container := ctx.Value(sysWorkerContainerKey).(*SysWorkerContainer)
	logger := container.Logger.WithFields(bark.Fields{
		logging.TagArchiveRequestDomainID:          request.DomainID,
		logging.TagArchiveRequestWorkflowID:        request.WorkflowID,
		logging.TagArchiveRequestRunID:             request.RunID,
		logging.TagArchiveRequestEventStoreVersion: request.EventStoreVersion,
		logging.TagArchiveRequestLastFirstEventID:  request.LastFirstEventID,
	})
	metricsClient := container.MetricsClient
	domainCache := container.DomainCache
	clusterMetadata := container.ClusterMetadata
	domainCacheEntry, err := getDomainByIDRetryForever(domainCache, request.DomainID)
	if err != nil {
		logger.WithError(err).Error("failed to get domain from domain cache")
		metricsClient.IncCounter(metrics.ArchivalUploadActivityScope, metrics.SysWorkerGetDomainFailures)
		return errArchivalUploadActivityGetDomain
	}
	if !clusterMetadata.IsArchivalEnabled() {
		// for now if archival is disabled simply abort the activity
		// a more in depth design meeting is needed to decide the correct way to handle backfilling/pausing archival
		logger.Warn("archival is not enabled for cluster, skipping archival upload")
		metricsClient.IncCounter(metrics.ArchivalUploadActivityScope, metrics.SysWorkerArchivalNotEnabledForCluster)
		return nil
	}
	if domainCacheEntry.GetConfig().ArchivalStatus != shared.ArchivalStatusEnabled {
		// for now if archival is disabled simply abort the activity
		// a more in depth design meeting is needed to decide the correct way to handle backfilling/pausing archival
		logger.Warn("archival is not enabled for domain, skipping archival upload")
		metricsClient.IncCounter(metrics.ArchivalUploadActivityScope, metrics.SysWorkerArchivalNotEnabledForDomain)
		return nil
	}

	domainName := domainCacheEntry.GetInfo().Name
	clusterName := container.ClusterMetadata.GetCurrentClusterName()
	historyBlobItr := container.HistoryBlobIterator
	if historyBlobItr == nil {
		historyBlobItr = NewHistoryBlobIterator(logger, metricsClient, request, container, domainName, clusterName)
	}

	blobstoreClient := container.Blobstore
	bucket := domainCacheEntry.GetConfig().ArchivalBucket
	for historyBlobItr.HasNext() {
		historyBlob, err := nextBlobRetryForever(historyBlobItr)
		if err != nil {
			logger.WithError(err).Error("failed to get next blob from iterator, stopping archival upload")
			metricsClient.IncCounter(metrics.ArchivalUploadActivityScope, metrics.SysWorkerNextBlobNonRetryableFailures)
			return errArchivalUploadActivityNextBlob
		}
		key, err := NewHistoryBlobKey(request.DomainID, request.WorkflowID, request.RunID, *historyBlob.Header.CurrentPageToken)
		if err != nil {
			logger.WithError(err).Error("failed to construct blob key, stopping archival upload")
			metricsClient.IncCounter(metrics.ArchivalUploadActivityScope, metrics.SysWorkerKeyConstructionFailures)
			return errArchivalUploadActivityConstructKey
		}
		if exists, err := blobExistsRetryForever(blobstoreClient, bucket, key); err != nil {
			logger.WithError(err).Error("failed to check if blob exists already, stopping archival upload")
			metricsClient.IncCounter(metrics.ArchivalUploadActivityScope, metrics.SysWorkerBlobExistsNonRetryableFailures)
			return errArchivalUploadActivityBlobExists
		} else if exists {
			continue
		}
		body, err := json.Marshal(historyBlob)
		if err != nil {
			logger.WithError(err).Error("failed to marshal history blob. stopping archival upload")
			metricsClient.IncCounter(metrics.ArchivalUploadActivityScope, metrics.SysWorkerMarshalBlobFailures)
			return errArchivalUploadActivityMarshalBlob
		}
		tags, err := ConvertHeaderToTags(historyBlob.Header)
		if err != nil {
			logger.WithError(err).Error("failed to convert header to tags, stopping archival upload")
			metricsClient.IncCounter(metrics.ArchivalUploadActivityScope, metrics.SysWorkerConvertHeaderToTagsFailures)
			return errArchivalUploadActivityConvertHeaderToTags
		}
		wrapFunctions := []blob.WrapFn{blob.JSONEncoded()}
		if container.Config.EnableArchivalCompression(domainName) {
			wrapFunctions = append(wrapFunctions, blob.GzipCompressed())
		}
		currBlob, err := blob.Wrap(blob.NewBlob(body, tags), wrapFunctions...)
		if err != nil {
			logger.WithError(err).Error("failed to wrap blob, stopping archival upload")
			metricsClient.IncCounter(metrics.ArchivalUploadActivityScope, metrics.SysWorkerWrapBlobFailures)
			return errArchivalUploadActivityWrapBlob
		}
		if err := blobUploadRetryForever(blobstoreClient, bucket, key, currBlob); err != nil {
			logger.WithError(err).Error("failed to upload blob, stopping archival upload")
			metricsClient.IncCounter(metrics.ArchivalUploadActivityScope, metrics.SysWorkerBlobUploadNonRetryableFailures)
			return errArchivalUploadActivityUploadBlob
		}
	}
	return nil
}

// ArchivalDeleteHistoryActivity deletes the workflow execution history from persistence.
// All retryable errors are retried forever. If an error is returned it is of type archivalActivityNonRetryableErr.
func ArchivalDeleteHistoryActivity(ctx context.Context, request ArchiveRequest) error {
	container := ctx.Value(sysWorkerContainerKey).(*SysWorkerContainer)
	logger := container.Logger.WithFields(bark.Fields{
		logging.TagArchiveRequestDomainID:          request.DomainID,
		logging.TagArchiveRequestWorkflowID:        request.WorkflowID,
		logging.TagArchiveRequestRunID:             request.RunID,
		logging.TagArchiveRequestEventStoreVersion: request.EventStoreVersion,
		logging.TagArchiveRequestLastFirstEventID:  request.LastFirstEventID,
	})
	metricsClient := container.MetricsClient
	if request.EventStoreVersion == persistence.EventStoreVersionV2 {
		err := persistence.DeleteWorkflowExecutionHistoryV2(container.HistoryV2Manager, request.BranchToken, container.Logger)
		if err == nil {
			return nil
		}
		op := func() error {
			return persistence.DeleteWorkflowExecutionHistoryV2(container.HistoryV2Manager, request.BranchToken, container.Logger)
		}
		for err != nil && common.IsPersistenceTransientError(err) {
			err = backoff.Retry(op, common.CreatePersistanceRetryPolicy(), common.IsPersistenceTransientError)
		}
		logger.WithError(err).Error("failed to delete history from events v2")
		metricsClient.IncCounter(metrics.ArchivalDeleteHistoryActivityScope, metrics.SysWorkerDeleteHistoryV2NonRetryableFailures)
		return errDeleteHistoryActivityDeleteFromV2
	}
	deleteHistoryReq := &persistence.DeleteWorkflowExecutionHistoryRequest{
		DomainID: request.DomainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(request.WorkflowID),
			RunId:      common.StringPtr(request.RunID),
		},
	}
	err := container.HistoryManager.DeleteWorkflowExecutionHistory(deleteHistoryReq)
	if err == nil {
		return nil
	}
	op := func() error {
		return container.HistoryManager.DeleteWorkflowExecutionHistory(deleteHistoryReq)
	}
	for err != nil && common.IsPersistenceTransientError(err) {
		err = backoff.Retry(op, common.CreatePersistanceRetryPolicy(), common.IsPersistenceTransientError)
	}
	logger.WithError(err).Error("failed to delete history from events v1")
	metricsClient.IncCounter(metrics.ArchivalDeleteHistoryActivityScope, metrics.SysWorkerDeleteHistoryV1NonRetryableFailures)
	return errDeleteHistoryActivityDeleteFromV1
}

func nextBlobRetryForever(historyBlobItr HistoryBlobIterator) (*HistoryBlob, error) {
	result, err := historyBlobItr.Next()
	if err == nil {
		return result, nil
	}

	op := func() error {
		result, err = historyBlobItr.Next()
		return err
	}
	for err != nil && common.IsPersistenceTransientError(err) {
		err = backoff.Retry(op, common.CreatePersistanceRetryPolicy(), common.IsPersistenceTransientError)
	}
	return result, err
}

func blobExistsRetryForever(blobstoreClient blobstore.Client, bucket string, key blob.Key) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), blobstoreOperationsDefaultTimeout)
	exists, err := blobstoreClient.Exists(ctx, bucket, key)
	cancel()
	for err != nil && common.IsBlobstoreTransientError(err) {
		// blobstoreClient is already retryable so no extra retry/backoff logic is needed here
		ctx, cancel = context.WithTimeout(context.Background(), blobstoreOperationsDefaultTimeout)
		exists, err = blobstoreClient.Exists(ctx, bucket, key)
		cancel()
	}
	return exists, err
}

func blobUploadRetryForever(blobstoreClient blobstore.Client, bucket string, key blob.Key, blob *blob.Blob) error {
	ctx, cancel := context.WithTimeout(context.Background(), blobstoreOperationsDefaultTimeout)
	err := blobstoreClient.Upload(ctx, bucket, key, blob)
	cancel()
	for err != nil && common.IsBlobstoreTransientError(err) {
		// blobstoreClient is already retryable so no extra retry/backoff logic is needed here
		ctx, cancel = context.WithTimeout(context.Background(), blobstoreOperationsDefaultTimeout)
		err = blobstoreClient.Upload(ctx, bucket, key, blob)
		cancel()
	}
	return err
}

func getDomainByIDRetryForever(domainCache cache.DomainCache, id string) (*cache.DomainCacheEntry, error) {
	entry, err := domainCache.GetDomainByID(id)
	if err == nil {
		return entry, nil
	}
	op := func() error {
		entry, err = domainCache.GetDomainByID(id)
		return err
	}
	for err != nil && common.IsPersistenceTransientError(err) {
		backoff.Retry(op, common.CreatePersistanceRetryPolicy(), common.IsPersistenceTransientError)
	}
	return entry, err
}
