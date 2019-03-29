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

package archiver

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/blobstore/blob"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
)

const (
	uploadHistoryActivityFnName = "uploadHistoryActivity"
	deleteHistoryActivityFnName = "deleteHistoryActivity"
	blobstoreTimeout            = 30 * time.Second

	errGetDomainByID = "could not get domain cache entry"
	errGetTags       = "could not get blob tags"
	errUploadBlob    = "could not upload blob"
	errReadBlob      = "could not read blob"
	errEmptyBucket   = "domain is enabled for archival but bucket is not set"
	errConstructBlob = "failed to construct blob"
	errDownloadBlob  = "could not download existing blob"

	errDeleteHistoryV1 = "failed to delete history from events_v1"
	errDeleteHistoryV2 = "failed to delete history from events_v2"
)

var (
	uploadHistoryActivityNonRetryableErrors = []string{errGetDomainByID, errGetTags, errUploadBlob, errReadBlob, errEmptyBucket, errConstructBlob}
	deleteHistoryActivityNonRetryableErrors = []string{errDeleteHistoryV1, errDeleteHistoryV2}
	errContextTimeout                       = errors.New("activity aborted because context timed out")
)

// uploadHistoryActivity is used to upload a workflow execution history to blobstore.
// method will retry all retryable operations until context expires.
// archival will be skipped and no error will be returned if cluster or domain is not figured for archival.
// method will always return either: nil, errContextTimeout or an error from uploadHistoryActivityNonRetryableErrors.
func uploadHistoryActivity(ctx context.Context, request ArchiveRequest) (err error) {
	container := ctx.Value(bootstrapContainerKey).(*BootstrapContainer)
	metricsClient := container.MetricsClient
	sw := metricsClient.StartTimer(metrics.ArchiverUploadHistoryActivityScope, metrics.CadenceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if err == errContextTimeout {
				metricsClient.IncCounter(metrics.ArchiverUploadHistoryActivityScope, metrics.CadenceErrContextTimeoutCounter)
			} else {
				metricsClient.IncCounter(metrics.ArchiverUploadHistoryActivityScope, metrics.ArchiverNonRetryableErrorCount)
			}
		}
	}()

	logger := tagLoggerWithRequest(container.Logger, request).WithField(logging.TagAttempt, activity.GetInfo(ctx).Attempt)
	domainCache := container.DomainCache
	clusterMetadata := container.ClusterMetadata
	domainCacheEntry, err := getDomainByID(ctx, domainCache, request.DomainID)
	if err != nil {
		logging.LogFailArchivalUploadAttempt(logger, err, "could not get domain cache entry", "", "")
		return err
	}
	if clusterMetadata.ArchivalConfig().GetArchivalStatus() != cluster.ArchivalEnabled {
		logging.LogSkipArchivalUpload(logger, "cluster is not enabled for archival")
		metricsClient.IncCounter(metrics.ArchiverUploadHistoryActivityScope, metrics.ArchiverSkipUploadCount)
		return nil
	}
	if domainCacheEntry.GetConfig().ArchivalStatus != shared.ArchivalStatusEnabled {
		logging.LogSkipArchivalUpload(logger, "domain is not enabled for archival")
		metricsClient.IncCounter(metrics.ArchiverUploadHistoryActivityScope, metrics.ArchiverSkipUploadCount)
		return nil
	}
	bucket := domainCacheEntry.GetConfig().ArchivalBucket
	if len(bucket) == 0 {
		// this should not be able to occur, if domain enables archival bucket should always be set
		logging.LogFailArchivalUploadAttempt(logger, err, "domain enables archival but does not have a bucket set", "", "")
		return cadence.NewCustomError(errEmptyBucket)
	}
	domainName := domainCacheEntry.GetInfo().Name
	clusterName := container.ClusterMetadata.GetCurrentClusterName()
	historyBlobReader := container.HistoryBlobReader
	if historyBlobReader == nil { // only will be set by testing code
		historyBlobReader = NewHistoryBlobReader(NewHistoryBlobIterator(request, container, domainName, clusterName))
	}
	blobstoreClient := container.Blobstore
	handledLastBlob := false
	for pageToken := common.FirstBlobPageToken; !handledLastBlob; pageToken++ {
		key, err := NewHistoryBlobKey(request.DomainID, request.WorkflowID, request.RunID, pageToken)
		if err != nil {
			logging.LogFailArchivalUploadAttempt(logger, err, "could not construct blob key", bucket, "")
			return cadence.NewCustomError(errConstructBlob)
		}
		tags, err := getTags(ctx, blobstoreClient, bucket, key)
		if err != nil && err != blobstore.ErrBlobNotExists {
			logging.LogFailArchivalUploadAttempt(logger, err, "could not get blob tags", bucket, key.String())
			return err
		}
		runConstTest := false
		if err == nil {
			handledLastBlob = IsLast(tags)
			// this is a sampling based sanity check used to ensure deterministic blob construction
			// is operating as expected, the correctness of archival depends on this deterministic construction
			runConstTest = runConstructionCheck(container.Config.DeterministicConstructionCheckProbability())
			if !runConstTest {
				continue
			}
		}
		historyBlob, err := getBlob(ctx, historyBlobReader, pageToken)
		if err != nil {
			logging.LogFailArchivalUploadAttempt(logger, err, "could not get history blob from reader", bucket, "")
			return err
		}
		blob, reason, err := constructBlob(historyBlob, container.Config.EnableArchivalCompression(domainName))
		if err != nil {
			logging.LogFailArchivalUploadAttempt(logger, err, reason, bucket, key.String())
			return cadence.NewCustomError(errConstructBlob)
		}
		if runConstTest {
			existingBlob, err := downloadBlob(ctx, blobstoreClient, bucket, key)
			if err != nil {
				logger.WithError(err).Error("failed to download blob for deterministic construction verification")
				metricsClient.IncCounter(metrics.ArchiverUploadHistoryActivityScope, metrics.ArchiverCouldNotRunDeterministicConstructionCheckCount)
			} else if !blob.Equal(existingBlob) {
				logger.Error("deterministic construction check failed")
				metricsClient.IncCounter(metrics.ArchiverUploadHistoryActivityScope, metrics.ArchiverDeterministicConstructionCheckFailedCount)
			}
			continue
		}
		if err := uploadBlob(ctx, blobstoreClient, bucket, key, blob); err != nil {
			logging.LogFailArchivalUploadAttempt(logger, err, "could not upload blob", bucket, key.String())
			return err
		}
		handledLastBlob = *historyBlob.Header.IsLast
	}
	return nil
}

// deleteHistoryActivity deletes workflow execution history from persistence.
// method will retry all retryable operations until context expires.
// method will always return either: nil, contextTimeoutErr or an error from deleteHistoryActivityNonRetryableErrors.
func deleteHistoryActivity(ctx context.Context, request ArchiveRequest) (err error) {
	container := ctx.Value(bootstrapContainerKey).(*BootstrapContainer)
	metricsClient := container.MetricsClient
	sw := metricsClient.StartTimer(metrics.ArchiverDeleteHistoryActivityScope, metrics.CadenceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if err == errContextTimeout {
				metricsClient.IncCounter(metrics.ArchiverDeleteHistoryActivityScope, metrics.CadenceErrContextTimeoutCounter)
			} else {
				metricsClient.IncCounter(metrics.ArchiverDeleteHistoryActivityScope, metrics.ArchiverNonRetryableErrorCount)
			}
		}
	}()
	logger := tagLoggerWithRequest(container.Logger, request).WithField(logging.TagAttempt, activity.GetInfo(ctx).Attempt)
	if request.EventStoreVersion == persistence.EventStoreVersionV2 {
		if err := deleteHistoryV2(ctx, container, request); err != nil {
			logger.WithError(err).Error("failed to delete history from events v2")
			return err
		}
		return nil
	}
	if err := deleteHistoryV1(ctx, container, request); err != nil {
		logger.WithError(err).Error("failed to delete history from events v1")
		return err
	}
	return nil
}

func getBlob(ctx context.Context, historyBlobReader HistoryBlobReader, blobPage int) (*HistoryBlob, error) {
	blob, err := historyBlobReader.GetBlob(blobPage)
	op := func() error {
		blob, err = historyBlobReader.GetBlob(blobPage)
		return err
	}
	for err != nil {
		activity.RecordHeartbeat(ctx)
		if !common.IsPersistenceTransientError(err) {
			return nil, cadence.NewCustomError(errReadBlob)
		}
		if contextExpired(ctx) {
			return nil, errContextTimeout
		}
		err = backoff.Retry(op, common.CreatePersistanceRetryPolicy(), common.IsPersistenceTransientError)
	}
	return blob, nil
}

func getTags(ctx context.Context, blobstoreClient blobstore.Client, bucket string, key blob.Key) (map[string]string, error) {
	bCtx, cancel := context.WithTimeout(ctx, blobstoreTimeout)
	tags, err := blobstoreClient.GetTags(bCtx, bucket, key)
	cancel()
	for err != nil {
		activity.RecordHeartbeat(ctx)
		if err == blobstore.ErrBlobNotExists {
			return nil, err
		}
		if !blobstoreClient.IsRetryableError(err) {
			return nil, cadence.NewCustomError(errGetTags)
		}
		if contextExpired(ctx) {
			return nil, errContextTimeout
		}
		bCtx, cancel = context.WithTimeout(ctx, blobstoreTimeout)
		tags, err = blobstoreClient.GetTags(bCtx, bucket, key)
		cancel()
	}
	return tags, nil
}

func uploadBlob(ctx context.Context, blobstoreClient blobstore.Client, bucket string, key blob.Key, blob *blob.Blob) error {
	bCtx, cancel := context.WithTimeout(ctx, blobstoreTimeout)
	err := blobstoreClient.Upload(bCtx, bucket, key, blob)
	cancel()
	for err != nil {
		activity.RecordHeartbeat(ctx)
		if !blobstoreClient.IsRetryableError(err) {
			return cadence.NewCustomError(errUploadBlob)
		}
		if contextExpired(ctx) {
			return errContextTimeout
		}
		bCtx, cancel = context.WithTimeout(ctx, blobstoreTimeout)
		err = blobstoreClient.Upload(bCtx, bucket, key, blob)
		cancel()
	}
	return nil
}

func downloadBlob(ctx context.Context, blobstoreClient blobstore.Client, bucket string, key blob.Key) (*blob.Blob, error) {
	bCtx, cancel := context.WithTimeout(ctx, blobstoreTimeout)
	blob, err := blobstoreClient.Download(bCtx, bucket, key)
	cancel()
	for err != nil {
		activity.RecordHeartbeat(ctx)
		if !blobstoreClient.IsRetryableError(err) {
			return nil, cadence.NewCustomError(errDownloadBlob)
		}
		if contextExpired(ctx) {
			return nil, errContextTimeout
		}
		bCtx, cancel = context.WithTimeout(ctx, blobstoreTimeout)
		blob, err = blobstoreClient.Download(bCtx, bucket, key)
		cancel()
	}
	return blob, nil
}

func getDomainByID(ctx context.Context, domainCache cache.DomainCache, id string) (*cache.DomainCacheEntry, error) {
	entry, err := domainCache.GetDomainByID(id)
	op := func() error {
		entry, err = domainCache.GetDomainByID(id)
		return err
	}
	for err != nil {
		activity.RecordHeartbeat(ctx)
		if !common.IsPersistenceTransientError(err) {
			return nil, cadence.NewCustomError(errGetDomainByID)
		}
		if contextExpired(ctx) {
			return nil, errContextTimeout
		}
		err = backoff.Retry(op, common.CreatePersistanceRetryPolicy(), common.IsPersistenceTransientError)
	}
	return entry, nil
}

func constructBlob(historyBlob *HistoryBlob, enableCompression bool) (*blob.Blob, string, error) {
	body, err := json.Marshal(historyBlob)
	if err != nil {
		return nil, "failed to serialize blob", err
	}
	tags, err := ConvertHeaderToTags(historyBlob.Header)
	if err != nil {
		return nil, "failed to convert header to tags", err
	}
	wrapFunctions := []blob.WrapFn{blob.JSONEncoded()}
	if enableCompression {
		wrapFunctions = append(wrapFunctions, blob.GzipCompressed())
	}
	blob, err := blob.Wrap(blob.NewBlob(body, tags), wrapFunctions...)
	if err != nil {
		return nil, "failed to wrap blob", err
	}
	return blob, "", nil
}

func deleteHistoryV1(ctx context.Context, container *BootstrapContainer, request ArchiveRequest) error {
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
	for err != nil {
		activity.RecordHeartbeat(ctx)
		if !common.IsPersistenceTransientError(err) {
			return cadence.NewCustomError(errDeleteHistoryV1)
		}
		if contextExpired(ctx) {
			return errContextTimeout
		}
		err = backoff.Retry(op, common.CreatePersistanceRetryPolicy(), common.IsPersistenceTransientError)
	}
	return nil
}

func deleteHistoryV2(ctx context.Context, container *BootstrapContainer, request ArchiveRequest) error {
	err := persistence.DeleteWorkflowExecutionHistoryV2(container.HistoryV2Manager, request.BranchToken, container.Logger)
	if err == nil {
		return nil
	}
	op := func() error {
		return persistence.DeleteWorkflowExecutionHistoryV2(container.HistoryV2Manager, request.BranchToken, container.Logger)
	}
	for err != nil {
		activity.RecordHeartbeat(ctx)
		if !common.IsPersistenceTransientError(err) {
			return cadence.NewCustomError(errDeleteHistoryV2)
		}
		if contextExpired(ctx) {
			return errContextTimeout
		}
		err = backoff.Retry(op, common.CreatePersistanceRetryPolicy(), common.IsPersistenceTransientError)
	}
	return nil
}

func contextExpired(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func runConstructionCheck(probability float64) bool {
	if probability <= 0 {
		return false
	}
	if probability >= 1.0 {
		return true
	}
	return rand.Intn(int(1.0/probability)) == 0
}
