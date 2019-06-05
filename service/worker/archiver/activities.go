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
	"errors"
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/blobstore/blob"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
)

const (
	uploadHistoryActivityFnName = "uploadHistoryActivity"
	deleteBlobActivityFnName    = "deleteBlobActivity"
	deleteHistoryActivityFnName = "deleteHistoryActivity"
	blobstoreTimeout            = 30 * time.Second

	errGetDomainByID = "could not get domain cache entry"
	errConstructKey  = "could not construct blob key"
	errGetTags       = "could not get blob tags"
	errUploadBlob    = "could not upload blob"
	errReadBlob      = "could not read blob"
	errEmptyBucket   = "domain is enabled for archival but bucket is not set"
	errConstructBlob = "failed to construct blob"
	errDownloadBlob  = "could not download existing blob"
	errDeleteBlob    = "could not delete existing blob"

	errDeleteHistoryV1 = "failed to delete history from events_v1"
	errDeleteHistoryV2 = "failed to delete history from events_v2"

	errHistoryMutated = "history was mutated during uploading"
)

var (
	uploadHistoryActivityNonRetryableErrors = []string{errGetDomainByID, errConstructKey, errGetTags, errUploadBlob, errReadBlob, errEmptyBucket, errConstructBlob, errDownloadBlob, errHistoryMutated}
	deleteBlobActivityNonRetryableErrors    = []string{errConstructKey, errGetTags, errUploadBlob, errEmptyBucket, errDeleteBlob}
	deleteHistoryActivityNonRetryableErrors = []string{errDeleteHistoryV1, errDeleteHistoryV2}
	errContextTimeout                       = errors.New("activity aborted because context timed out")
)

const (
	uploadErrorMsg = "Archival upload attempt is giving up, possibly could retry."
	uploadSkipMsg  = "Archival upload request is being skipped, will not retry."
)

// uploadHistoryActivity is used to upload a workflow execution history to blobstore.
// method will retry all retryable operations until context expires.
// archival will be skipped and no error will be returned if cluster or domain is not figured for archival.
// method will always return either: nil, errContextTimeout or an error from uploadHistoryActivityNonRetryableErrors.
func uploadHistoryActivity(ctx context.Context, request ArchiveRequest) (err error) {
	container := ctx.Value(bootstrapContainerKey).(*BootstrapContainer)
	scope := container.MetricsClient.Scope(metrics.ArchiverUploadHistoryActivityScope, metrics.DomainTag(request.DomainName))
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if err == errContextTimeout {
				scope.IncCounter(metrics.CadenceErrContextTimeoutCounter)
			} else {
				scope.IncCounter(metrics.ArchiverNonRetryableErrorCount)
			}
		}
	}()

	logger := tagLoggerWithRequest(container.Logger, request).WithTags(tag.Attempt(activity.GetInfo(ctx).Attempt))
	domainCache := container.DomainCache
	clusterMetadata := container.ClusterMetadata
	domainCacheEntry, err := getDomainByID(ctx, domainCache, request.DomainID)
	if err != nil {
		logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(errorDetails(err)), tag.Error(err))
		return err
	}
	if clusterMetadata.ArchivalConfig().GetArchivalStatus() != cluster.ArchivalEnabled {
		logger.Error(uploadSkipMsg, tag.ArchivalUploadFailReason("cluster is not enabled for archival"))
		scope.IncCounter(metrics.ArchiverSkipUploadCount)
		return nil
	}
	if domainCacheEntry.GetConfig().ArchivalStatus != shared.ArchivalStatusEnabled {
		logger.Error(uploadSkipMsg, tag.ArchivalUploadFailReason("domain is not enabled for archival"))
		scope.IncCounter(metrics.ArchiverSkipUploadCount)
		return nil
	}
	if err := validateArchivalRequest(&request); err != nil {
		logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(err.Error()))
		return err
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
		key, err := NewHistoryBlobKey(request.DomainID, request.WorkflowID, request.RunID, request.CloseFailoverVersion, pageToken)
		if err != nil {
			logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason("could not construct blob key"))
			return cadence.NewCustomError(errConstructKey, err.Error())
		}

		tags, err := getTags(ctx, blobstoreClient, request.BucketName, key)
		if err != nil && err != blobstore.ErrBlobNotExists {
			logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(errorDetails(err)), tag.ArchivalBlobKey(key.String()), tag.Error(err))
			return err
		}

		runConstTest := false
		blobAlreadyExists := err == nil
		if blobAlreadyExists {
			handledLastBlob = IsLast(tags)
			// this is a sampling based sanity check used to ensure deterministic blob construction
			// is operating as expected, the correctness of archival depends on this deterministic construction
			runConstTest = shouldRun(container.Config.DeterministicConstructionCheckProbability())
			if !runConstTest {
				continue
			}
			scope.IncCounter(metrics.ArchiverRunningDeterministicConstructionCheckCount)
		}

		historyBlob, err := getBlob(ctx, historyBlobReader, pageToken)
		if err != nil {
			logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(errorDetails(err)), tag.Error(err))
			return err
		}

		if historyMutated(historyBlob, &request) {
			scope.IncCounter(metrics.ArchiverHistoryMutatedCount)
			logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason("history was mutated during archiving"))
			return cadence.NewCustomError(errHistoryMutated)
		}

		if runConstTest {
			// some tags are specific to the cluster and time a blob was uploaded from/when
			// this only updates those specific tags, all other parts of the blob are left unchanged
			modifyBlobForConstCheck(historyBlob, tags)
		}

		blob, reason, err := constructBlob(historyBlob, container.Config.EnableArchivalCompression(domainName))
		if err != nil {
			logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(reason), tag.ArchivalBlobKey(key.String()))
			return cadence.NewCustomError(errConstructBlob, err.Error())
		}
		if runConstTest {
			existingBlob, err := downloadBlob(ctx, blobstoreClient, request.BucketName, key)
			if err != nil {
				logger.Error("failed to download blob for deterministic construction verification", tag.ArchivalUploadFailReason(errorDetails(err)), tag.Error(err))
				scope.IncCounter(metrics.ArchiverCouldNotRunDeterministicConstructionCheckCount)
			} else if !blob.Equal(existingBlob) {
				logger.Error("deterministic construction check failed")
				scope.IncCounter(metrics.ArchiverDeterministicConstructionCheckFailedCount)
			}
			continue
		}

		if err := uploadBlob(ctx, blobstoreClient, request.BucketName, key, blob); err != nil {
			logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(errorDetails(err)), tag.ArchivalBlobKey(key.String()), tag.Error(err))
			return err
		}
		handledLastBlob = *historyBlob.Header.IsLast
	}
	indexBlobKey, err := NewHistoryIndexBlobKey(request.DomainID, request.WorkflowID, request.RunID)
	if err != nil {
		logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason("could not construct index blob key"))
		return cadence.NewCustomError(errConstructKey, err.Error())
	}
	existingVersions, err := getTags(ctx, blobstoreClient, request.BucketName, indexBlobKey)
	if err != nil && err != blobstore.ErrBlobNotExists {
		logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(errorDetails(err)), tag.ArchivalBlobKey(indexBlobKey.String()), tag.Error(err))
		return err
	}
	indexBlobWithVersion := addVersion(request.CloseFailoverVersion, existingVersions)
	if indexBlobWithVersion == nil {
		return nil
	}
	if err := uploadBlob(ctx, blobstoreClient, request.BucketName, indexBlobKey, indexBlobWithVersion); err != nil {
		logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(errorDetails(err)), tag.ArchivalBlobKey(indexBlobKey.String()), tag.Error(err))
		return err
	}
	return nil
}

// deleteHistoryActivity deletes workflow execution history from persistence.
// method will retry all retryable operations until context expires.
// method will always return either: nil, contextTimeoutErr or an error from deleteHistoryActivityNonRetryableErrors.
func deleteHistoryActivity(ctx context.Context, request ArchiveRequest) (err error) {
	container := ctx.Value(bootstrapContainerKey).(*BootstrapContainer)
	scope := container.MetricsClient.Scope(metrics.ArchiverDeleteHistoryActivityScope, metrics.DomainTag(request.DomainName))
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if err == errContextTimeout {
				scope.IncCounter(metrics.CadenceErrContextTimeoutCounter)
			} else {
				scope.IncCounter(metrics.ArchiverNonRetryableErrorCount)
			}
		}
	}()
	logger := tagLoggerWithRequest(container.Logger, request).WithTags(tag.Attempt(activity.GetInfo(ctx).Attempt))
	if request.EventStoreVersion == persistence.EventStoreVersionV2 {
		if err := deleteHistoryV2(ctx, container, request); err != nil {
			logger.Error("failed to delete history from events v2", tag.ArchivalDeleteHistoryFailReason(errorDetails(err)), tag.Error(err))
			return err
		}
		return nil
	}
	if err := deleteHistoryV1(ctx, container, request); err != nil {
		logger.Error("failed to delete history from events v1", tag.ArchivalDeleteHistoryFailReason(errorDetails(err)), tag.Error(err))
		return err
	}
	return nil
}

// deleteBlobActivity deletes uploaded history blobs from blob store.
// method will retry all retryable operations until context expires.
// method will always return either: nil, contextTimeoutErr or an error from deleteBlobActivityNonRetryableErrors.
// TODO: after heartbeating during uploadHistoryActivity is implemented, this activity should take
// a list of uploaded blob keys as input.
func deleteBlobActivity(ctx context.Context, request ArchiveRequest) (err error) {
	container := ctx.Value(bootstrapContainerKey).(*BootstrapContainer)
	scope := container.MetricsClient.Scope(metrics.ArchiverDeleteBlobActivityScope, metrics.DomainTag(request.DomainName))
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if err == errContextTimeout {
				scope.IncCounter(metrics.CadenceErrContextTimeoutCounter)
			} else {
				scope.IncCounter(metrics.ArchiverNonRetryableErrorCount)
			}
		}
	}()
	logger := tagLoggerWithRequest(container.Logger, request).WithTags(tag.Attempt(activity.GetInfo(ctx).Attempt))
	blobstoreClient := container.Blobstore

	if err := validateArchivalRequest(&request); err != nil {
		logger.Error(errEmptyBucket)
		return err
	}

	// delete index blob
	indexBlobKey, err := NewHistoryIndexBlobKey(request.DomainID, request.WorkflowID, request.RunID)
	if err != nil {
		logger.Error("could not construct index blob key", tag.Error(err))
		return cadence.NewCustomError(errConstructKey, err.Error())
	}
	existingVersions, err := getTags(ctx, blobstoreClient, request.BucketName, indexBlobKey)
	if err != nil && err != blobstore.ErrBlobNotExists {
		logger.Error("could not get index blob tags", tag.ArchivalBlobKey(indexBlobKey.String()), tag.ArchivalDeleteHistoryFailReason(errorDetails(err)), tag.Error(err))
		return err
	}
	if err != blobstore.ErrBlobNotExists {
		if indexBlobWithoutVersion := deleteVersion(request.CloseFailoverVersion, existingVersions); indexBlobWithoutVersion != nil {
			// We changed the existing versions, either upload the new blob or delete the exising one
			if len(indexBlobWithoutVersion.Tags) == 0 {
				// We removed the last version in the tag, delete the whole index blob.
				if _, err := deleteBlob(ctx, blobstoreClient, request.BucketName, indexBlobKey); err != nil {
					logger.Error("failed to delete index blob", tag.ArchivalBlobKey(indexBlobKey.String()), tag.ArchivalDeleteHistoryFailReason(errorDetails(err)), tag.Error(err))
					return err
				}
			} else {
				if err := uploadBlob(ctx, blobstoreClient, request.BucketName, indexBlobKey, indexBlobWithoutVersion); err != nil {
					logger.Error("could not upload index blob", tag.ArchivalBlobKey(indexBlobKey.String()), tag.ArchivalDeleteHistoryFailReason(errorDetails(err)), tag.Error(err))
					return err
				}
			}
		}
	}

	pageToken := common.FirstBlobPageToken
	if activity.HasHeartbeatDetails(ctx) {
		var prevPageToken int
		if err := activity.GetHeartbeatDetails(ctx, &prevPageToken); err == nil {
			pageToken = prevPageToken + 1
		}
	}

	startPageToken := pageToken
	for {
		key, err := NewHistoryBlobKey(request.DomainID, request.WorkflowID, request.RunID, request.CloseFailoverVersion, pageToken)
		if err != nil {
			logger.Error("could not construct blob key", tag.Error(err))
			return cadence.NewCustomError(errConstructKey, err.Error())
		}

		deleted, err := deleteBlob(ctx, blobstoreClient, request.BucketName, key)
		if err != nil {
			logger.Error("failed to delete blob", tag.ArchivalBlobKey(key.String()), tag.ArchivalDeleteHistoryFailReason(errorDetails(err)), tag.Error(err))
			return err
		}
		if !deleted && pageToken != startPageToken {
			// Blob does not exist. This means we have deleted all uploaded blobs.
			// Note we should not break if the first page does not exist as it's possible that a blob has been deleted,
			// but the worker restarts before heartbeat is recorded.
			break
		}
		activity.RecordHeartbeat(ctx, pageToken)
		pageToken++
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
		if !common.IsPersistenceTransientError(err) {
			return nil, cadence.NewCustomError(errReadBlob, err.Error())
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
		if err == blobstore.ErrBlobNotExists {
			return nil, err
		}
		if !blobstoreClient.IsRetryableError(err) {
			return nil, cadence.NewCustomError(errGetTags, err.Error())
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
		if !blobstoreClient.IsRetryableError(err) {
			return cadence.NewCustomError(errUploadBlob, err.Error())
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
		if !blobstoreClient.IsRetryableError(err) {
			return nil, cadence.NewCustomError(errDownloadBlob, err.Error())
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

// deleteBlob should not return error when blob does not exist, it should return false, nil in such case
func deleteBlob(ctx context.Context, blobstoreClient blobstore.Client, bucket string, key blob.Key) (bool, error) {
	dCtx, cancel := context.WithTimeout(ctx, blobstoreTimeout)
	deleted, err := blobstoreClient.Delete(dCtx, bucket, key)
	cancel()
	for err != nil {
		if err == blobstore.ErrBlobNotExists {
			return false, nil
		}
		if !blobstoreClient.IsRetryableError(err) {
			return deleted, cadence.NewCustomError(errDeleteBlob, err.Error())
		}
		if contextExpired(ctx) {
			return deleted, errContextTimeout
		}
		dCtx, cancel = context.WithTimeout(ctx, blobstoreTimeout)
		deleted, err = blobstoreClient.Delete(dCtx, bucket, key)
		cancel()
	}
	return deleted, nil
}

func getDomainByID(ctx context.Context, domainCache cache.DomainCache, id string) (*cache.DomainCacheEntry, error) {
	entry, err := domainCache.GetDomainByID(id)
	op := func() error {
		entry, err = domainCache.GetDomainByID(id)
		return err
	}
	for err != nil {
		if !common.IsPersistenceTransientError(err) {
			return nil, cadence.NewCustomError(errGetDomainByID, err.Error())
		}
		if contextExpired(ctx) {
			return nil, errContextTimeout
		}
		err = backoff.Retry(op, common.CreatePersistanceRetryPolicy(), common.IsPersistenceTransientError)
	}
	return entry, nil
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
		if !common.IsPersistenceTransientError(err) {
			return cadence.NewCustomError(errDeleteHistoryV1, err.Error())
		}
		if contextExpired(ctx) {
			return errContextTimeout
		}
		err = backoff.Retry(op, common.CreatePersistanceRetryPolicy(), common.IsPersistenceTransientError)
	}
	return nil
}

func deleteHistoryV2(ctx context.Context, container *BootstrapContainer, request ArchiveRequest) error {
	err := persistence.DeleteWorkflowExecutionHistoryV2(container.HistoryV2Manager, request.BranchToken, common.IntPtr(request.ShardID), container.Logger)
	if err == nil {
		return nil
	}
	op := func() error {
		return persistence.DeleteWorkflowExecutionHistoryV2(container.HistoryV2Manager, request.BranchToken, common.IntPtr(request.ShardID), container.Logger)
	}
	for err != nil {
		if !common.IsPersistenceTransientError(err) {
			return cadence.NewCustomError(errDeleteHistoryV2, err.Error())
		}
		if contextExpired(ctx) {
			return errContextTimeout
		}
		err = backoff.Retry(op, common.CreatePersistanceRetryPolicy(), common.IsPersistenceTransientError)
	}
	return nil
}
