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
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/blobstore/blob"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
)

type (
	// HistoryBlobUploader is used to upload history blobs
	HistoryBlobUploader interface {
		UploadHistory(context.Context, *ArchiveRequest) (*uploadResult, error)
	}

	historyBlobUploader struct {
		request   *ArchiveRequest
		container *BootstrapContainer
		logger    log.Logger
		scope     metrics.Scope

		historyBlobReader          HistoryBlobReader
		historyBlobIterator        HistoryBlobIterator
		uploadedHistoryEventHashes []uint64
		uploadedBlobs              []string
	}
)

// NewHistoryBlobUploader returns a new HistoryBlobUploader
func NewHistoryBlobUploader() HistoryBlobUploader {
	return &historyBlobUploader{}
}

// UploadHistory is used to upload entire workflow history to blobstore.
// method will retry all retryable operations until context expires.
// archival will be skipped and no error will be returned if cluster or domain is not figured for archival.
// an error will be returned if context timeout, request is invalid, or failed to get domain cache entry.
// all other errors (and the error details) will be included in the uploadResult.
// the result will also contain a list of blob keys to delete when there's an error.
func (h *historyBlobUploader) UploadHistory(ctx context.Context, request *ArchiveRequest) (result *uploadResult, err error) {
	h.container = ctx.Value(bootstrapContainerKey).(*BootstrapContainer)
	h.request = request
	h.logger = tagLoggerWithRequest(tagLoggerWithActivityInfo(h.container.Logger, activity.GetInfo(ctx)), *request)
	h.scope = h.container.MetricsClient.Scope(metrics.ArchiverUploadHistoryActivityScope, metrics.DomainTag(request.DomainName))
	sw := h.scope.StartTimer(metrics.CadenceLatency)
	progress := h.loadPrevProgress(ctx)
	h.uploadedBlobs = progress.UploadedBlobs

	defer func() {
		sw.Stop()
		result, err = getUploadHistoryActivityResponse(h.uploadedBlobs, err)
		if err != nil || (result != nil && result.ErrorWithDetails != errContextTimeout.Error()) {
			h.scope.IncCounter(metrics.ArchiverNonRetryableErrorCount)
		}
	}()

	domainCache := h.container.DomainCache
	clusterMetadata := h.container.ClusterMetadata
	domainCacheEntry, err := getDomainByID(ctx, domainCache, h.request.DomainID)
	if err != nil {
		h.logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(errorDetails(err)), tag.Error(err))
		return result, err
	}
	if clusterMetadata.ArchivalConfig().GetArchivalStatus() != cluster.ArchivalEnabled {
		h.logger.Error(uploadSkipMsg, tag.ArchivalUploadFailReason("cluster is not enabled for archival"))
		h.scope.IncCounter(metrics.ArchiverSkipUploadCount)
		return result, nil
	}
	if domainCacheEntry.GetConfig().ArchivalStatus != shared.ArchivalStatusEnabled {
		h.logger.Error(uploadSkipMsg, tag.ArchivalUploadFailReason("domain is not enabled for archival"))
		h.scope.IncCounter(metrics.ArchiverSkipUploadCount)
		return result, nil
	}
	if err := validateArchivalRequest(h.request); err != nil {
		h.logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(err.Error()))
		return result, err
	}

	domainName := domainCacheEntry.GetInfo().Name
	clusterName := h.container.ClusterMetadata.GetCurrentClusterName()

	if err := h.construtHistoryReaderAndIterator(domainName, clusterName, progress.IteratorState); err != nil {
		return result, err
	}

	var totalUploadSize int64
	handledLastBlob := progress.HandledLastBlob
	runBlobIntegrityCheck := !activity.HasHeartbeatDetails(ctx) && shouldRun(h.container.Config.BlobIntegrityCheckProbability())
	for pageToken := progress.BlobPageToken; !handledLastBlob; pageToken++ {
		var currBlobSize int64
		currBlobSize, handledLastBlob, err = h.uploadSingleHistoryBlob(ctx, domainName, pageToken, runBlobIntegrityCheck)
		if err != nil {
			return result, err
		}
		totalUploadSize = totalUploadSize + currBlobSize
	}
	h.scope.RecordTimer(metrics.ArchiverTotalUploadSize, time.Duration(totalUploadSize))

	if err := h.uploadIndexBlob(ctx); err != nil {
		return result, err
	}

	if runBlobIntegrityCheck {
		h.integrityCheckHelper(ctx)
	}
	return result, nil
}

func (h *historyBlobUploader) loadPrevProgress(ctx context.Context) *uploadProgress {
	progress := &uploadProgress{
		BlobPageToken: common.FirstBlobPageToken,
	}
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &progress); err != nil {
			// reset to initial state
			progress = &uploadProgress{
				UploadedBlobs:   nil,
				IteratorState:   nil,
				HandledLastBlob: false,
				BlobPageToken:   common.FirstBlobPageToken,
			}
			h.logger.Error("failed to get previous progress, start from beginning")
			return progress
		}
		progress.BlobPageToken++
	}
	return progress
}

func (h *historyBlobUploader) recordCurrentProgress(ctx context.Context, currKey blob.Key, currPage int, handledLastBlob bool) {
	currIteratorState, err := h.historyBlobIterator.GetState()
	if err != nil {
		h.logger.Error("failed to get history blob iterator state", tag.Error(err))
		return
	}

	progress := uploadProgress{
		UploadedBlobs:   h.uploadedBlobs,
		IteratorState:   currIteratorState,
		BlobPageToken:   currPage,
		HandledLastBlob: handledLastBlob,
	}
	activity.RecordHeartbeat(ctx, progress)
}

func (h *historyBlobUploader) uploadSingleHistoryBlob(ctx context.Context, domainName string, pageToken int, runBlobIntegrityCheck bool) (int64, bool, error) {
	key, err := NewHistoryBlobKey(h.request.DomainID, h.request.WorkflowID, h.request.RunID, h.request.CloseFailoverVersion, pageToken)
	if err != nil {
		h.logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason("could not construct blob key"))
		return 0, false, cadence.NewCustomError(errConstructKey, err.Error())
	}

	tags, err := getTags(ctx, h.container.Blobstore, h.request.BucketName, key)
	if err != nil && err != blobstore.ErrBlobNotExists {
		h.logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(errorDetails(err)), tag.ArchivalBlobKey(key.String()), tag.Error(err))
		return 0, false, err
	}

	var handledLastBlob bool
	runConstTest := false
	blobAlreadyExists := err == nil
	if blobAlreadyExists {
		h.scope.IncCounter(metrics.ArchiverBlobExistsCount)
		handledLastBlob = IsLast(tags)
		// this is a sampling based sanity check used to ensure deterministic blob construction
		// is operating as expected, the correctness of archival depends on this deterministic construction
		runConstTest = shouldRun(h.container.Config.DeterministicConstructionCheckProbability())
		if !runConstTest {
			return 0, handledLastBlob, nil
		}
		h.scope.IncCounter(metrics.ArchiverRunningDeterministicConstructionCheckCount)
	}

	historyBlob, err := getBlob(ctx, h.historyBlobReader, pageToken)
	if err != nil {
		h.logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(errorDetails(err)), tag.Error(err))
		return 0, false, err
	}
	if runBlobIntegrityCheck {
		for _, e := range historyBlob.Body.Events {
			h.uploadedHistoryEventHashes = append(h.uploadedHistoryEventHashes, hash(e.String()))
		}
	}

	if historyMutated(historyBlob, h.request) {
		h.scope.IncCounter(metrics.ArchiverHistoryMutatedCount)
		h.logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason("history was mutated during archiving"))
		return 0, false, cadence.NewCustomError(errHistoryMutated)
	}

	if runConstTest {
		// some tags are specific to the cluster and time a blob was uploaded from/when
		// this only updates those specific tags, all other parts of the blob are left unchanged
		modifyBlobForConstCheck(historyBlob, tags)
	}

	blob, reason, err := constructBlob(historyBlob, h.container.Config.EnableArchivalCompression(domainName))
	if err != nil {
		h.logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(reason), tag.ArchivalBlobKey(key.String()))
		return 0, false, cadence.NewCustomError(errConstructBlob, err.Error())
	}

	if runConstTest {
		h.deterministicTestHelper(ctx, blob, key)
		return 0, handledLastBlob, nil
	}

	if err := uploadBlob(ctx, h.container.Blobstore, h.request.BucketName, key, blob); err != nil {
		h.logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(errorDetails(err)), tag.ArchivalBlobKey(key.String()), tag.Error(err))
		return 0, false, err
	}
	h.uploadedBlobs = append(h.uploadedBlobs, key.String())
	currBlobSize := int64(len(blob.Body))
	h.scope.RecordTimer(metrics.ArchiverBlobSize, time.Duration(currBlobSize))

	handledLastBlob = *historyBlob.Header.IsLast
	h.recordCurrentProgress(ctx, key, pageToken, handledLastBlob)
	return currBlobSize, handledLastBlob, nil
}

func (h *historyBlobUploader) construtHistoryReaderAndIterator(domainName, clusterName string, initialState []byte) error {
	var err error
	h.historyBlobIterator, err = NewHistoryBlobIterator(*h.request, h.container, domainName, clusterName, initialState)
	if err != nil {
		h.logger.Error("failed to decode history blob iterator state, start from beginning", tag.Error(err))
		h.historyBlobIterator, err = NewHistoryBlobIterator(*h.request, h.container, domainName, clusterName, nil)
		if err != nil {
			// this should never occur
			h.logger.Error("failed to create new history blob iterator", tag.Error(err))
			return err
		}
	}
	h.historyBlobReader = h.container.HistoryBlobReader
	if h.historyBlobReader == nil { // only will be set by testing code
		h.historyBlobReader = NewHistoryBlobReader(h.historyBlobIterator)
	}
	return nil
}

func (h *historyBlobUploader) deterministicTestHelper(ctx context.Context, blob *blob.Blob, key blob.Key) {
	existingBlob, err := downloadBlob(ctx, h.container.Blobstore, h.request.BucketName, key)
	if err != nil {
		h.logger.Error("failed to download blob for deterministic construction verification", tag.ArchivalUploadFailReason(errorDetails(err)), tag.Error(err))
		h.scope.IncCounter(metrics.ArchiverCouldNotRunDeterministicConstructionCheckCount)
		return
	}
	if equal, reason := blob.EqualWithDetails(existingBlob); !equal {
		h.logger.Error("deterministic construction check failed",
			tag.ArchivalBlobKey(key.String()),
			tag.ArchivalDeterministicConstructionCheckFailReason(reason))
		h.scope.IncCounter(metrics.ArchiverDeterministicConstructionCheckFailedCount)

		nonDeterministicBlobKey, err := NewNonDeterministicBlobKey(key)
		if err != nil {
			h.logger.Error("failed to construct non-deterministic blob key", tag.Error(err))
		} else if err := uploadBlob(ctx, h.container.Blobstore, h.request.BucketName, nonDeterministicBlobKey, blob); err != nil {
			h.logger.Error("failed to upload non-deterministic blob", tag.ArchivalUploadFailReason(errorDetails(err)), tag.Error(err))
		} else {
			h.logger.Info("uploaded non-deterministic blob", tag.ArchivalBlobKey(key.String()), tag.ArchivalNonDeterministicBlobKey(nonDeterministicBlobKey.String()))
		}
	}
}

func (h *historyBlobUploader) uploadIndexBlob(ctx context.Context) error {
	indexBlobKey, err := NewHistoryIndexBlobKey(h.request.DomainID, h.request.WorkflowID, h.request.RunID)
	if err != nil {
		h.logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason("could not construct index blob key"))
		return cadence.NewCustomError(errConstructKey, err.Error())
	}
	existingVersions, err := getTags(ctx, h.container.Blobstore, h.request.BucketName, indexBlobKey)
	if err != nil && err != blobstore.ErrBlobNotExists {
		h.logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(errorDetails(err)), tag.ArchivalBlobKey(indexBlobKey.String()), tag.Error(err))
		return err
	}
	indexBlobWithVersion := addVersion(h.request.CloseFailoverVersion, existingVersions)
	if indexBlobWithVersion != nil {
		if err := uploadBlob(ctx, h.container.Blobstore, h.request.BucketName, indexBlobKey, indexBlobWithVersion); err != nil {
			h.logger.Error(uploadErrorMsg, tag.ArchivalUploadFailReason(errorDetails(err)), tag.ArchivalBlobKey(indexBlobKey.String()), tag.Error(err))
			return err
		}
	}
	return nil
}

func (h *historyBlobUploader) integrityCheckHelper(ctx context.Context) {
	h.scope.IncCounter(metrics.ArchiverRunningBlobIntegrityCheckCount)
	blobDownloader := h.container.HistoryBlobDownloader
	if blobDownloader == nil {
		blobDownloader = NewHistoryBlobDownloader(h.container.Blobstore)
	}
	req := &DownloadBlobRequest{
		ArchivalBucket:       h.request.BucketName,
		DomainID:             h.request.DomainID,
		WorkflowID:           h.request.WorkflowID,
		RunID:                h.request.RunID,
		CloseFailoverVersion: common.Int64Ptr(h.request.CloseFailoverVersion),
	}
	var fetchedHistoryEventHashes []uint64
	for len(fetchedHistoryEventHashes) == 0 || len(req.NextPageToken) != 0 {
		resp, err := blobDownloader.DownloadBlob(ctx, req)
		if err != nil {
			h.scope.IncCounter(metrics.ArchiverCouldNotRunBlobIntegrityCheckCount)
			h.logger.Error("failed to access history for blob integrity check", tag.Error(err))
			return
		}
		for _, e := range resp.HistoryBlob.Body.Events {
			fetchedHistoryEventHashes = append(fetchedHistoryEventHashes, hash(e.String()))
		}
		req.NextPageToken = resp.NextPageToken
	}
	if !hashesEqual(fetchedHistoryEventHashes, h.uploadedHistoryEventHashes) {
		h.scope.IncCounter(metrics.ArchiverBlobIntegrityCheckFailedCount)
		h.logger.Error("uploaded history does not match fetched history")
	}
}
