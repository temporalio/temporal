// Copyright (c) 2020 Uber Technologies, Inc.
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

package gcloud

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"

	"github.com/uber/cadence/common/archiver/gcloud/connector"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
)

var (
	errUploadNonRetriable = errors.New("upload non-retriable error")
)

const (
	// URIScheme is the scheme for the gcloud storage implementation
	URIScheme = "gs"

	targetHistoryBlobSize = 2 * 1024 * 1024 // 2MB
	errEncodeHistory      = "failed to encode history batches"
	errBucketHistory      = "failed to get google storage bucket handle"
	errWriteFile          = "failed to write history to google storage"
)

type historyArchiver struct {
	container     *archiver.HistoryBootstrapContainer
	gcloudStorage connector.Client

	// only set in test code
	historyIterator archiver.HistoryIterator
}

type progress struct {
	CurrentPageNumber int
	IteratorState     []byte
}

type getHistoryToken struct {
	CloseFailoverVersion int64
	HighestPart          int
	CurrentPart          int
	BatchIdxOffset       int
}

// NewHistoryArchiver creates a new gcloud storage HistoryArchiver
func NewHistoryArchiver(
	container *archiver.HistoryBootstrapContainer,
	config *config.GstorageArchiver,
) (archiver.HistoryArchiver, error) {
	storage, err := connector.NewClient(context.Background(), config)
	if err == nil {
		return newHistoryArchiver(container, nil, storage), nil
	}
	return nil, err
}

func newHistoryArchiver(container *archiver.HistoryBootstrapContainer, historyIterator archiver.HistoryIterator, storage connector.Client) archiver.HistoryArchiver {
	return &historyArchiver{
		container:       container,
		gcloudStorage:   storage,
		historyIterator: historyIterator,
	}
}

// Archive is used to archive a workflow history. When the context expires the method should stop trying to archive.
// Implementors are free to archive however they want, including implementing retries of sub-operations. The URI defines
// the resource that histories should be archived into. The implementor gets to determine how to interpret the URI.
// The Archive method may or may not be automatically retried by the caller. The ArchiveOptions are used
// to interact with these retries including giving the implementor the ability to cancel retries and record progress
// between retry attempts.
// This method will be invoked after a workflow passes its retention period.
func (h *historyArchiver) Archive(ctx context.Context, URI archiver.URI, request *archiver.ArchiveHistoryRequest, opts ...archiver.ArchiveOption) (err error) {
	scope := h.container.MetricsClient.Scope(metrics.HistoryArchiverScope, metrics.DomainTag(request.DomainName))
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer func() {
		sw.Stop()
		if err != nil {

			if err.Error() != errUploadNonRetriable.Error() {
				scope.IncCounter(metrics.HistoryArchiverArchiveTransientErrorCount)
				return
			}

			scope.IncCounter(metrics.HistoryArchiverArchiveNonRetryableErrorCount)
			if featureCatalog.NonRetriableError != nil {
				err = featureCatalog.NonRetriableError()
			}

		}
	}()

	logger := archiver.TagLoggerWithArchiveHistoryRequestAndURI(h.container.Logger, request, URI.String())

	if err := h.ValidateURI(URI); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidURI), tag.Error(err))
		return errUploadNonRetriable
	}

	if err := archiver.ValidateHistoryArchiveRequest(request); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidArchiveRequest), tag.Error(err))
		return errUploadNonRetriable
	}

	var totalUploadSize int64
	historyIterator := h.historyIterator
	var progress progress
	if historyIterator == nil { // will only be set by testing code
		historyIterator, _ = loadHistoryIterator(ctx, request, h.container.HistoryV2Manager, featureCatalog, &progress)
	}

	for historyIterator.HasNext() {
		part := progress.CurrentPageNumber
		historyBlob, err := getNextHistoryBlob(ctx, historyIterator)

		if err != nil {
			logger := logger.WithTags(tag.ArchivalArchiveFailReason(archiver.ErrReasonReadHistory), tag.Error(err))
			if !common.IsPersistenceTransientError(err) {
				logger.Error(archiver.ArchiveNonRetriableErrorMsg)
				return errUploadNonRetriable
			}
			logger.Error(archiver.ArchiveTransientErrorMsg)
			return err
		}

		if historyMutated(request, historyBlob.Body, *historyBlob.Header.IsLast) {
			logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonHistoryMutated))
			return archiver.ErrHistoryMutated
		}

		encodedHistoryPart, err := encode(historyBlob.Body)
		if err != nil {
			logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(errEncodeHistory), tag.Error(err))
			return errUploadNonRetriable
		}

		filename := constructHistoryFilenameMultipart(request.DomainID, request.WorkflowID, request.RunID, request.CloseFailoverVersion, part)
		if exist, _ := h.gcloudStorage.Exist(ctx, URI, filename); !exist {
			if err := h.gcloudStorage.Upload(ctx, URI, filename, encodedHistoryPart); err != nil {
				logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(errWriteFile), tag.Error(err))
				scope.IncCounter(metrics.HistoryArchiverArchiveTransientErrorCount)
				return err
			}

			totalUploadSize = totalUploadSize + int64(binary.Size(encodedHistoryPart))
		}

		saveHistoryIteratorState(ctx, featureCatalog, historyIterator, part, &progress)
	}

	scope.AddCounter(metrics.HistoryArchiverTotalUploadSize, totalUploadSize)
	scope.AddCounter(metrics.HistoryArchiverHistorySize, totalUploadSize)
	scope.IncCounter(metrics.HistoryArchiverArchiveSuccessCount)
	return
}

// Get is used to access an archived history. When context expires method should stop trying to fetch history.
// The URI identifies the resource from which history should be accessed and it is up to the implementor to interpret this URI.
// This method should thrift errors - see filestore as an example.
func (h *historyArchiver) Get(ctx context.Context, URI archiver.URI, request *archiver.GetHistoryRequest) (*archiver.GetHistoryResponse, error) {

	err := h.ValidateURI(URI)
	if err != nil {
		return nil, &shared.BadRequestError{Message: archiver.ErrInvalidURI.Error()}
	}

	if err := archiver.ValidateGetRequest(request); err != nil {
		return nil, &shared.BadRequestError{Message: archiver.ErrInvalidGetHistoryRequest.Error()}
	}

	var token *getHistoryToken
	if request.NextPageToken != nil {
		token, err = deserializeGetHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, &shared.BadRequestError{Message: archiver.ErrNextPageTokenCorrupted.Error()}
		}
	} else {
		highestVersion, historyhighestPart, historyCurrentPart, err := h.getHighestVersion(ctx, URI, request)
		if err != nil {
			return nil, &shared.InternalServiceError{Message: err.Error()}
		}
		token = &getHistoryToken{
			CloseFailoverVersion: *highestVersion,
			HighestPart:          *historyhighestPart,
			CurrentPart:          *historyCurrentPart,
			BatchIdxOffset:       0,
		}
	}

	response := &archiver.GetHistoryResponse{}
	response.HistoryBatches = []*shared.History{}
	numOfEvents := 0

outer:
	for token.CurrentPart <= token.HighestPart {

		filename := constructHistoryFilenameMultipart(request.DomainID, request.WorkflowID, request.RunID, token.CloseFailoverVersion, token.CurrentPart)
		encodedHistoryBatches, err := h.gcloudStorage.Get(ctx, URI, filename)

		if err != nil {
			return nil, &shared.InternalServiceError{Message: err.Error()}
		}

		if encodedHistoryBatches == nil {
			return nil, &shared.InternalServiceError{Message: "Fail retrieving history file: " + URI.String() + "/" + filename}
		}

		batches, err := decodeHistoryBatches(encodedHistoryBatches)
		if err != nil {
			return nil, &shared.InternalServiceError{Message: err.Error()}
		}
		// trim the batches in the beginning based on token.BatchIdxOffset
		batches = batches[token.BatchIdxOffset:]

		for idx, batch := range batches {
			response.HistoryBatches = append(response.HistoryBatches, batch)
			token.BatchIdxOffset++
			numOfEvents += len(batch.Events)

			if numOfEvents >= request.PageSize {
				if idx == len(batches)-1 {
					// handle the edge case where page size is meeted after adding the last batch
					token.BatchIdxOffset = 0
					token.CurrentPart++
				}
				break outer
			}
		}

		// reset the offset to 0 as we will read a new page
		token.BatchIdxOffset = 0
		token.CurrentPart++

	}

	if token.CurrentPart <= token.HighestPart {
		nextToken, err := serializeToken(token)
		if err != nil {
			return nil, &shared.InternalServiceError{Message: err.Error()}
		}
		response.NextPageToken = nextToken
	}

	return response, nil
}

// ValidateURI is used to define what a valid URI for an implementation is.
func (h *historyArchiver) ValidateURI(URI archiver.URI) (err error) {

	if err = h.validateURI(URI); err == nil {
		_, err = h.gcloudStorage.Exist(context.Background(), URI, "")
	}

	return
}

func (h *historyArchiver) validateURI(URI archiver.URI) (err error) {
	if URI.Scheme() != URIScheme {
		return archiver.ErrURISchemeMismatch
	}

	if URI.Path() == "" || URI.Hostname() == "" {
		return archiver.ErrInvalidURI
	}

	return
}

func getNextHistoryBlob(ctx context.Context, historyIterator archiver.HistoryIterator) (*archiver.HistoryBlob, error) {
	historyBlob, err := historyIterator.Next()
	op := func() error {
		historyBlob, err = historyIterator.Next()
		return err
	}
	for err != nil {
		if !common.IsPersistenceTransientError(err) {
			return nil, err
		}
		if contextExpired(ctx) {
			return nil, archiver.ErrContextTimeout
		}
		err = backoff.Retry(op, common.CreatePersistanceRetryPolicy(), common.IsPersistenceTransientError)
	}
	return historyBlob, nil
}

func historyMutated(request *archiver.ArchiveHistoryRequest, historyBatches []*shared.History, isLast bool) bool {
	lastBatch := historyBatches[len(historyBatches)-1].Events
	lastEvent := lastBatch[len(lastBatch)-1]
	lastFailoverVersion := lastEvent.GetVersion()
	if lastFailoverVersion > request.CloseFailoverVersion {
		return true
	}

	if !isLast {
		return false
	}
	lastEventID := lastEvent.GetEventId()
	return lastFailoverVersion != request.CloseFailoverVersion || lastEventID+1 != request.NextEventID
}

func (h *historyArchiver) getHighestVersion(ctx context.Context, URI archiver.URI, request *archiver.GetHistoryRequest) (*int64, *int, *int, error) {

	filenames, err := h.gcloudStorage.Query(ctx, URI, constructHistoryFilenamePrefix(request.DomainID, request.WorkflowID, request.RunID))

	if err != nil {
		return nil, nil, nil, err
	}

	var highestVersion *int64
	var highestVersionPart *int
	var lowestVersionPart *int

	for _, filename := range filenames {
		version, partVersionID, err := extractCloseFailoverVersion(filepath.Base(filename))
		if err != nil || (request.CloseFailoverVersion != nil && version != *request.CloseFailoverVersion) {
			continue
		}

		if highestVersion == nil || version > *highestVersion {
			highestVersion = &version
			highestVersionPart = new(int)
			lowestVersionPart = new(int)
		}

		if *highestVersion == version {
			if highestVersionPart == nil || partVersionID > *highestVersionPart {
				highestVersionPart = &partVersionID
			}

			if lowestVersionPart == nil || partVersionID < *lowestVersionPart {
				lowestVersionPart = &partVersionID
			}
		}

	}

	return highestVersion, highestVersionPart, lowestVersionPart, nil
}

func loadHistoryIterator(ctx context.Context, request *archiver.ArchiveHistoryRequest, historyManager persistence.HistoryManager, featureCatalog *archiver.ArchiveFeatureCatalog, progress *progress) (historyIterator archiver.HistoryIterator, err error) {

	defer func() {
		if err != nil || historyIterator == nil {
			historyIterator, err = archiver.NewHistoryIteratorFromState(request, historyManager, targetHistoryBlobSize, nil)
		}
	}()

	if featureCatalog.ProgressManager != nil {
		if featureCatalog.ProgressManager.HasProgress(ctx) {
			err = featureCatalog.ProgressManager.LoadProgress(ctx, &progress)
			if err == nil {
				historyIterator, err = archiver.NewHistoryIteratorFromState(request, historyManager, targetHistoryBlobSize, progress.IteratorState)
			}
		}

	}
	return
}

func saveHistoryIteratorState(ctx context.Context, featureCatalog *archiver.ArchiveFeatureCatalog, historyIterator archiver.HistoryIterator, currentPartNum int, progress *progress) (err error) {
	var state []byte
	if featureCatalog.ProgressManager != nil {
		state, err = historyIterator.GetState()
		if err == nil {
			progress.CurrentPageNumber = currentPartNum + 1
			progress.IteratorState = state

			err = featureCatalog.ProgressManager.RecordProgress(ctx, progress)
		}
	}

	return err
}
