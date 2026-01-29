package azure_store

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/azure_store/connector"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

var (
	errUploadNonRetryable = errors.New("upload non-retryable error")
)

const (
	// URIScheme is the scheme for the azure blob storage implementation
	URIScheme = "azblob"

	targetHistoryBlobSize = 2 * 1024 * 1024 // 2MB
	errEncodeHistory      = "failed to encode history batches"
	errBucketHistory      = "failed to get azure storage container handle"
	errWriteFile          = "failed to write history to azure storage"
)

type historyArchiver struct {
	executionManager persistence.ExecutionManager
	logger           log.Logger
	metricsHandler   metrics.Handler
	azureStorage     connector.Client

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

// NewHistoryArchiver creates a new azure storage HistoryArchiver
func NewHistoryArchiver(
	executionManager persistence.ExecutionManager,
	logger log.Logger,
	metricsHandler metrics.Handler,
	config *config.AzblobArchiver,
) (archiver.HistoryArchiver, error) {
	storage, err := connector.NewClient(config)
	if err == nil {
		return newHistoryArchiver(executionManager, logger, metricsHandler, nil, storage), nil
	}
	return nil, err
}

func newHistoryArchiver(executionManager persistence.ExecutionManager, logger log.Logger, metricsHandler metrics.Handler, historyIterator archiver.HistoryIterator, storage connector.Client) archiver.HistoryArchiver {
	return &historyArchiver{
		executionManager: executionManager,
		logger:           logger,
		metricsHandler:   metricsHandler,
		azureStorage:     storage,
		historyIterator:  historyIterator,
	}
}

func (h *historyArchiver) Archive(ctx context.Context, URI archiver.URI, request *archiver.ArchiveHistoryRequest, opts ...archiver.ArchiveOption) (err error) {
	handler := h.metricsHandler.WithTags(metrics.OperationTag(metrics.HistoryArchiverScope), metrics.NamespaceTag(request.Namespace))
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	startTime := time.Now().UTC()
	defer func() {
		metrics.ServiceLatency.With(handler).Record(time.Since(startTime))
		if err != nil {
			if err.Error() != errUploadNonRetryable.Error() {
				metrics.HistoryArchiverArchiveTransientErrorCount.With(handler).Record(1)
				return
			}
			metrics.HistoryArchiverArchiveNonRetryableErrorCount.With(handler).Record(1)
			if featureCatalog.NonRetryableError != nil {
				err = featureCatalog.NonRetryableError()
			}
		}
	}()

	logger := archiver.TagLoggerWithArchiveHistoryRequestAndURI(h.logger, request, URI.String())

	if err := h.ValidateURI(URI); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidURI), tag.Error(err))
		return errUploadNonRetryable
	}

	if err := archiver.ValidateHistoryArchiveRequest(request); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidArchiveRequest), tag.Error(err))
		return errUploadNonRetryable
	}

	var totalUploadSize int64
	historyIterator := h.historyIterator
	var progress progress
	if historyIterator == nil { // will only be set by testing code
		historyIterator, _ = loadHistoryIterator(ctx, request, h.executionManager, featureCatalog, &progress)
	}

	encoder := codec.NewJSONPBEncoder()

	for historyIterator.HasNext() {
		part := progress.CurrentPageNumber
		historyBlob, err := historyIterator.Next(ctx)
		if err != nil {
			if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
				logger.Info(archiver.ArchiveSkippedInfoMsg)
				metrics.HistoryArchiverDuplicateArchivalsCount.With(handler).Record(1)
				return nil
			}

			logger = log.With(logger, tag.ArchivalArchiveFailReason(archiver.ErrReasonReadHistory), tag.Error(err))
			if !common.IsPersistenceTransientError(err) {
				logger.Error(archiver.ArchiveNonRetryableErrorMsg)
				return errUploadNonRetryable
			}
			logger.Error(archiver.ArchiveTransientErrorMsg)
			return err
		}

		if historyMutated(request, historyBlob.Body, historyBlob.Header.IsLast) {
			logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonHistoryMutated))
			return archiver.ErrHistoryMutated
		}

		encodedHistoryPart, err := encoder.EncodeHistories(historyBlob.Body)
		if err != nil {
			logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errEncodeHistory), tag.Error(err))
			return errUploadNonRetryable
		}

		filename := constructHistoryFilenameMultipart(request.NamespaceID, request.WorkflowID, request.RunID, request.CloseFailoverVersion, part)
		if exist, _ := h.azureStorage.Exist(ctx, URI, filename); !exist {
			if err := h.azureStorage.Upload(ctx, URI, filename, encodedHistoryPart); err != nil {
				logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(errWriteFile), tag.Error(err))
				metrics.HistoryArchiverArchiveTransientErrorCount.With(handler).Record(1)
				return err
			}

			totalUploadSize = totalUploadSize + int64(binary.Size(encodedHistoryPart))
		}

		if err := saveHistoryIteratorState(ctx, featureCatalog, historyIterator, part, &progress); err != nil {
			return err
		}
	}

	metrics.HistoryArchiverTotalUploadSize.With(handler).Record(totalUploadSize)
	metrics.HistoryArchiverHistorySize.With(handler).Record(totalUploadSize)
	metrics.HistoryArchiverArchiveSuccessCount.With(handler).Record(1)
	return
}

func (h *historyArchiver) Get(ctx context.Context, URI archiver.URI, request *archiver.GetHistoryRequest) (*archiver.GetHistoryResponse, error) {
	err := h.ValidateURI(URI)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidURI.Error())
	}

	if err := archiver.ValidateGetRequest(request); err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidGetHistoryRequest.Error())
	}

	var token *getHistoryToken
	if request.NextPageToken != nil {
		token, err = deserializeGetHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(archiver.ErrNextPageTokenCorrupted.Error())
		}
	} else {
		highestVersion, historyhighestPart, historyCurrentPart, err := h.getHighestVersion(ctx, URI, request)
		if err != nil {
			return nil, serviceerror.NewUnavailable(err.Error())
		}
		if highestVersion == nil {
			return nil, serviceerror.NewNotFound(archiver.ErrHistoryNotExist.Error())
		}
		token = &getHistoryToken{
			CloseFailoverVersion: *highestVersion,
			HighestPart:          *historyhighestPart,
			CurrentPart:          *historyCurrentPart,
			BatchIdxOffset:       0,
		}
	}

	response := &archiver.GetHistoryResponse{}
	response.HistoryBatches = []*historypb.History{}
	numOfEvents := 0
	encoder := codec.NewJSONPBEncoder()

outer:
	for token.CurrentPart <= token.HighestPart {
		filename := constructHistoryFilenameMultipart(request.NamespaceID, request.WorkflowID, request.RunID, token.CloseFailoverVersion, token.CurrentPart)
		encodedHistoryBatches, err := h.azureStorage.Get(ctx, URI, filename)
		if err != nil {
			return nil, serviceerror.NewUnavailable(err.Error())
		}
		if encodedHistoryBatches == nil {
			return nil, serviceerror.NewInternal("Fail retrieving history file: " + URI.String() + "/" + filename)
		}

		batches, err := encoder.DecodeHistories(encodedHistoryBatches)
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}
		batches = batches[token.BatchIdxOffset:]

		for idx, batch := range batches {
			response.HistoryBatches = append(response.HistoryBatches, batch)
			token.BatchIdxOffset++
			numOfEvents += len(batch.Events)

			if numOfEvents >= request.PageSize {
				if idx == len(batches)-1 {
					token.BatchIdxOffset = 0
					token.CurrentPart++
				}
				break outer
			}
		}

		token.BatchIdxOffset = 0
		token.CurrentPart++
	}

	if token.CurrentPart <= token.HighestPart {
		nextToken, err := serializeToken(token)
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}
		response.NextPageToken = nextToken
	}

	return response, nil
}

func (h *historyArchiver) ValidateURI(URI archiver.URI) (err error) {
	if err = h.validateURI(URI); err == nil {
		_, err = h.azureStorage.Exist(context.Background(), URI, "")
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

func historyMutated(request *archiver.ArchiveHistoryRequest, historyBatches []*historypb.History, isLast bool) bool {
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
	filenames, err := h.azureStorage.Query(ctx, URI, constructHistoryFilenamePrefix(request.NamespaceID, request.WorkflowID, request.RunID))
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

func loadHistoryIterator(ctx context.Context, request *archiver.ArchiveHistoryRequest, executionManager persistence.ExecutionManager, featureCatalog *archiver.ArchiveFeatureCatalog, progress *progress) (historyIterator archiver.HistoryIterator, err error) {
	defer func() {
		if err != nil || historyIterator == nil {
			historyIterator, err = archiver.NewHistoryIteratorFromState(request, executionManager, targetHistoryBlobSize, nil)
		}
	}()

	if featureCatalog.ProgressManager != nil {
		if featureCatalog.ProgressManager.HasProgress(ctx) {
			err = featureCatalog.ProgressManager.LoadProgress(ctx, &progress)
			if err == nil {
				historyIterator, err = archiver.NewHistoryIteratorFromState(request, executionManager, targetHistoryBlobSize, progress.IteratorState)
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
