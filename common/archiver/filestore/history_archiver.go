// Filestore History Archiver will archive workflow histories to local disk.

// Each Archive() request results in a file named in the format of
// hash(namespaceID, workflowID, runID)_version.history being created in the specified
// directory. Workflow histories stored in that file are encoded in JSON format.

// The Get() method retrieves the archived histories from the directory specified in the
// URI. It optionally takes in a NextPageToken which specifies the workflow close failover
// version and the index of the first history batch that should be returned. Instead of
// NextPageToken, caller can also provide a close failover version, in which case, Get() method
// will return history batches starting from the beginning of that history version. If neither
// of NextPageToken or close failover version is specified, the highest close failover version
// will be picked.

package filestore

import (
	"context"
	"errors"
	"os"
	"path"
	"strconv"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

const (
	// URIScheme is the scheme for the filestore implementation
	URIScheme = "file"

	errEncodeHistory = "failed to encode history batches"
	errMakeDirectory = "failed to make directory"
	errWriteFile     = "failed to write history to file"

	targetHistoryBlobSize = 2 * 1024 * 1024 // 2MB
)

var (
	errInvalidFileMode = errors.New("invalid file mode")
	errInvalidDirMode  = errors.New("invalid directory mode")
)

type (
	historyArchiver struct {
		executionManager persistence.ExecutionManager
		logger           log.Logger
		metricsHandler   metrics.Handler
		fileMode         os.FileMode
		dirMode          os.FileMode

		// only set in test code
		historyIterator archiver.HistoryIterator
	}

	getHistoryToken struct {
		CloseFailoverVersion int64
		NextBatchIdx         int
	}
)

// NewHistoryArchiver creates a new archiver.HistoryArchiver based on filestore
func NewHistoryArchiver(
	executionManager persistence.ExecutionManager,
	logger log.Logger,
	metricsHandler metrics.Handler,
	config *config.FilestoreArchiver,
) (archiver.HistoryArchiver, error) {
	return newHistoryArchiver(executionManager, logger, metricsHandler, config, nil)
}

func newHistoryArchiver(
	executionManager persistence.ExecutionManager,
	logger log.Logger,
	metricsHandler metrics.Handler,
	config *config.FilestoreArchiver,
	historyIterator archiver.HistoryIterator,
) (*historyArchiver, error) {
	fileMode, err := strconv.ParseUint(config.FileMode, 0, 32)
	if err != nil {
		return nil, errInvalidFileMode
	}
	dirMode, err := strconv.ParseUint(config.DirMode, 0, 32)
	if err != nil {
		return nil, errInvalidDirMode
	}
	return &historyArchiver{
		executionManager: executionManager,
		logger:           logger,
		metricsHandler:   metricsHandler,
		fileMode:         os.FileMode(fileMode),
		dirMode:          os.FileMode(dirMode),
		historyIterator:  historyIterator,
	}, nil
}

func (h *historyArchiver) Archive(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.ArchiveHistoryRequest,
	opts ...archiver.ArchiveOption,
) (err error) {
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	defer func() {
		if err != nil && !common.IsPersistenceTransientError(err) && featureCatalog.NonRetryableError != nil {
			err = featureCatalog.NonRetryableError()
		}
	}()

	logger := archiver.TagLoggerWithArchiveHistoryRequestAndURI(h.logger, request, URI.String())

	if err := h.ValidateURI(URI); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidURI), tag.Error(err))
		return err
	}

	if err := archiver.ValidateHistoryArchiveRequest(request); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidArchiveRequest), tag.Error(err))
		return err
	}

	historyIterator := h.historyIterator
	if historyIterator == nil { // will only be set by testing code
		historyIterator = archiver.NewHistoryIterator(request, h.executionManager, targetHistoryBlobSize)
	}

	var historyBatches []*historypb.History
	for historyIterator.HasNext() {
		historyBlob, err := historyIterator.Next(ctx)
		if err != nil {
			if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
				// workflow history no longer exists, may due to duplicated archival signal
				// this may happen even in the middle of iterating history as two archival signals
				// can be processed concurrently.
				logger.Info(archiver.ArchiveSkippedInfoMsg)
				return nil
			}

			logger = log.With(logger, tag.ArchivalArchiveFailReason(archiver.ErrReasonReadHistory), tag.Error(err))
			if !common.IsPersistenceTransientError(err) {
				logger.Error(archiver.ArchiveNonRetryableErrorMsg)
			} else {
				logger.Error(archiver.ArchiveTransientErrorMsg)
			}
			return err
		}

		if historyMutated(request, historyBlob.Body, historyBlob.Header.IsLast) {
			logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonHistoryMutated))
			return archiver.ErrHistoryMutated
		}

		historyBatches = append(historyBatches, historyBlob.Body...)
	}

	encoder := codec.NewJSONPBEncoder()
	encodedHistoryBatches, err := encoder.EncodeHistories(historyBatches)
	if err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errEncodeHistory), tag.Error(err))
		return err
	}

	dirPath := URI.Path()
	if err = mkdirAll(dirPath, h.dirMode); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errMakeDirectory), tag.Error(err))
		return err
	}

	filename := constructHistoryFilename(request.NamespaceID, request.WorkflowID, request.RunID, request.CloseFailoverVersion)
	if err := writeFile(path.Join(dirPath, filename), encodedHistoryBatches, h.fileMode); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errWriteFile), tag.Error(err))
		return err
	}

	return nil
}

func (h *historyArchiver) Get(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.GetHistoryRequest,
) (*archiver.GetHistoryResponse, error) {
	if err := h.ValidateURI(URI); err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidURI.Error())
	}

	if err := archiver.ValidateGetRequest(request); err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidGetHistoryRequest.Error())
	}

	dirPath := URI.Path()
	exists, err := directoryExists(dirPath)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	if !exists {
		return nil, serviceerror.NewNotFound(archiver.ErrHistoryNotExist.Error())
	}

	var token *getHistoryToken
	if request.NextPageToken != nil {
		token, err = deserializeGetHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(archiver.ErrNextPageTokenCorrupted.Error())
		}
	} else if request.CloseFailoverVersion != nil {
		token = &getHistoryToken{
			CloseFailoverVersion: *request.CloseFailoverVersion,
			NextBatchIdx:         0,
		}
	} else {
		highestVersion, err := getHighestVersion(dirPath, request)
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}
		token = &getHistoryToken{
			CloseFailoverVersion: *highestVersion,
			NextBatchIdx:         0,
		}
	}

	filename := constructHistoryFilename(request.NamespaceID, request.WorkflowID, request.RunID, token.CloseFailoverVersion)
	filepath := path.Join(dirPath, filename)
	exists, err = fileExists(filepath)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	if !exists {
		return nil, serviceerror.NewNotFound(archiver.ErrHistoryNotExist.Error())
	}

	encodedHistoryBatches, err := readFile(filepath)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}

	encoder := codec.NewJSONPBEncoder()
	historyBatches, err := encoder.DecodeHistories(encodedHistoryBatches)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	historyBatches = historyBatches[token.NextBatchIdx:]

	response := &archiver.GetHistoryResponse{}
	numOfEvents := 0
	numOfBatches := 0
	for _, batch := range historyBatches {
		response.HistoryBatches = append(response.HistoryBatches, batch)
		numOfBatches++
		numOfEvents += len(batch.Events)
		if numOfEvents >= request.PageSize {
			break
		}
	}

	if numOfBatches < len(historyBatches) {
		token.NextBatchIdx += numOfBatches
		nextToken, err := serializeToken(token)
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}
		response.NextPageToken = nextToken
	}

	return response, nil
}

func (h *historyArchiver) ValidateURI(URI archiver.URI) error {
	if URI.Scheme() != URIScheme {
		return archiver.ErrURISchemeMismatch
	}

	return validateDirPath(URI.Path())
}

func getHighestVersion(dirPath string, request *archiver.GetHistoryRequest) (*int64, error) {
	filenames, err := listFilesByPrefix(dirPath, constructHistoryFilenamePrefix(request.NamespaceID, request.WorkflowID, request.RunID))
	if err != nil {
		return nil, err
	}

	var highestVersion *int64
	for _, filename := range filenames {
		version, err := extractCloseFailoverVersion(filename)
		if err != nil {
			continue
		}
		if highestVersion == nil || version > *highestVersion {
			highestVersion = &version
		}
	}
	if highestVersion == nil {
		return nil, archiver.ErrHistoryNotExist
	}
	return highestVersion, nil
}
