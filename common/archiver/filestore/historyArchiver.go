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

// Filestore History Archiver will archive workflow histories to local disk.

// Each Archive() request results in a file named in the format of
// hash(domainID, workflowID, runID)_version.history being created in the specified
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

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/service/config"
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
		container *archiver.HistoryBootstrapContainer
		fileMode  os.FileMode
		dirMode   os.FileMode

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
	container *archiver.HistoryBootstrapContainer,
	config *config.FilestoreArchiver,
) (archiver.HistoryArchiver, error) {
	return newHistoryArchiver(container, config, nil)
}

func newHistoryArchiver(
	container *archiver.HistoryBootstrapContainer,
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
		container:       container,
		fileMode:        os.FileMode(fileMode),
		dirMode:         os.FileMode(dirMode),
		historyIterator: historyIterator,
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
		if err != nil && !common.IsPersistenceTransientError(err) && featureCatalog.NonRetriableError != nil {
			err = featureCatalog.NonRetriableError()
		}
	}()

	logger := archiver.TagLoggerWithArchiveHistoryRequestAndURI(h.container.Logger, request, URI.String())

	if err := h.ValidateURI(URI); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidURI), tag.Error(err))
		return err
	}

	if err := archiver.ValidateHistoryArchiveRequest(request); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidArchiveRequest), tag.Error(err))
		return err
	}

	historyIterator := h.historyIterator
	if historyIterator == nil { // will only be set by testing code
		historyIterator = archiver.NewHistoryIterator(request, h.container.HistoryV2Manager, targetHistoryBlobSize)
	}

	historyBatches := []*shared.History{}
	for historyIterator.HasNext() {
		historyBlob, err := getNextHistoryBlob(ctx, historyIterator)
		if err != nil {
			logger := logger.WithTags(tag.ArchivalArchiveFailReason(archiver.ErrReasonReadHistory), tag.Error(err))
			if !common.IsPersistenceTransientError(err) {
				logger.Error(archiver.ArchiveNonRetriableErrorMsg)
			} else {
				logger.Error(archiver.ArchiveTransientErrorMsg)
			}
			return err
		}

		if historyMutated(request, historyBlob.Body, *historyBlob.Header.IsLast) {
			logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonHistoryMutated))
			return archiver.ErrHistoryMutated
		}

		historyBatches = append(historyBatches, historyBlob.Body...)
	}

	encodedHistoryBatches, err := encode(historyBatches)
	if err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(errEncodeHistory), tag.Error(err))
		return err
	}

	dirPath := URI.Path()
	if err = mkdirAll(dirPath, h.dirMode); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(errMakeDirectory), tag.Error(err))
		return err
	}

	filename := constructHistoryFilename(request.DomainID, request.WorkflowID, request.RunID, request.CloseFailoverVersion)
	if err := writeFile(path.Join(dirPath, filename), encodedHistoryBatches, h.fileMode); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(errWriteFile), tag.Error(err))
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
		return nil, &shared.BadRequestError{Message: archiver.ErrInvalidURI.Error()}
	}

	if err := archiver.ValidateGetRequest(request); err != nil {
		return nil, &shared.BadRequestError{Message: archiver.ErrInvalidGetHistoryRequest.Error()}
	}

	dirPath := URI.Path()
	exists, err := directoryExists(dirPath)
	if err != nil {
		return nil, &shared.InternalServiceError{Message: err.Error()}
	}
	if !exists {
		return nil, &shared.BadRequestError{Message: archiver.ErrHistoryNotExist.Error()}
	}

	var token *getHistoryToken
	if request.NextPageToken != nil {
		token, err = deserializeGetHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, &shared.BadRequestError{Message: archiver.ErrNextPageTokenCorrupted.Error()}
		}
	} else if request.CloseFailoverVersion != nil {
		token = &getHistoryToken{
			CloseFailoverVersion: *request.CloseFailoverVersion,
			NextBatchIdx:         0,
		}
	} else {
		highestVersion, err := getHighestVersion(dirPath, request)
		if err != nil {
			return nil, &shared.InternalServiceError{Message: err.Error()}
		}
		token = &getHistoryToken{
			CloseFailoverVersion: *highestVersion,
			NextBatchIdx:         0,
		}
	}

	filename := constructHistoryFilename(request.DomainID, request.WorkflowID, request.RunID, token.CloseFailoverVersion)
	filepath := path.Join(dirPath, filename)
	exists, err = fileExists(filepath)
	if err != nil {
		return nil, &shared.InternalServiceError{Message: err.Error()}
	}
	if !exists {
		return nil, &shared.EntityNotExistsError{Message: archiver.ErrHistoryNotExist.Error()}
	}

	encodedHistoryBatches, err := readFile(filepath)
	if err != nil {
		return nil, &shared.InternalServiceError{Message: err.Error()}
	}

	historyBatches, err := decodeHistoryBatches(encodedHistoryBatches)
	if err != nil {
		return nil, &shared.InternalServiceError{Message: err.Error()}
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
			return nil, &shared.InternalServiceError{Message: err.Error()}
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

func getHighestVersion(dirPath string, request *archiver.GetHistoryRequest) (*int64, error) {
	filenames, err := listFilesByPrefix(dirPath, constructHistoryFilenamePrefix(request.DomainID, request.WorkflowID, request.RunID))
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
