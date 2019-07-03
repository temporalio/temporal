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
// The location is specified by the URI which has the form file:///path/to/directory.

// Each Archive() request results in a file named in the format of
// hash(domainID, workflowID, runID)_version.history being created in the specified
// directory. Workflow histories stored in that file are encoded in JSON format.

// The Get() method retrieves the archived histories from the directory specified in the
// URI. It optionally takes in a NextPageToken which specifies the workflow close failover
// version and the index of the first history batch that should be returned. Instead of
// NextPageToken, caller can also provide a close failover version, in which case, Get() method
// will return history batches starting from the beginning of that history verison. If neither
// of NextPageToken or close failover version is specified, the highest close failover version
// will be picked.

package filestore

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log/tag"
)

const (
	// URIScheme is the scheme for the filestore implementation
	URIScheme = "file://"

	errEncodeHistory = "failed to encode history batches"
	errMakeDirectory = "failed to make directory"
	errWriteFile     = "failed to write history to file"
)

type (
	// HistoryArchiverConfig configs the filestore implementation of archiver.HistoryArchiver interface
	HistoryArchiverConfig struct {
		*archiver.HistoryIteratorConfig
	}

	historyArchiver struct {
		container archiver.HistoryBootstrapContainer
		config    *HistoryArchiverConfig

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
	container archiver.HistoryBootstrapContainer,
	config *HistoryArchiverConfig,
) archiver.HistoryArchiver {
	return newHistoryArchiver(container, config, nil)
}

func newHistoryArchiver(
	container archiver.HistoryBootstrapContainer,
	config *HistoryArchiverConfig,
	historyIterator archiver.HistoryIterator,
) *historyArchiver {
	return &historyArchiver{
		container:       container,
		config:          config,
		historyIterator: historyIterator,
	}
}

func (h *historyArchiver) Archive(
	ctx context.Context,
	URI string,
	request *archiver.ArchiveHistoryRequest,
	opts ...archiver.ArchiveOption,
) error {
	logger := tagLoggerWithArchiveHistoryRequest(h.container.Logger, request)

	if err := h.ValidateURI(URI); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrInvalidURI), tag.Error(err), tag.ArchivalURI(URI))
		return archiver.ErrArchiveNonRetriable
	}

	if err := validateArchiveRequest(request); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrInvalidArchiveRequest), tag.Error(err))
		return archiver.ErrArchiveNonRetriable
	}

	historyIterator := h.historyIterator
	if historyIterator == nil { // will only be set by testing code
		var err error
		historyIterator, err = archiver.NewHistoryIterator(request, h.container.HistoryManager, h.container.HistoryV2Manager, h.config.HistoryIteratorConfig, nil, nil)
		if err != nil {
			// this should not happen
			logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrConstructHistoryIterator), tag.Error(err))
			return archiver.ErrArchiveNonRetriable
		}
	}

	historyBatches := []*shared.History{}
	for historyIterator.HasNext() {
		historyBlob, err := getNextHistoryBlob(ctx, historyIterator)
		if err != nil {
			logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReadHistory), tag.Error(err))
			return archiver.ErrArchiveNonRetriable
		}

		if historyMutated(request, historyBlob.Body, *historyBlob.Header.IsLast) {
			logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrHistoryMutated))
			return archiver.ErrArchiveNonRetriable
		}

		historyBatches = append(historyBatches, historyBlob.Body...)
	}

	encodedHistoryBatches, err := encodeHistoryBatches(historyBatches)
	if err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(errEncodeHistory), tag.Error(err))
		return archiver.ErrArchiveNonRetriable
	}

	dirPath := getDirPathFromURI(URI)
	if err = mkdirAll(dirPath); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(errMakeDirectory), tag.Error(err))
		return archiver.ErrArchiveNonRetriable
	}

	filename := constructFilename(request.DomainID, request.WorkflowID, request.RunID, request.CloseFailoverVersion)
	if err := writeFile(path.Join(dirPath, filename), encodedHistoryBatches); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(errWriteFile), tag.Error(err))
		return archiver.ErrArchiveNonRetriable
	}

	return nil
}

func (h *historyArchiver) Get(
	ctx context.Context,
	URI string,
	request *archiver.GetHistoryRequest,
) (*archiver.GetHistoryResponse, error) {
	if err := h.ValidateURI(URI); err != nil {
		return nil, fmt.Errorf("%s: %v", archiver.ErrInvalidURI, err)
	}

	if err := validateGetRequest(request); err != nil {
		return nil, archiver.ErrInvalidGetHistoryRequest
	}

	dirPath := getDirPathFromURI(URI)
	exists, err := directoryExists(dirPath)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, archiver.ErrHistoryNotExist
	}

	var token *getHistoryToken
	if request.NextPageToken != nil {
		token, err = deserializeGetHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, archiver.ErrGetHistoryTokenCorrupted
		}
	} else if request.CloseFailoverVersion != nil {
		token = &getHistoryToken{
			CloseFailoverVersion: *request.CloseFailoverVersion,
			NextBatchIdx:         0,
		}
	} else {
		highestVersion, err := getHighestVersion(dirPath, request)
		if err != nil {
			return nil, err
		}
		token = &getHistoryToken{
			CloseFailoverVersion: highestVersion,
			NextBatchIdx:         0,
		}
	}

	filename := constructFilename(request.DomainID, request.WorkflowID, request.RunID, token.CloseFailoverVersion)
	filepath := path.Join(dirPath, filename)
	exists, err = fileExists(filepath)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, archiver.ErrHistoryNotExist
	}

	encodedHistoryBatches, err := readFile(filepath)
	if err != nil {
		return nil, err
	}

	historyBatches, err := decodeHistoryBatches(encodedHistoryBatches)
	if err != nil {
		return nil, err
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
		nextToken, err := serializeGetHistoryToken(token)
		if err != nil {
			return nil, err
		}
		response.NextPageToken = nextToken
	}

	return response, nil
}

func (h *historyArchiver) ValidateURI(URI string) error {
	if !strings.HasPrefix(URI, URIScheme) {
		return archiver.ErrInvalidURIScheme
	}

	return validateDirPath(getDirPathFromURI(URI))
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

func getHighestVersion(dirPath string, request *archiver.GetHistoryRequest) (int64, error) {
	filenames, err := listFilesByPrefix(dirPath, constructFilenamePrefix(request.DomainID, request.WorkflowID, request.RunID))
	if err != nil {
		return -1, err
	}

	highestVersion := int64(-1)
	for _, filename := range filenames {
		version, err := extractCloseFailoverVersion(filename)
		if err != nil {
			return -1, err
		}
		if version > highestVersion {
			highestVersion = version
		}
	}
	return highestVersion, nil
}
