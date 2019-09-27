// Copyright (c) 2019 Uber Technologies, Inc.
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

package filestore

import (
	"context"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/service/config"
)

const (
	errEncodeVisibilityRecord    = "failed to encode visibility record"
	errInvalidVisibilityFilename = "failed to parse visibility file name"
)

type (
	visibilityArchiver struct {
		container   *archiver.VisibilityBootstrapContainer
		fileMode    os.FileMode
		dirMode     os.FileMode
		queryParser QueryParser
	}

	queryVisibilityToken struct {
		LastCloseTime int64
		LastRunID     string
	}

	visibilityRecord archiver.ArchiveVisibilityRequest

	queryVisibilityRequest struct {
		domainID      string
		pageSize      int
		nextPageToken []byte
		parsedQuery   *parsedQuery
	}
)

// NewVisibilityArchiver creates a new archiver.VisibilityArchiver based on filestore
func NewVisibilityArchiver(
	container *archiver.VisibilityBootstrapContainer,
	config *config.FilestoreArchiver,
) (archiver.VisibilityArchiver, error) {
	fileMode, err := strconv.ParseUint(config.FileMode, 0, 32)
	if err != nil {
		return nil, errInvalidFileMode
	}
	dirMode, err := strconv.ParseUint(config.DirMode, 0, 32)
	if err != nil {
		return nil, errInvalidDirMode
	}
	return &visibilityArchiver{
		container:   container,
		fileMode:    os.FileMode(fileMode),
		dirMode:     os.FileMode(dirMode),
		queryParser: NewQueryParser(),
	}, nil
}

func (v *visibilityArchiver) Archive(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.ArchiveVisibilityRequest,
	opts ...archiver.ArchiveOption,
) (err error) {
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	defer func() {
		if err != nil && featureCatalog.NonRetriableError != nil {
			err = featureCatalog.NonRetriableError()
		}
	}()

	logger := archiver.TagLoggerWithArchiveVisibilityRequestAndURI(v.container.Logger, request, URI.String())

	if err := v.ValidateURI(URI); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidURI), tag.Error(err))
		return err
	}

	if err := archiver.ValidateVisibilityArchivalRequest(request); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidArchiveRequest), tag.Error(err))
		return err
	}

	dirPath := path.Join(URI.Path(), request.DomainID)
	if err = mkdirAll(dirPath, v.dirMode); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(errMakeDirectory), tag.Error(err))
		return err
	}

	encodedVisibilityRecord, err := encode(request)
	if err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(errEncodeVisibilityRecord), tag.Error(err))
		return err
	}

	// The filename has the format: closeTimestamp_hash(runID).visibility
	// This format allows the archiver to sort all records without reading the file contents
	filename := constructVisibilityFilename(request.CloseTimestamp, request.RunID)
	if err := writeFile(path.Join(dirPath, filename), encodedVisibilityRecord, v.fileMode); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(errWriteFile), tag.Error(err))
		return err
	}

	return nil
}

func (v *visibilityArchiver) Query(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.QueryVisibilityRequest,
) (*archiver.QueryVisibilityResponse, error) {
	if err := v.ValidateURI(URI); err != nil {
		return nil, &shared.BadRequestError{Message: archiver.ErrInvalidURI.Error()}
	}

	if err := archiver.ValidateQueryRequest(request); err != nil {
		return nil, &shared.BadRequestError{Message: archiver.ErrInvalidQueryVisibilityRequest.Error()}
	}

	parsedQuery, err := v.queryParser.Parse(request.Query)
	if err != nil {
		return nil, &shared.BadRequestError{Message: err.Error()}
	}

	if parsedQuery.emptyResult {
		return &archiver.QueryVisibilityResponse{}, nil
	}

	return v.query(ctx, URI, &queryVisibilityRequest{
		domainID:      request.DomainID,
		pageSize:      request.PageSize,
		nextPageToken: request.NextPageToken,
		parsedQuery:   parsedQuery,
	})
}

func (v *visibilityArchiver) query(
	ctx context.Context,
	URI archiver.URI,
	request *queryVisibilityRequest,
) (*archiver.QueryVisibilityResponse, error) {
	var token *queryVisibilityToken
	if request.nextPageToken != nil {
		var err error
		token, err = deserializeQueryVisibilityToken(request.nextPageToken)
		if err != nil {
			return nil, &shared.BadRequestError{Message: archiver.ErrNextPageTokenCorrupted.Error()}
		}
	}

	dirPath := path.Join(URI.Path(), request.domainID)
	exists, err := directoryExists(dirPath)
	if err != nil {
		return nil, &shared.InternalServiceError{Message: err.Error()}
	}
	if !exists {
		return &archiver.QueryVisibilityResponse{}, nil
	}

	files, err := listFiles(dirPath)
	if err != nil {
		return nil, &shared.InternalServiceError{Message: err.Error()}
	}

	files, err = sortAndFilterFiles(files, token)
	if err != nil {
		return nil, &shared.InternalServiceError{Message: err.Error()}
	}
	if len(files) == 0 {
		return &archiver.QueryVisibilityResponse{}, nil
	}

	response := &archiver.QueryVisibilityResponse{}
	for idx, file := range files {
		encodedRecord, err := readFile(path.Join(dirPath, file))
		if err != nil {
			return nil, &shared.InternalServiceError{Message: err.Error()}
		}

		record, err := decodeVisibilityRecord(encodedRecord)
		if err != nil {
			return nil, &shared.InternalServiceError{Message: err.Error()}
		}

		if record.CloseTimestamp < request.parsedQuery.earliestCloseTime {
			break
		}

		if matchQuery(record, request.parsedQuery) {
			response.Executions = append(response.Executions, convertToExecutionInfo(record))
			if len(response.Executions) == request.pageSize {
				if idx != len(files) {
					newToken := &queryVisibilityToken{
						LastCloseTime: record.CloseTimestamp,
						LastRunID:     record.RunID,
					}
					encodedToken, err := serializeToken(newToken)
					if err != nil {
						return nil, &shared.InternalServiceError{Message: err.Error()}
					}
					response.NextPageToken = encodedToken
				}
				break
			}
		}
	}

	return response, nil
}

func (v *visibilityArchiver) ValidateURI(URI archiver.URI) error {
	if URI.Scheme() != URIScheme {
		return archiver.ErrURISchemeMismatch
	}

	return validateDirPath((URI.Path()))
}

type parsedVisFilename struct {
	name        string
	closeTime   int64
	hashedRunID string
}

// sortAndFilterFiles sort visibility record file names based on close timestamp (desc) and use hashed runID to break ties.
// if a nextPageToken is give, it only returns filenames that have a smaller close timestamp
func sortAndFilterFiles(filenames []string, token *queryVisibilityToken) ([]string, error) {
	var parsedFilenames []*parsedVisFilename
	for _, name := range filenames {
		pieces := strings.FieldsFunc(name, func(r rune) bool {
			return r == '_' || r == '.'
		})
		if len(pieces) != 3 {
			return nil, fmt.Errorf("failed to parse visibility filename %s", name)
		}

		closeTime, err := strconv.ParseInt(pieces[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse visibility filename %s", name)
		}
		parsedFilenames = append(parsedFilenames, &parsedVisFilename{
			name:        name,
			closeTime:   closeTime,
			hashedRunID: pieces[1],
		})
	}

	sort.Slice(parsedFilenames, func(i, j int) bool {
		if parsedFilenames[i].closeTime == parsedFilenames[j].closeTime {
			return parsedFilenames[i].hashedRunID > parsedFilenames[j].hashedRunID
		}
		return parsedFilenames[i].closeTime > parsedFilenames[j].closeTime
	})

	startIdx := 0
	if token != nil {
		LastHashedRunID := hash(token.LastRunID)
		startIdx = sort.Search(len(parsedFilenames), func(i int) bool {
			if parsedFilenames[i].closeTime == token.LastCloseTime {
				return parsedFilenames[i].hashedRunID < LastHashedRunID
			}
			return parsedFilenames[i].closeTime < token.LastCloseTime
		})
	}

	if startIdx == len(parsedFilenames) {
		return []string{}, nil
	}

	var filteredFilenames []string
	for _, parsedFilename := range parsedFilenames[startIdx:] {
		filteredFilenames = append(filteredFilenames, parsedFilename.name)
	}
	return filteredFilenames, nil
}

func matchQuery(record *visibilityRecord, query *parsedQuery) bool {
	if record.CloseTimestamp < query.earliestCloseTime || record.CloseTimestamp > query.latestCloseTime {
		return false
	}
	if query.workflowID != nil && record.WorkflowID != *query.workflowID {
		return false
	}
	if query.runID != nil && record.RunID != *query.runID {
		return false
	}
	if query.workflowTypeName != nil && record.WorkflowTypeName != *query.workflowTypeName {
		return false
	}
	if query.closeStatus != nil && record.CloseStatus != *query.closeStatus {
		return false
	}
	return true
}

func convertToExecutionInfo(record *visibilityRecord) *shared.WorkflowExecutionInfo {
	return &shared.WorkflowExecutionInfo{
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(record.WorkflowID),
			RunId:      common.StringPtr(record.RunID),
		},
		Type: &shared.WorkflowType{
			Name: common.StringPtr(record.WorkflowTypeName),
		},
		StartTime:     common.Int64Ptr(record.StartTimestamp),
		ExecutionTime: common.Int64Ptr(record.ExecutionTimestamp),
		CloseTime:     common.Int64Ptr(record.CloseTimestamp),
		CloseStatus:   record.CloseStatus.Ptr(),
		HistoryLength: common.Int64Ptr(record.HistoryLength),
		Memo:          record.Memo,
		SearchAttributes: &shared.SearchAttributes{
			IndexedFields: archiver.ConvertSearchAttrToBytes(record.SearchAttributes),
		},
	}
}
