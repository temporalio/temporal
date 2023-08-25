// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
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

package filestore

import (
	"context"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
)

const (
	errEncodeVisibilityRecord = "failed to encode visibility record"
)

type (
	visibilityArchiver struct {
		container   *archiver.VisibilityBootstrapContainer
		fileMode    os.FileMode
		dirMode     os.FileMode
		queryParser QueryParser
	}

	queryVisibilityToken struct {
		LastCloseTime time.Time
		LastRunID     string
	}

	queryVisibilityRequest struct {
		namespaceID   string
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
	request *archiverspb.VisibilityRecord,
	opts ...archiver.ArchiveOption,
) (err error) {
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	defer func() {
		if err != nil && featureCatalog.NonRetryableError != nil {
			err = featureCatalog.NonRetryableError()
		}
	}()

	logger := archiver.TagLoggerWithArchiveVisibilityRequestAndURI(v.container.Logger, request, URI.String())

	if err := v.ValidateURI(URI); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidURI), tag.Error(err))
		return err
	}

	if err := archiver.ValidateVisibilityArchivalRequest(request); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidArchiveRequest), tag.Error(err))
		return err
	}

	dirPath := path.Join(URI.Path(), request.GetNamespaceId())
	if err = mkdirAll(dirPath, v.dirMode); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errMakeDirectory), tag.Error(err))
		return err
	}

	encodedVisibilityRecord, err := encode(request)
	if err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errEncodeVisibilityRecord), tag.Error(err))
		return err
	}

	// The filename has the format: closeTimestamp_hash(runID).visibility
	// This format allows the archiver to sort all records without reading the file contents
	filename := constructVisibilityFilename(request.CloseTime, request.GetRunId())
	if err := writeFile(path.Join(dirPath, filename), encodedVisibilityRecord, v.fileMode); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errWriteFile), tag.Error(err))
		return err
	}

	return nil
}

func (v *visibilityArchiver) Query(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.QueryVisibilityRequest,
	saTypeMap searchattribute.NameTypeMap,
) (*archiver.QueryVisibilityResponse, error) {
	if err := v.ValidateURI(URI); err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidURI.Error())
	}

	if err := archiver.ValidateQueryRequest(request); err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidQueryVisibilityRequest.Error())
	}

	parsedQuery, err := v.queryParser.Parse(request.Query)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}

	if parsedQuery.emptyResult {
		return &archiver.QueryVisibilityResponse{}, nil
	}

	return v.query(
		ctx,
		URI,
		&queryVisibilityRequest{
			namespaceID:   request.NamespaceID,
			pageSize:      request.PageSize,
			nextPageToken: request.NextPageToken,
			parsedQuery:   parsedQuery,
		},
		saTypeMap,
	)
}

func (v *visibilityArchiver) query(
	ctx context.Context,
	URI archiver.URI,
	request *queryVisibilityRequest,
	saTypeMap searchattribute.NameTypeMap,
) (*archiver.QueryVisibilityResponse, error) {
	var token *queryVisibilityToken
	if request.nextPageToken != nil {
		var err error
		token, err = deserializeQueryVisibilityToken(request.nextPageToken)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(archiver.ErrNextPageTokenCorrupted.Error())
		}
	}

	dirPath := path.Join(URI.Path(), request.namespaceID)
	exists, err := directoryExists(dirPath)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	if !exists {
		return &archiver.QueryVisibilityResponse{}, nil
	}

	files, err := listFiles(dirPath)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}

	files, err = sortAndFilterFiles(files, token)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	if len(files) == 0 {
		return &archiver.QueryVisibilityResponse{}, nil
	}

	response := &archiver.QueryVisibilityResponse{}
	for idx, file := range files {
		encodedRecord, err := readFile(path.Join(dirPath, file))
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}

		record, err := decodeVisibilityRecord(encodedRecord)
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}

		if record.CloseTime.Before(request.parsedQuery.earliestCloseTime) {
			break
		}

		if matchQuery(record, request.parsedQuery) {
			executionInfo, err := convertToExecutionInfo(record, saTypeMap)
			if err != nil {
				return nil, serviceerror.NewInternal(err.Error())
			}

			response.Executions = append(response.Executions, executionInfo)
			if len(response.Executions) == request.pageSize {
				if idx != len(files) {
					newToken := &queryVisibilityToken{
						LastCloseTime: timestamp.TimeValue(record.CloseTime),
						LastRunID:     record.GetRunId(),
					}
					encodedToken, err := serializeToken(newToken)
					if err != nil {
						return nil, serviceerror.NewInternal(err.Error())
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
	closeTime   time.Time
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
			closeTime:   timestamp.UnixOrZeroTime(closeTime),
			hashedRunID: pieces[1],
		})
	}

	sort.Slice(parsedFilenames, func(i, j int) bool {
		if parsedFilenames[i].closeTime.Equal(parsedFilenames[j].closeTime) {
			return parsedFilenames[i].hashedRunID > parsedFilenames[j].hashedRunID
		}
		return parsedFilenames[i].closeTime.After(parsedFilenames[j].closeTime)
	})

	startIdx := 0
	if token != nil {
		LastHashedRunID := hash(token.LastRunID)
		startIdx = sort.Search(len(parsedFilenames), func(i int) bool {
			if parsedFilenames[i].closeTime.Equal(token.LastCloseTime) {
				return parsedFilenames[i].hashedRunID < LastHashedRunID
			}
			return parsedFilenames[i].closeTime.Before(token.LastCloseTime)
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

func matchQuery(record *archiverspb.VisibilityRecord, query *parsedQuery) bool {
	if record.CloseTime.Before(query.earliestCloseTime) || record.CloseTime.After(query.latestCloseTime) {
		return false
	}
	if query.workflowID != nil && record.GetWorkflowId() != *query.workflowID {
		return false
	}
	if query.runID != nil && record.GetRunId() != *query.runID {
		return false
	}
	if query.workflowTypeName != nil && record.WorkflowTypeName != *query.workflowTypeName {
		return false
	}
	if query.status != nil && record.Status != *query.status {
		return false
	}
	return true
}

func convertToExecutionInfo(record *archiverspb.VisibilityRecord, saTypeMap searchattribute.NameTypeMap) (*workflowpb.WorkflowExecutionInfo, error) {
	searchAttributes, err := searchattribute.Parse(record.SearchAttributes, &saTypeMap)
	if err != nil {
		return nil, err
	}

	return &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: record.GetWorkflowId(),
			RunId:      record.GetRunId(),
		},
		Type: &commonpb.WorkflowType{
			Name: record.WorkflowTypeName,
		},
		StartTime:        record.StartTime,
		ExecutionTime:    record.ExecutionTime,
		CloseTime:        record.CloseTime,
		Status:           record.Status,
		HistoryLength:    record.HistoryLength,
		Memo:             record.Memo,
		SearchAttributes: searchAttributes,
	}, nil
}
