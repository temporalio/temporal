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

package gcloud

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/searchattribute"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/gcloud/connector"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	errEncodeVisibilityRecord = "failed to encode visibility record"
	indexKeyStartTimeout      = "startTimeout"
	indexKeyCloseTimeout      = "closeTimeout"
	timeoutInSeconds          = 5
)

var (
	errRetryable = errors.New("retryable error")
)

type (
	visibilityArchiver struct {
		container     *archiver.VisibilityBootstrapContainer
		gcloudStorage connector.Client
		queryParser   QueryParser
	}

	queryVisibilityToken struct {
		Offset int
	}

	queryVisibilityRequest struct {
		namespaceID   string
		pageSize      int
		nextPageToken []byte
		parsedQuery   *parsedQuery
	}
)

func newVisibilityArchiver(container *archiver.VisibilityBootstrapContainer, storage connector.Client) *visibilityArchiver {
	return &visibilityArchiver{
		container:     container,
		gcloudStorage: storage,
		queryParser:   NewQueryParser(),
	}
}

// NewVisibilityArchiver creates a new archiver.VisibilityArchiver based on filestore
func NewVisibilityArchiver(container *archiver.VisibilityBootstrapContainer, config *config.GstorageArchiver) (archiver.VisibilityArchiver, error) {
	storage, err := connector.NewClient(context.Background(), config)
	return newVisibilityArchiver(container, storage), err
}

// Archive is used to archive one workflow visibility record.
// Check the Archive() method of the HistoryArchiver interface in Step 2 for parameters' meaning and requirements.
// The only difference is that the ArchiveOption parameter won't include an option for recording process.
// Please make sure your implementation is lossless. If any in-memory batching mechanism is used, then those batched records will be lost during server restarts.
// This method will be invoked when workflow closes. Note that because of conflict resolution, it is possible for a workflow to through the closing process multiple times, which means that this method can be invoked more than once after a workflow closes.
func (v *visibilityArchiver) Archive(ctx context.Context, URI archiver.URI, request *archiverspb.VisibilityRecord, opts ...archiver.ArchiveOption) (err error) {
	scope := v.container.MetricsClient.Scope(metrics.HistoryArchiverScope, metrics.NamespaceTag(request.Namespace))
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	sw := scope.StartTimer(metrics.ServiceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if isRetryableError(err) {
				scope.IncCounter(metrics.VisibilityArchiverArchiveTransientErrorCount)
			} else {
				scope.IncCounter(metrics.VisibilityArchiverArchiveNonRetryableErrorCount)
				if featureCatalog.NonRetryableError != nil {
					err = featureCatalog.NonRetryableError()
				}
			}
		}
	}()

	logger := archiver.TagLoggerWithArchiveVisibilityRequestAndURI(v.container.Logger, request, URI.String())

	if err := v.ValidateURI(URI); err != nil {
		if isRetryableError(err) {
			logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidURI), tag.Error(err))
			return err
		}
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidURI), tag.Error(err))
		return err
	}

	if err := archiver.ValidateVisibilityArchivalRequest(request); err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidArchiveRequest), tag.Error(err))
		return err
	}

	encodedVisibilityRecord, err := encode(request)
	if err != nil {
		logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(errEncodeVisibilityRecord), tag.Error(err))
		return err
	}

	// The filename has the format: closeTimestamp_hash(runID).visibility
	// This format allows the archiver to sort all records without reading the file contents
	filename := constructVisibilityFilename(request.GetNamespaceId(), request.WorkflowTypeName, request.GetWorkflowId(), request.GetRunId(), indexKeyCloseTimeout, timestamp.TimeValue(request.CloseTime))
	if err := v.gcloudStorage.Upload(ctx, URI, filename, encodedVisibilityRecord); err != nil {
		logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(errWriteFile), tag.Error(err))
		return errRetryable
	}

	filename = constructVisibilityFilename(request.GetNamespaceId(), request.WorkflowTypeName, request.GetWorkflowId(), request.GetRunId(), indexKeyStartTimeout, timestamp.TimeValue(request.StartTime))
	if err := v.gcloudStorage.Upload(ctx, URI, filename, encodedVisibilityRecord); err != nil {
		logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(errWriteFile), tag.Error(err))
		return errRetryable
	}

	scope.IncCounter(metrics.VisibilityArchiveSuccessCount)
	return nil
}

// Query is used to retrieve archived visibility records.
// Check the Get() method of the HistoryArchiver interface in Step 2 for parameters' meaning and requirements.
// The request includes a string field called query, which describes what kind of visibility records should be returned. For example, it can be some SQL-like syntax query string.
// Your implementation is responsible for parsing and validating the query, and also returning all visibility records that match the query.
// Currently the maximum context timeout passed into the method is 3 minutes, so it's ok if this method takes a long time to run.
func (v *visibilityArchiver) Query(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.QueryVisibilityRequest,
	saTypeMap searchattribute.NameTypeMap,
) (*archiver.QueryVisibilityResponse, error) {

	if err := v.ValidateURI(URI); err != nil {
		return nil, &serviceerror.InvalidArgument{Message: archiver.ErrInvalidURI.Error()}
	}

	if err := archiver.ValidateQueryRequest(request); err != nil {
		return nil, &serviceerror.InvalidArgument{Message: archiver.ErrInvalidQueryVisibilityRequest.Error()}
	}

	parsedQuery, err := v.queryParser.Parse(request.Query)
	if err != nil {
		return nil, &serviceerror.InvalidArgument{Message: err.Error()}
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

	token := new(queryVisibilityToken)
	if request.nextPageToken != nil {
		var err error
		token, err = deserializeQueryVisibilityToken(request.nextPageToken)
		if err != nil {
			return nil, &serviceerror.InvalidArgument{Message: archiver.ErrNextPageTokenCorrupted.Error()}
		}
	}

	var prefix = constructVisibilityFilenamePrefix(request.namespaceID, indexKeyCloseTimeout)
	if !request.parsedQuery.closeTime.IsZero() {
		prefix = constructTimeBasedSearchKey(request.namespaceID, indexKeyCloseTimeout, request.parsedQuery.closeTime, *request.parsedQuery.searchPrecision)
	}
	if !request.parsedQuery.startTime.IsZero() {
		prefix = constructTimeBasedSearchKey(request.namespaceID, indexKeyStartTimeout, request.parsedQuery.startTime, *request.parsedQuery.searchPrecision)
	}

	filters := make([]connector.Precondition, 0)
	if request.parsedQuery.workflowID != nil {
		filters = append(filters, newWorkflowIDPrecondition(hash(*request.parsedQuery.workflowID)))
	}

	if request.parsedQuery.runID != nil {
		filters = append(filters, newWorkflowIDPrecondition(hash(*request.parsedQuery.runID)))
	}

	if request.parsedQuery.workflowType != nil {
		filters = append(filters, newWorkflowIDPrecondition(hash(*request.parsedQuery.workflowType)))
	}

	filenames, completed, currentCursorPos, err := v.gcloudStorage.QueryWithFilters(ctx, URI, prefix, request.pageSize, token.Offset, filters)
	if err != nil {
		return nil, &serviceerror.InvalidArgument{Message: err.Error()}
	}

	response := &archiver.QueryVisibilityResponse{}
	for _, file := range filenames {
		encodedRecord, err := v.gcloudStorage.Get(ctx, URI, fmt.Sprintf("%s/%s", request.namespaceID, filepath.Base(file)))
		if err != nil {
			return nil, &serviceerror.InvalidArgument{Message: err.Error()}
		}

		record, err := decodeVisibilityRecord(encodedRecord)
		if err != nil {
			return nil, &serviceerror.InvalidArgument{Message: err.Error()}
		}

		executionInfo, err := convertToExecutionInfo(record, saTypeMap)
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}
		response.Executions = append(response.Executions, executionInfo)
	}

	if !completed {
		newToken := &queryVisibilityToken{
			Offset: currentCursorPos,
		}
		encodedToken, err := serializeToken(newToken)
		if err != nil {
			return nil, &serviceerror.InvalidArgument{Message: err.Error()}
		}
		response.NextPageToken = encodedToken
	}

	return response, nil
}

// ValidateURI is used to define what a valid URI for an implementation is.
func (v *visibilityArchiver) ValidateURI(URI archiver.URI) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutInSeconds*time.Second)
	defer cancel()

	if err = v.validateURI(URI); err == nil {
		_, err = v.gcloudStorage.Exist(ctx, URI, "")
	}

	return
}

func (v *visibilityArchiver) validateURI(URI archiver.URI) (err error) {
	if URI.Scheme() != URIScheme {
		return archiver.ErrURISchemeMismatch
	}

	if URI.Path() == "" || URI.Hostname() == "" {
		return archiver.ErrInvalidURI
	}

	return
}
