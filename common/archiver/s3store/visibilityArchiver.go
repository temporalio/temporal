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

package s3store

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/searchattribute"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	visibilityArchiver struct {
		container   *archiver.VisibilityBootstrapContainer
		s3cli       s3iface.S3API
		queryParser QueryParser
	}

	queryVisibilityRequest struct {
		namespaceID   string
		pageSize      int
		nextPageToken []byte
		parsedQuery   *parsedQuery
	}

	indexToArchive struct {
		primaryIndex            string
		primaryIndexValue       string
		secondaryIndex          string
		secondaryIndexTimestamp time.Time
	}
)

const (
	errEncodeVisibilityRecord       = "failed to encode visibility record"
	secondaryIndexKeyStartTimeout   = "startTimeout"
	secondaryIndexKeyCloseTimeout   = "closeTimeout"
	primaryIndexKeyWorkflowTypeName = "workflowTypeName"
	primaryIndexKeyWorkflowID       = "workflowID"
)

// NewVisibilityArchiver creates a new archiver.VisibilityArchiver based on s3
func NewVisibilityArchiver(
	container *archiver.VisibilityBootstrapContainer,
	config *config.S3Archiver,
) (archiver.VisibilityArchiver, error) {
	return newVisibilityArchiver(container, config)
}

func newVisibilityArchiver(
	container *archiver.VisibilityBootstrapContainer,
	config *config.S3Archiver) (*visibilityArchiver, error) {
	s3Config := &aws.Config{
		Endpoint:         config.Endpoint,
		Region:           aws.String(config.Region),
		S3ForcePathStyle: aws.Bool(config.S3ForcePathStyle),
	}
	sess, err := session.NewSession(s3Config)
	if err != nil {
		return nil, err
	}
	return &visibilityArchiver{
		container:   container,
		s3cli:       s3.New(sess),
		queryParser: NewQueryParser(),
	}, nil
}

func (v *visibilityArchiver) Archive(
	ctx context.Context,
	URI archiver.URI,
	request *archiverspb.VisibilityRecord,
	opts ...archiver.ArchiveOption,
) (err error) {
	scope := v.container.MetricsClient.Scope(metrics.VisibilityArchiverScope, metrics.NamespaceTag(request.Namespace))
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	sw := scope.StartTimer(metrics.ServiceLatency)
	logger := archiver.TagLoggerWithArchiveVisibilityRequestAndURI(v.container.Logger, request, URI.String())
	archiveFailReason := ""
	defer func() {
		sw.Stop()
		if err != nil {
			if isRetryableError(err) {
				scope.IncCounter(metrics.VisibilityArchiverArchiveTransientErrorCount)
				logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(archiveFailReason), tag.Error(err))
			} else {
				scope.IncCounter(metrics.VisibilityArchiverArchiveNonRetryableErrorCount)
				logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiveFailReason), tag.Error(err))
				if featureCatalog.NonRetryableError != nil {
					err = featureCatalog.NonRetryableError()
				}
			}
		}
	}()

	if err := softValidateURI(URI); err != nil {
		archiveFailReason = archiver.ErrReasonInvalidURI
		return err
	}

	if err := archiver.ValidateVisibilityArchivalRequest(request); err != nil {
		archiveFailReason = archiver.ErrReasonInvalidArchiveRequest
		return err
	}

	encodedVisibilityRecord, err := encode(request)
	if err != nil {
		archiveFailReason = errEncodeVisibilityRecord
		return err
	}
	indexes := createIndexesToArchive(request)
	// Upload archive to all indexes
	for _, element := range indexes {
		key := constructTimestampIndex(URI.Path(), request.GetNamespaceId(), element.primaryIndex, element.primaryIndexValue, element.secondaryIndex, element.secondaryIndexTimestamp, request.GetRunId())
		if err := upload(ctx, v.s3cli, URI, key, encodedVisibilityRecord); err != nil {
			archiveFailReason = errWriteKey
			return err
		}
	}
	scope.IncCounter(metrics.VisibilityArchiveSuccessCount)
	return nil
}
func createIndexesToArchive(request *archiverspb.VisibilityRecord) []indexToArchive {
	return []indexToArchive{
		{primaryIndexKeyWorkflowTypeName, request.WorkflowTypeName, secondaryIndexKeyCloseTimeout, timestamp.TimeValue(request.CloseTime)},
		{primaryIndexKeyWorkflowTypeName, request.WorkflowTypeName, secondaryIndexKeyStartTimeout, timestamp.TimeValue(request.StartTime)},
		{primaryIndexKeyWorkflowID, request.GetWorkflowId(), secondaryIndexKeyCloseTimeout, timestamp.TimeValue(request.CloseTime)},
		{primaryIndexKeyWorkflowID, request.GetWorkflowId(), secondaryIndexKeyStartTimeout, timestamp.TimeValue(request.StartTime)},
	}
}

func (v *visibilityArchiver) Query(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.QueryVisibilityRequest,
	saTypeMap searchattribute.NameTypeMap,
) (*archiver.QueryVisibilityResponse, error) {

	if err := softValidateURI(URI); err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidURI.Error())
	}

	if err := archiver.ValidateQueryRequest(request); err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidQueryVisibilityRequest.Error())
	}

	parsedQuery, err := v.queryParser.Parse(request.Query)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(err.Error())
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
	ctx, cancel := ensureContextTimeout(ctx)
	defer cancel()
	var token *string
	if request.nextPageToken != nil {
		token = deserializeQueryVisibilityToken(request.nextPageToken)
	}
	primaryIndex := primaryIndexKeyWorkflowTypeName
	primaryIndexValue := request.parsedQuery.workflowTypeName
	if request.parsedQuery.workflowID != nil {
		primaryIndex = primaryIndexKeyWorkflowID
		primaryIndexValue = request.parsedQuery.workflowID
	}
	var prefix = constructVisibilitySearchPrefix(URI.Path(), request.namespaceID, primaryIndex, *primaryIndexValue, secondaryIndexKeyCloseTimeout) + "/"
	if request.parsedQuery.closeTime != nil {
		prefix = constructTimeBasedSearchKey(URI.Path(), request.namespaceID, primaryIndex, *primaryIndexValue, secondaryIndexKeyCloseTimeout, *request.parsedQuery.closeTime, *request.parsedQuery.searchPrecision)
	}
	if request.parsedQuery.startTime != nil {
		prefix = constructTimeBasedSearchKey(URI.Path(), request.namespaceID, primaryIndex, *primaryIndexValue, secondaryIndexKeyStartTimeout, *request.parsedQuery.startTime, *request.parsedQuery.searchPrecision)
	}

	results, err := v.s3cli.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(URI.Hostname()),
		Prefix:            aws.String(prefix),
		MaxKeys:           aws.Int64(int64(request.pageSize)),
		ContinuationToken: token,
	})
	if err != nil {
		if isRetryableError(err) {
			return nil, serviceerror.NewUnavailable(err.Error())
		}
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}
	if len(results.Contents) == 0 {
		return &archiver.QueryVisibilityResponse{}, nil
	}

	response := &archiver.QueryVisibilityResponse{}
	if *results.IsTruncated {
		response.NextPageToken = serializeQueryVisibilityToken(*results.NextContinuationToken)
	}
	for _, item := range results.Contents {
		encodedRecord, err := download(ctx, v.s3cli, URI, *item.Key)
		if err != nil {
			return nil, serviceerror.NewUnavailable(err.Error())
		}

		record, err := decodeVisibilityRecord(encodedRecord)
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}
		executionInfo, err := convertToExecutionInfo(record, saTypeMap)
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}
		response.Executions = append(response.Executions, executionInfo)
	}
	return response, nil
}

func (v *visibilityArchiver) ValidateURI(URI archiver.URI) error {
	err := softValidateURI(URI)
	if err != nil {
		return err
	}
	return bucketExists(context.TODO(), v.s3cli, URI)
}
