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

	"github.com/uber/cadence/common/metrics"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/service/config"
)

type (
	visibilityArchiver struct {
		container   *archiver.VisibilityBootstrapContainer
		s3cli       s3iface.S3API
		queryParser QueryParser
	}

	visibilityRecord archiver.ArchiveVisibilityRequest

	queryVisibilityRequest struct {
		domainID      string
		pageSize      int
		nextPageToken []byte
		parsedQuery   *parsedQuery
	}

	indexToArchive struct {
		primaryIndex            string
		primaryIndexValue       string
		secondaryIndex          string
		secondaryIndexTimestamp int64
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
	request *archiver.ArchiveVisibilityRequest,
	opts ...archiver.ArchiveOption,
) (err error) {
	scope := v.container.MetricsClient.Scope(metrics.VisibilityArchiverScope, metrics.DomainTag(request.DomainName))
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	sw := scope.StartTimer(metrics.CadenceLatency)
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
				logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiveFailReason), tag.Error(err))
				if featureCatalog.NonRetriableError != nil {
					err = featureCatalog.NonRetriableError()
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
		key := constructTimestampIndex(URI.Path(), request.DomainID, element.primaryIndex, element.primaryIndexValue, element.secondaryIndex, element.secondaryIndexTimestamp, request.RunID)
		if err := upload(ctx, v.s3cli, URI, key, encodedVisibilityRecord); err != nil {
			archiveFailReason = errWriteKey
			return err
		}
	}
	scope.IncCounter(metrics.VisibilityArchiveSuccessCount)
	return nil
}
func createIndexesToArchive(request *archiver.ArchiveVisibilityRequest) []indexToArchive {
	return []indexToArchive{
		{primaryIndexKeyWorkflowTypeName, request.WorkflowTypeName, secondaryIndexKeyCloseTimeout, request.CloseTimestamp},
		{primaryIndexKeyWorkflowTypeName, request.WorkflowTypeName, secondaryIndexKeyStartTimeout, request.StartTimestamp},
		{primaryIndexKeyWorkflowID, request.WorkflowID, secondaryIndexKeyCloseTimeout, request.CloseTimestamp},
		{primaryIndexKeyWorkflowID, request.WorkflowID, secondaryIndexKeyStartTimeout, request.StartTimestamp},
	}
}

func (v *visibilityArchiver) Query(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.QueryVisibilityRequest,
) (*archiver.QueryVisibilityResponse, error) {
	if err := softValidateURI(URI); err != nil {
		return nil, &shared.BadRequestError{Message: archiver.ErrInvalidURI.Error()}
	}

	if err := archiver.ValidateQueryRequest(request); err != nil {
		return nil, &shared.BadRequestError{Message: archiver.ErrInvalidQueryVisibilityRequest.Error()}
	}

	parsedQuery, err := v.queryParser.Parse(request.Query)
	if err != nil {
		return nil, &shared.BadRequestError{Message: err.Error()}
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
	var prefix = constructVisibilitySearchPrefix(URI.Path(), request.domainID, primaryIndex, *primaryIndexValue, secondaryIndexKeyCloseTimeout) + "/"
	if request.parsedQuery.closeTime != nil {
		prefix = constructTimeBasedSearchKey(URI.Path(), request.domainID, primaryIndex, *primaryIndexValue, secondaryIndexKeyCloseTimeout, *request.parsedQuery.closeTime, *request.parsedQuery.searchPrecision)
	}
	if request.parsedQuery.startTime != nil {
		prefix = constructTimeBasedSearchKey(URI.Path(), request.domainID, primaryIndex, *primaryIndexValue, secondaryIndexKeyStartTimeout, *request.parsedQuery.startTime, *request.parsedQuery.searchPrecision)
	}

	results, err := v.s3cli.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(URI.Hostname()),
		Prefix:            aws.String(prefix),
		MaxKeys:           aws.Int64(int64(request.pageSize)),
		ContinuationToken: token,
	})
	if err != nil {
		if isRetryableError(err) {
			return nil, &shared.InternalServiceError{Message: err.Error()}
		}
		return nil, &shared.BadRequestError{Message: err.Error()}
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
			return nil, &shared.InternalServiceError{Message: err.Error()}
		}

		record, err := decodeVisibilityRecord(encodedRecord)
		if err != nil {
			return nil, &shared.InternalServiceError{Message: err.Error()}
		}
		response.Executions = append(response.Executions, convertToExecutionInfo(record))
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
