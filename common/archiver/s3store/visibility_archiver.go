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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"

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
		LogLevel:         (*aws.LogLevelType)(&config.LogLevel),
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
	handler := v.container.MetricsHandler.WithTags(metrics.OperationTag(metrics.VisibilityArchiverScope), metrics.NamespaceTag(request.Namespace))
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	startTime := time.Now().UTC()
	logger := archiver.TagLoggerWithArchiveVisibilityRequestAndURI(v.container.Logger, request, URI.String())
	archiveFailReason := ""
	defer func() {
		metrics.ServiceLatency.With(handler).Record(time.Since(startTime))
		if err != nil {
			if isRetryableError(err) {
				metrics.VisibilityArchiverArchiveTransientErrorCount.With(handler).Record(1)
				logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(archiveFailReason), tag.Error(err))
			} else {
				metrics.VisibilityArchiverArchiveNonRetryableErrorCount.With(handler).Record(1)
				logger.Error(archiver.ArchiveNonRetryableErrorMsg, tag.ArchivalArchiveFailReason(archiveFailReason), tag.Error(err))
				if featureCatalog.NonRetryableError != nil {
					err = featureCatalog.NonRetryableError()
				}
			}
		}
	}()

	if err := SoftValidateURI(URI); err != nil {
		archiveFailReason = archiver.ErrReasonInvalidURI
		return err
	}

	if err := archiver.ValidateVisibilityArchivalRequest(request); err != nil {
		archiveFailReason = archiver.ErrReasonInvalidArchiveRequest
		return err
	}

	encodedVisibilityRecord, err := Encode(request)
	if err != nil {
		archiveFailReason = errEncodeVisibilityRecord
		return err
	}
	indexes := createIndexesToArchive(request)
	// Upload archive to all indexes
	for _, element := range indexes {
		key := constructTimestampIndex(URI.Path(), request.GetNamespaceId(), element.primaryIndex, element.primaryIndexValue, element.secondaryIndex, element.secondaryIndexTimestamp, request.GetRunId())
		if err := Upload(ctx, v.s3cli, URI, key, encodedVisibilityRecord); err != nil {
			archiveFailReason = errWriteKey
			return err
		}
	}
	metrics.VisibilityArchiveSuccessCount.With(handler).Record(1)
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

	if err := SoftValidateURI(URI); err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidURI.Error())
	}

	if err := archiver.ValidateQueryRequest(request); err != nil {
		return nil, serviceerror.NewInvalidArgument(archiver.ErrInvalidQueryVisibilityRequest.Error())
	}

	if strings.TrimSpace(request.Query) == "" {
		return v.queryAll(ctx, URI, request, saTypeMap)
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

// queryAll returns all workflow executions in the archive.
func (v *visibilityArchiver) queryAll(
	ctx context.Context,
	uri archiver.URI,
	request *archiver.QueryVisibilityRequest,
	saTypeMap searchattribute.NameTypeMap,
) (*archiver.QueryVisibilityResponse, error) {
	// remaining is the number of workflow executions left to return before we reach pageSize.
	remaining := request.PageSize
	nextPageToken := request.NextPageToken
	var executions []*workflowpb.WorkflowExecutionInfo
	// We need to loop because the number of workflow executions returned by each call to query may be fewer than
	// pageSize. This is because we may have to skip some workflow executions after querying S3 (client-side filtering)
	// because there are 2 entries in S3 for each workflow execution indexed by workflowTypeName (one for closeTimeout
	// and one for startTimeout), and we only want to return one entry per workflow execution. See
	// createIndexesToArchive for a list of all indexes.
	for {
		searchPrefix := constructVisibilitySearchPrefix(uri.Path(), request.NamespaceID)
		// We suffix searchPrefix with workflowTypeName because the data in S3 is duplicated across combinations of 2
		// different primary indices (workflowID and workflowTypeName) and 2 different secondary indices (closeTimeout
		// and startTimeout). We only want to return one entry per workflow execution, but the full path to the S3 key
		// is <primaryIndexKey>/<primaryIndexValue>/<secondaryIndexKey>/<secondaryIndexValue>/<runID>, and we don't have
		// the primaryIndexValue when we make the call to query, so we can only specify the primaryIndexKey.
		searchPrefix += "/" + primaryIndexKeyWorkflowTypeName
		// The pageSize we supply here is actually the maximum number of keys to fetch from S3. For each execution,
		// there should be 2 keys in S3 for this prefix, so you might think that we should multiply the pageSize by 2.
		// However, if we do that, we may end up returning more than pageSize workflow executions to the end user of
		// this API. This is because we aren't guaranteed that both keys for a given workflow execution will be returned
		// in the same call. For example, if the user supplies a pageSize of 1, and we specify a maximum number of keys
		// of 2 to S3, we may get back entries from S3 for 2 different workflow executions. You might think that we can
		// just truncate this result to 1 workflow execution, but then the nextPageToken would be incorrect. So, we may
		// need to make multiple calls to S3 to get the correct number of workflow executions, which will probably make
		// this API call slower.
		res, err := v.queryPrefix(ctx, uri, &queryVisibilityRequest{
			namespaceID:   request.NamespaceID,
			pageSize:      remaining,
			nextPageToken: nextPageToken,
			parsedQuery:   &parsedQuery{},
		}, saTypeMap, searchPrefix, func(key string) bool {
			// We only want to return entries for the closeTimeout secondary index, which will always be of the form:
			// .../closeTimeout/<closeTimeout>/<runID>, so we split the key on "/" and check that the third-to-last
			// element is "closeTimeout".
			elements := strings.Split(key, "/")
			return len(elements) >= 3 && elements[len(elements)-3] == secondaryIndexKeyCloseTimeout
		})
		if err != nil {
			return nil, err
		}
		nextPageToken = res.NextPageToken
		executions = append(executions, res.Executions...)
		remaining -= len(res.Executions)
		if len(nextPageToken) == 0 || remaining <= 0 {
			break
		}
	}
	return &archiver.QueryVisibilityResponse{
		Executions:    executions,
		NextPageToken: nextPageToken,
	}, nil
}

func (v *visibilityArchiver) query(
	ctx context.Context,
	URI archiver.URI,
	request *queryVisibilityRequest,
	saTypeMap searchattribute.NameTypeMap,
) (*archiver.QueryVisibilityResponse, error) {
	primaryIndex := primaryIndexKeyWorkflowTypeName
	primaryIndexValue := request.parsedQuery.workflowTypeName
	if request.parsedQuery.workflowID != nil {
		primaryIndex = primaryIndexKeyWorkflowID
		primaryIndexValue = request.parsedQuery.workflowID
	}

	prefix := constructIndexedVisibilitySearchPrefix(
		URI.Path(),
		request.namespaceID,
		primaryIndex,
		*primaryIndexValue,
		secondaryIndexKeyCloseTimeout,
	) + "/"
	if request.parsedQuery.closeTime != nil {
		prefix = constructTimeBasedSearchKey(
			URI.Path(),
			request.namespaceID,
			primaryIndex,
			*primaryIndexValue,
			secondaryIndexKeyCloseTimeout,
			*request.parsedQuery.closeTime,
			*request.parsedQuery.searchPrecision,
		)
	}
	if request.parsedQuery.startTime != nil {
		prefix = constructTimeBasedSearchKey(
			URI.Path(),
			request.namespaceID,
			primaryIndex,
			*primaryIndexValue,
			secondaryIndexKeyStartTimeout,
			*request.parsedQuery.startTime,
			*request.parsedQuery.searchPrecision,
		)
	}

	return v.queryPrefix(ctx, URI, request, saTypeMap, prefix, nil)
}

// queryPrefix returns all workflow executions in the archive that match the given prefix. The keyFilter function is an
// optional filter that can be used to further filter the results. If keyFilter returns false for a given key, that key
// will be skipped, and the object will not be downloaded from S3 or included in the results.
func (v *visibilityArchiver) queryPrefix(
	ctx context.Context,
	uri archiver.URI,
	request *queryVisibilityRequest,
	saTypeMap searchattribute.NameTypeMap,
	prefix string,
	keyFilter func(key string) bool,
) (*archiver.QueryVisibilityResponse, error) {
	ctx, cancel := ensureContextTimeout(ctx)
	defer cancel()

	var token *string

	if request.nextPageToken != nil {
		token = deserializeQueryVisibilityToken(request.nextPageToken)
	}
	results, err := v.s3cli.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(uri.Hostname()),
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
		if keyFilter != nil && !keyFilter(*item.Key) {
			continue
		}

		encodedRecord, err := Download(ctx, v.s3cli, uri, *item.Key)
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
	err := SoftValidateURI(URI)
	if err != nil {
		return err
	}
	return BucketExists(context.TODO(), v.s3cli, URI)
}
