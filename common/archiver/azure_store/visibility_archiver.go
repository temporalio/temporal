package azure_store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"go.temporal.io/api/serviceerror"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/azure_store/connector"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/searchattribute"
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
		logger         log.Logger
		metricsHandler metrics.Handler
		azureStorage   connector.Client
		queryParser    QueryParser
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

func newVisibilityArchiver(logger log.Logger, metricsHandler metrics.Handler, storage connector.Client) *visibilityArchiver {
	return &visibilityArchiver{
		logger:         logger,
		metricsHandler: metricsHandler,
		azureStorage:   storage,
		queryParser:    NewQueryParser(),
	}
}

// NewVisibilityArchiver creates a new archiver.VisibilityArchiver based on azure blob storage
func NewVisibilityArchiver(logger log.Logger, metricsHandler metrics.Handler, cfg *config.AzblobArchiver) (archiver.VisibilityArchiver, error) {
	storage, err := connector.NewClient(cfg, logger)
	return newVisibilityArchiver(logger, metricsHandler, storage), err
}

func (v *visibilityArchiver) Archive(ctx context.Context, URI archiver.URI, request *archiverspb.VisibilityRecord, opts ...archiver.ArchiveOption) (err error) {
	handler := v.metricsHandler.WithTags(metrics.OperationTag(metrics.VisibilityArchiverScope), metrics.NamespaceTag(request.Namespace))
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	startTime := time.Now().UTC()
	defer func() {
		metrics.ServiceLatency.With(handler).Record(time.Since(startTime))
		if err != nil {
			if isRetryableError(err) {
				metrics.VisibilityArchiverArchiveTransientErrorCount.With(handler).Record(1)
			} else {
				metrics.VisibilityArchiverArchiveNonRetryableErrorCount.With(handler).Record(1)
				if featureCatalog.NonRetryableError != nil {
					err = featureCatalog.NonRetryableError()
				}
			}
		}
	}()

	logger := archiver.TagLoggerWithArchiveVisibilityRequestAndURI(v.logger, request, URI.String())

	if err := v.ValidateURI(URI); err != nil {
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

	filename := constructVisibilityFilename(request.GetNamespaceId(), request.WorkflowTypeName, request.GetWorkflowId(), request.GetRunId(), indexKeyCloseTimeout, request.CloseTime.AsTime())
	if err := v.azureStorage.Upload(ctx, URI, filename, encodedVisibilityRecord); err != nil {
		logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(errWriteFile), tag.Error(err))
		return errRetryable
	}

	filename = constructVisibilityFilename(request.GetNamespaceId(), request.WorkflowTypeName, request.GetWorkflowId(), request.GetRunId(), indexKeyStartTimeout, request.StartTime.AsTime())
	if err := v.azureStorage.Upload(ctx, URI, filename, encodedVisibilityRecord); err != nil {
		logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(errWriteFile), tag.Error(err))
		return errRetryable
	}

	metrics.VisibilityArchiveSuccessCount.With(handler).Record(1)
	return nil
}

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

	if strings.TrimSpace(request.Query) == "" {
		return v.queryAll(ctx, URI, request, saTypeMap)
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
	uri archiver.URI,
	request *queryVisibilityRequest,
	saTypeMap searchattribute.NameTypeMap,
) (*archiver.QueryVisibilityResponse, error) {
	prefix := constructVisibilityFilenamePrefix(request.namespaceID, indexKeyCloseTimeout)
	if !request.parsedQuery.closeTime.IsZero() {
		prefix = constructTimeBasedSearchKey(
			request.namespaceID,
			indexKeyCloseTimeout,
			request.parsedQuery.closeTime,
			*request.parsedQuery.searchPrecision,
		)
	}

	if !request.parsedQuery.startTime.IsZero() {
		prefix = constructTimeBasedSearchKey(
			request.namespaceID,
			indexKeyStartTimeout,
			request.parsedQuery.startTime,
			*request.parsedQuery.searchPrecision,
		)
	}

	return v.queryPrefix(ctx, uri, request, saTypeMap, prefix)
}

func (v *visibilityArchiver) queryAll(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.QueryVisibilityRequest,
	saTypeMap searchattribute.NameTypeMap,
) (*archiver.QueryVisibilityResponse, error) {

	return v.queryPrefix(ctx, URI, &queryVisibilityRequest{
		namespaceID:   request.NamespaceID,
		pageSize:      request.PageSize,
		nextPageToken: request.NextPageToken,
		parsedQuery:   &parsedQuery{},
	}, saTypeMap, request.NamespaceID)
}

func (v *visibilityArchiver) queryPrefix(ctx context.Context, uri archiver.URI, request *queryVisibilityRequest, saTypeMap searchattribute.NameTypeMap, prefix string) (*archiver.QueryVisibilityResponse, error) {
	token, err := v.parseToken(request.nextPageToken)
	if err != nil {
		return nil, err
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

	filenames, completed, currentCursorPos, err := v.azureStorage.QueryWithFilters(ctx, uri, prefix, request.pageSize, token.Offset, filters)
	if err != nil {
		return nil, &serviceerror.InvalidArgument{Message: err.Error()}
	}

	response := &archiver.QueryVisibilityResponse{}
	for _, file := range filenames {
		encodedRecord, err := v.azureStorage.Get(ctx, uri, fmt.Sprintf("%s/%s", request.namespaceID, filepath.Base(file)))
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

func (v *visibilityArchiver) parseToken(nextPageToken []byte) (*queryVisibilityToken, error) {
	token := new(queryVisibilityToken)
	if nextPageToken != nil {
		var err error
		token, err = deserializeQueryVisibilityToken(nextPageToken)
		if err != nil {
			return nil, &serviceerror.InvalidArgument{Message: archiver.ErrNextPageTokenCorrupted.Error()}
		}
	}
	return token, nil
}

func (v *visibilityArchiver) ValidateURI(URI archiver.URI) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutInSeconds*time.Second)
	defer cancel()

	if err = v.validateURI(URI); err == nil {
		_, err = v.azureStorage.Exist(ctx, URI, "")
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
