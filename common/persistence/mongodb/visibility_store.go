package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"google.golang.org/protobuf/proto"
)

const (
	collectionVisibilityExecutions       = "visibility_executions"
	collectionVisibilitySearchAttributes = "visibility_search_attributes"
)

type (
	visibilityStore struct {
		transactionalStore

		db                             client.Database
		cfg                            config.MongoDB
		logger                         log.Logger
		transactionsEnabled            bool
		searchAttributesProvider       searchattribute.Provider
		searchAttributesMapperProvider searchattribute.MapperProvider
		chasmRegistry                  chasmComponentProvider

		executionsCol       client.Collection
		searchAttributesCol client.Collection
	}

	chasmComponentProvider interface {
		ComponentByID(chasm.ArchetypeID) (*chasm.RegistrableComponent, bool)
	}

	visibilityExecutionDocument struct {
		ID                   string         `bson:"_id"`
		NamespaceID          string         `bson:"namespace_id"`
		WorkflowID           string         `bson:"workflow_id"`
		RunID                string         `bson:"run_id"`
		WorkflowTypeName     string         `bson:"workflow_type_name"`
		TaskQueue            string         `bson:"task_queue,omitempty"`
		StartTime            time.Time      `bson:"start_time"`
		ExecutionTime        time.Time      `bson:"execution_time"`
		VisibilityTime       time.Time      `bson:"visibility_time"`
		CloseTime            *time.Time     `bson:"close_time,omitempty"`
		ExecutionDuration    *time.Duration `bson:"execution_duration,omitempty"`
		HistoryLength        *int64         `bson:"history_length,omitempty"`
		HistorySizeBytes     *int64         `bson:"history_size_bytes,omitempty"`
		StateTransitionCount *int64         `bson:"state_transition_count,omitempty"`
		Status               int32          `bson:"status"`
		Memo                 []byte         `bson:"memo,omitempty"`
		MemoEncoding         string         `bson:"memo_encoding,omitempty"`
		SearchAttributes     []byte         `bson:"search_attributes_data,omitempty"`
		SearchAttributesEnc  string         `bson:"search_attributes_encoding,omitempty"`
		SearchAttributesMap  map[string]any `bson:"search_attributes"`
		ParentWorkflowID     *string        `bson:"parent_workflow_id,omitempty"`
		ParentRunID          *string        `bson:"parent_run_id,omitempty"`
		RootWorkflowID       string         `bson:"root_workflow_id"`
		RootRunID            string         `bson:"root_run_id"`
		Version              int64          `bson:"version"`
	}

	visibilityPageToken struct {
		VisibilityTime time.Time `json:"visibility_time"`
		RunID          string    `json:"run_id"`
	}
)

var _ store.VisibilityStore = (*visibilityStore)(nil)

func NewVisibilityStore(
	db client.Database,
	mongoClient client.Client,
	cfg config.MongoDB,
	logger log.Logger,
	metricsHandler metrics.Handler,
	saProvider searchattribute.Provider,
	saMapperProvider searchattribute.MapperProvider,
	chasmRegistry chasmComponentProvider,
	transactionsEnabled bool,
) (store.VisibilityStore, error) {
	if !transactionsEnabled {
		return nil, errors.New("mongodb visibility store requires transactions-enabled topology")
	}
	if mongoClient == nil {
		return nil, errors.New("mongodb visibility store requires client with session support")
	}

	if saProvider == nil {
		saProvider = searchattribute.NewSystemProvider()
	}
	if saMapperProvider == nil {
		saMapperProvider = searchattribute.NewMapperProvider(nil, nil, saProvider, false)
	}

	vs := &visibilityStore{
		transactionalStore:             newTransactionalStore(mongoClient, metricsHandler),
		db:                             db,
		cfg:                            cfg,
		logger:                         logger,
		transactionsEnabled:            transactionsEnabled,
		searchAttributesProvider:       saProvider,
		searchAttributesMapperProvider: saMapperProvider,
		chasmRegistry:                  chasmRegistry,
		executionsCol:                  db.Collection(collectionVisibilityExecutions),
		searchAttributesCol:            db.Collection(collectionVisibilitySearchAttributes),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := vs.ensureIndexes(ctx); err != nil {
		return nil, err
	}

	return vs, nil
}

func (s *visibilityStore) Close() {}

func (s *visibilityStore) GetName() string {
	return "mongodb"
}

func (s *visibilityStore) GetIndexName() string {
	// Return the database name to match how search attributes are keyed in cluster metadata.
	// This ensures custom search attributes saved during test setup can be retrieved correctly.
	return s.cfg.DatabaseName
}

func (s *visibilityStore) ensureIndexes(ctx context.Context) error {
	idxView := s.executionsCol.Indexes()
	indexes := []struct {
		name  string
		model mongo.IndexModel
	}{
		{
			name: "visibility_namespace_workflow_run",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "namespace_id", Value: 1},
					{Key: "workflow_id", Value: 1},
					{Key: "run_id", Value: 1},
				},
				Options: options.Index().SetName("visibility_namespace_workflow_run").SetUnique(true),
			},
		},
		{
			name: "visibility_start_time",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "namespace_id", Value: 1},
					{Key: "start_time", Value: -1},
					{Key: "run_id", Value: 1},
				},
				Options: options.Index().SetName("visibility_start_time"),
			},
		},
		{
			name: "visibility_close_time",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "namespace_id", Value: 1},
					{Key: "close_time", Value: -1},
					{Key: "run_id", Value: 1},
				},
				Options: options.Index().SetName("visibility_close_time"),
			},
		},
		{
			name: "visibility_visibility_time",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "namespace_id", Value: 1},
					{Key: "visibility_time", Value: -1},
					{Key: "run_id", Value: 1},
				},
				Options: options.Index().SetName("visibility_visibility_time"),
			},
		},
	}

	for _, spec := range indexes {
		if _, err := idxView.CreateOne(ctx, spec.model); err != nil && !isDuplicateIndexError(err) {
			return serviceerror.NewUnavailablef("failed to ensure visibility index %q: %v", spec.name, err)
		}
	}

	return s.ensureSearchAttributeIndexes(ctx)
}

func (s *visibilityStore) ensureSearchAttributeIndexes(ctx context.Context) error {
	if s.searchAttributesCol == nil {
		return nil
	}

	idxView := s.searchAttributesCol.Indexes()
	model := mongo.IndexModel{
		Keys:    bson.D{{Key: "name", Value: 1}},
		Options: options.Index().SetName("visibility_sa_name_unique").SetUnique(true),
	}

	if _, err := idxView.CreateOne(ctx, model); err != nil && !isDuplicateIndexError(err) {
		return serviceerror.NewUnavailablef("failed to ensure search attribute index: %v", err)
	}
	return nil
}

func (s *visibilityStore) ValidateCustomSearchAttributes(searchAttributes map[string]any) (map[string]any, error) {
	return searchAttributes, nil
}

func (s *visibilityStore) RecordWorkflowExecutionStarted(ctx context.Context, request *store.InternalRecordWorkflowExecutionStartedRequest) error {
	if request == nil || request.InternalVisibilityRequestBase == nil {
		return serviceerror.NewInvalidArgument("RecordWorkflowExecutionStarted request is nil")
	}

	doc, err := s.buildVisibilityDocument(ctx, request.InternalVisibilityRequestBase)
	if err != nil {
		return err
	}

	doc.VisibilityTime = doc.StartTime
	doc.CloseTime = nil
	doc.ExecutionDuration = nil
	doc.HistoryLength = nil
	doc.HistorySizeBytes = nil
	doc.StateTransitionCount = nil

	_, err = s.executeTransaction(ctx, func(sessCtx context.Context) (interface{}, error) {
		_, err := s.executionsCol.InsertOne(sessCtx, doc)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return nil, nil
			}
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		return serviceerror.NewUnavailablef("failed to insert visibility execution: %v", err)
	}
	return nil
}

func (s *visibilityStore) RecordWorkflowExecutionClosed(ctx context.Context, request *store.InternalRecordWorkflowExecutionClosedRequest) error {
	if request == nil || request.InternalVisibilityRequestBase == nil {
		return serviceerror.NewInvalidArgument("RecordWorkflowExecutionClosed request is nil")
	}

	doc, err := s.buildVisibilityDocument(ctx, request.InternalVisibilityRequestBase)
	if err != nil {
		return err
	}

	removedTransient, err := s.stripTransientSearchAttributes(doc)
	if err != nil {
		return err
	}
	shouldUnsetTransient := removedTransient || enumspb.WorkflowExecutionStatus(doc.Status) != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING

	closeTime := request.CloseTime.UTC()
	doc.CloseTime = &closeTime
	doc.VisibilityTime = closeTime

	duration := request.ExecutionDuration
	doc.ExecutionDuration = &duration

	historyLength := request.HistoryLength
	doc.HistoryLength = &historyLength

	historySize := request.HistorySizeBytes
	doc.HistorySizeBytes = &historySize

	stateTransitions := request.StateTransitionCount
	doc.StateTransitionCount = &stateTransitions

	updateSet, err := visibilityDocumentUpdate(doc)
	if err != nil {
		return err
	}

	updateOps := bson.M{
		"$set":         updateSet,
		"$setOnInsert": bson.M{"_id": doc.ID},
	}
	if shouldUnsetTransient {
		updateOps["$unset"] = bson.M{sadefs.TemporalPauseInfo: ""}
	}

	filter := bson.M{"_id": doc.ID}
	filter["$or"] = versionMatchClauses(doc.Version)
	if enumspb.WorkflowExecutionStatus(doc.Status) == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		filter["status"] = bson.M{"$in": []int32{0, int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)}}
	}

	_, err = s.executeTransaction(ctx, func(sessCtx context.Context) (interface{}, error) {
		_, err := s.executionsCol.UpdateOne(sessCtx,
			filter,
			updateOps,
			options.Update().SetUpsert(true),
		)
		return nil, err
	})
	if err != nil {
		return serviceerror.NewUnavailablef("failed to upsert closed visibility execution: %v", err)
	}
	return nil
}

func (s *visibilityStore) UpsertWorkflowExecution(ctx context.Context, request *store.InternalUpsertWorkflowExecutionRequest) error {
	if request == nil || request.InternalVisibilityRequestBase == nil {
		return serviceerror.NewInvalidArgument("UpsertWorkflowExecution request is nil")
	}

	doc, err := s.buildVisibilityDocument(ctx, request.InternalVisibilityRequestBase)
	if err != nil {
		return err
	}

	doc.VisibilityTime = doc.StartTime

	removedTransient, err := s.stripTransientSearchAttributes(doc)
	if err != nil {
		return err
	}

	updateSet, err := visibilityDocumentUpdate(doc)
	if err != nil {
		return err
	}

	updateOps := bson.M{
		"$set":         updateSet,
		"$setOnInsert": bson.M{"_id": doc.ID},
	}
	if removedTransient {
		updateOps["$unset"] = bson.M{sadefs.TemporalPauseInfo: ""}
	}

	_, err = s.executeTransaction(ctx, func(sessCtx context.Context) (interface{}, error) {
		_, err := s.executionsCol.UpdateOne(sessCtx,
			bson.M{
				"_id": doc.ID,
				"$or": versionMatchClauses(doc.Version),
			},
			updateOps,
			options.Update().SetUpsert(true),
		)
		return nil, err
	})
	if err != nil {
		return serviceerror.NewUnavailablef("failed to upsert visibility execution: %v", err)
	}
	return nil
}

func (s *visibilityStore) DeleteWorkflowExecution(ctx context.Context, request *manager.VisibilityDeleteWorkflowExecutionRequest) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("DeleteWorkflowExecution request is nil")
	}

	_, err := s.executeTransaction(ctx, func(sessCtx context.Context) (interface{}, error) {
		_, err := s.executionsCol.DeleteOne(sessCtx, bson.M{
			"_id": visibilityDocID(request.NamespaceID.String(), request.WorkflowID, request.RunID),
		})
		return nil, err
	})
	if err != nil {
		return serviceerror.NewUnavailablef("failed to delete visibility execution: %v", err)
	}
	return nil
}

// buildListResponse constructs the response for list operations, handling pagination tokens.
func buildListResponse(
	docs []*visibilityExecutionDocument,
	pageSize int64,
	saTypeMap searchattribute.NameTypeMap,
) (*store.InternalListExecutionsResponse, error) {
	response := &store.InternalListExecutionsResponse{}
	if len(docs) == 0 {
		return response, nil
	}

	// Match ES behavior: return a token if we got at least pageSize results
	// (might have more results on next page)
	if int64(len(docs)) >= pageSize {
		last := docs[min(int64(len(docs))-1, pageSize-1)]
		tokenBytes, err := encodeVisibilityPageToken(&visibilityPageToken{
			VisibilityTime: last.VisibilityTime,
			RunID:          last.RunID,
		})
		if err != nil {
			return nil, err
		}
		response.NextPageToken = tokenBytes
		if int64(len(docs)) > pageSize {
			docs = docs[:pageSize]
		}
	}

	executions := make([]*store.InternalExecutionInfo, 0, len(docs))
	for _, doc := range docs {
		info, err := documentToExecutionInfo(doc, saTypeMap)
		if err != nil {
			return nil, err
		}
		executions = append(executions, info)
	}
	response.Executions = executions
	return response, nil
}

func (s *visibilityStore) ListWorkflowExecutions(ctx context.Context, request *manager.ListWorkflowExecutionsRequestV2) (*store.InternalListExecutionsResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("ListWorkflowExecutions request is nil")
	}
	if request.NamespaceID == namespace.EmptyID {
		return nil, serviceerror.NewInvalidArgument("namespace ID is required")
	}

	pageSize := int64(request.PageSize)
	if pageSize <= 0 {
		pageSize = 1000
	}

	filters := []bson.M{{"namespace_id": string(request.NamespaceID)}}

	params, err := s.convertQueryParams(
		ctx,
		request.NamespaceID,
		request.Namespace,
		request.Query,
		nil,
		chasm.UnspecifiedArchetypeID,
	)
	if err != nil {
		return nil, err
	}
	if params != nil && params.QueryExpr != nil {
		filters = append(filters, params.QueryExpr)
	}

	var sort bson.D
	if params != nil {
		sort, err = buildMongoSort(params.OrderBy)
		if err != nil {
			return nil, err
		}
	}
	if sort == nil {
		sort = bson.D{
			{Key: "visibility_time", Value: -1},
			{Key: "run_id", Value: 1},
		}
	}

	token, err := decodeVisibilityPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}
	if token != nil {
		filters = append(filters, bson.M{"$or": []bson.M{
			{"visibility_time": bson.M{"$lt": token.VisibilityTime}},
			{
				"visibility_time": token.VisibilityTime,
				"run_id":          bson.M{"$gt": token.RunID},
			},
		}})
	}

	filter := mongoAnd(filters...)
	if len(filter) == 0 {
		filter = bson.M{}
	}

	findOpts := options.Find().
		SetSort(sort).
		SetLimit(pageSize + 1)

	cursor, err := s.executionsCol.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to list visibility executions: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	docs := make([]*visibilityExecutionDocument, 0, pageSize+1)
	for cursor.Next(ctx) {
		var doc visibilityExecutionDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("failed to decode visibility execution: %v", err)
		}
		docs = append(docs, &doc)
	}
	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("failed during visibility listing: %v", err)
	}

	saTypeMap, err := s.getSearchAttributes(ctx)
	if err != nil {
		return nil, err
	}

	return buildListResponse(docs, pageSize, saTypeMap)
}

func (s *visibilityStore) ListChasmExecutions(ctx context.Context, request *manager.ListChasmExecutionsRequest) (*store.InternalListExecutionsResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("ListChasmExecutions request is nil")
	}
	if request.NamespaceID == namespace.EmptyID {
		return nil, serviceerror.NewInvalidArgument("namespace ID is required")
	}
	if s.chasmRegistry == nil {
		return nil, serviceerror.NewUnavailable("chasm registry not configured")
	}

	rc, ok := s.chasmRegistry.ComponentByID(request.ArchetypeID)
	if !ok {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unknown archetype ID: %d", request.ArchetypeID))
	}
	mapper := rc.SearchAttributesMapper()

	pageSize := int64(request.PageSize)
	if pageSize <= 0 {
		pageSize = 1000
	}

	filters := []bson.M{{"namespace_id": string(request.NamespaceID)}}

	qFilter, err := s.buildQueryFilter(
		ctx,
		request.NamespaceID,
		request.Namespace,
		request.Query,
		mapper,
		request.ArchetypeID,
	)
	if err != nil {
		return nil, err
	}
	if qFilter != nil {
		filters = append(filters, qFilter)
	}

	token, err := decodeVisibilityPageToken(request.NextPageToken)
	if err != nil {
		return nil, err
	}
	if token != nil {
		filters = append(filters, bson.M{"$or": []bson.M{
			{"visibility_time": bson.M{"$lt": token.VisibilityTime}},
			{
				"visibility_time": token.VisibilityTime,
				"run_id":          bson.M{"$gt": token.RunID},
			},
		}})
	}

	filter := mongoAnd(filters...)
	if len(filter) == 0 {
		filter = bson.M{}
	}

	findOpts := options.Find().
		SetSort(bson.D{{Key: "visibility_time", Value: -1}, {Key: "run_id", Value: 1}}).
		SetLimit(pageSize + 1)

	cursor, err := s.executionsCol.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to list chasm executions: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	docs := make([]*visibilityExecutionDocument, 0, pageSize+1)
	for cursor.Next(ctx) {
		var doc visibilityExecutionDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("failed to decode chasm visibility execution: %v", err)
		}
		docs = append(docs, &doc)
	}
	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("failed during chasm visibility listing: %v", err)
	}

	saTypeMap, err := s.getSearchAttributes(ctx)
	if err != nil {
		return nil, err
	}

	return buildListResponse(docs, pageSize, saTypeMap)
}

func (s *visibilityStore) CountChasmExecutions(ctx context.Context, request *manager.CountChasmExecutionsRequest) (*store.InternalCountExecutionsResponse, error) {
	if err := s.validateCountChasmRequest(request); err != nil {
		return nil, err
	}

	rc, ok := s.chasmRegistry.ComponentByID(request.ArchetypeID)
	if !ok {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unknown archetype ID: %d", request.ArchetypeID))
	}

	filter, params, err := s.buildChasmFilter(ctx, request, rc.SearchAttributesMapper())
	if err != nil {
		return nil, err
	}

	if params != nil && len(params.GroupBy) > 0 {
		return s.countChasmWithGroupBy(ctx, filter, params.GroupBy[0])
	}

	count, err := s.executionsCol.CountDocuments(ctx, filter)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to count chasm executions: %v", err)
	}

	return &store.InternalCountExecutionsResponse{Count: count}, nil
}

func (s *visibilityStore) validateCountChasmRequest(request *manager.CountChasmExecutionsRequest) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("CountChasmExecutions request is nil")
	}
	if request.NamespaceID == namespace.EmptyID {
		return serviceerror.NewInvalidArgument("namespace ID is required")
	}
	if s.chasmRegistry == nil {
		return serviceerror.NewUnavailable("chasm registry not configured")
	}
	return nil
}

func (s *visibilityStore) buildChasmFilter(ctx context.Context, request *manager.CountChasmExecutionsRequest, mapper *chasm.VisibilitySearchAttributesMapper) (bson.M, *query.QueryParams[bson.M], error) {
	filter := mongoAnd(bson.M{"namespace_id": string(request.NamespaceID)})

	if request.Query == "" {
		return filter, nil, nil
	}

	params, err := s.convertQueryParams(ctx, request.NamespaceID, request.Namespace, request.Query, mapper, request.ArchetypeID)
	if err != nil {
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, nil, converterErr.ToInvalidArgument()
		}
		return nil, nil, err
	}
	if params != nil && params.QueryExpr != nil {
		filter = mongoAnd(filter, params.QueryExpr)
	}
	return filter, params, nil
}

func (s *visibilityStore) countChasmWithGroupBy(ctx context.Context, filter bson.M, groupCol *query.SAColumn) (*store.InternalCountExecutionsResponse, error) {
	field, err := mongoFieldName(groupCol.FieldName)
	if err != nil {
		return nil, err
	}

	pipeline := mongo.Pipeline{}
	if len(filter) > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: filter}})
	}
	pipeline = append(pipeline,
		bson.D{{Key: "$group", Value: bson.M{
			"_id":   fmt.Sprintf("$%s", field),
			"count": bson.M{"$sum": 1},
		}}},
		bson.D{{Key: "$sort", Value: bson.M{"_id": 1}}},
	)

	cursor, err := s.executionsCol.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to aggregate chasm executions: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	type aggregationResult struct {
		ID    any   `bson:"_id"`
		Count int64 `bson:"count"`
	}

	var rawResults []aggregationResult
	if err := cursor.All(ctx, &rawResults); err != nil {
		return nil, serviceerror.NewUnavailablef("failed to read aggregation results: %v", err)
	}

	internalGroups := make([]store.InternalAggregationGroup, 0, len(rawResults))
	var total int64
	for _, res := range rawResults {
		payload, err := encodeGroupValuePayload(groupCol, res.ID)
		if err != nil {
			return nil, err
		}
		internalGroups = append(internalGroups, store.InternalAggregationGroup{
			GroupValues: []*commonpb.Payload{payload},
			Count:       res.Count,
		})
		total += res.Count
	}

	return &store.InternalCountExecutionsResponse{Count: total, Groups: internalGroups}, nil
}

func (s *visibilityStore) CountWorkflowExecutions(ctx context.Context, request *manager.CountWorkflowExecutionsRequest) (*store.InternalCountExecutionsResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("CountWorkflowExecutions request is nil")
	}
	if request.NamespaceID == namespace.EmptyID {
		return nil, serviceerror.NewInvalidArgument("namespace ID is required")
	}

	params, err := s.convertQueryParams(
		ctx,
		request.NamespaceID,
		request.Namespace,
		request.Query,
		nil,
		chasm.UnspecifiedArchetypeID,
	)
	if err != nil {
		return nil, err
	}

	filter := bson.M{"namespace_id": string(request.NamespaceID)}
	if params != nil && params.QueryExpr != nil {
		filter = mongoAnd(filter, params.QueryExpr)
	}

	if params != nil && len(params.GroupBy) > 0 {
		groupCol := params.GroupBy[0]
		field, err := mongoFieldName(groupCol.FieldName)
		if err != nil {
			return nil, err
		}

		pipeline := mongo.Pipeline{}
		if len(filter) > 0 {
			pipeline = append(pipeline, bson.D{{Key: "$match", Value: filter}})
		}
		pipeline = append(pipeline,
			bson.D{{Key: "$group", Value: bson.M{
				"_id":   fmt.Sprintf("$%s", field),
				"count": bson.M{"$sum": 1},
			}}},
			bson.D{{Key: "$sort", Value: bson.M{"_id": 1}}},
		)

		cursor, err := s.executionsCol.Aggregate(ctx, pipeline)
		if err != nil {
			return nil, serviceerror.NewUnavailablef("failed to aggregate visibility executions: %v", err)
		}
		defer func() { _ = cursor.Close(ctx) }()

		type aggregationResult struct {
			ID    any   `bson:"_id"`
			Count int64 `bson:"count"`
		}

		var rawResults []aggregationResult
		if err := cursor.All(ctx, &rawResults); err != nil {
			return nil, serviceerror.NewUnavailablef("failed to read aggregation results: %v", err)
		}

		internalGroups := make([]store.InternalAggregationGroup, 0, len(rawResults))
		var total int64
		for _, res := range rawResults {
			payload, err := encodeGroupValuePayload(groupCol, res.ID)
			if err != nil {
				return nil, err
			}
			internalGroups = append(internalGroups, store.InternalAggregationGroup{
				GroupValues: []*commonpb.Payload{payload},
				Count:       res.Count,
			})
			total += res.Count
		}

		return &store.InternalCountExecutionsResponse{Count: total, Groups: internalGroups}, nil
	}

	count, err := s.executionsCol.CountDocuments(ctx, filter)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to count visibility executions: %v", err)
	}

	return &store.InternalCountExecutionsResponse{Count: count}, nil
}

func (s *visibilityStore) GetWorkflowExecution(ctx context.Context, request *manager.GetWorkflowExecutionRequest) (*store.InternalGetWorkflowExecutionResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("GetWorkflowExecution request is nil")
	}
	if request.NamespaceID == namespace.EmptyID {
		return nil, serviceerror.NewInvalidArgument("namespace ID is required")
	}
	if request.RunID == "" {
		return nil, serviceerror.NewInvalidArgument("run ID is required")
	}

	filter := bson.M{
		"namespace_id": string(request.NamespaceID),
		"run_id":       request.RunID,
	}
	if request.WorkflowID != "" {
		filter = bson.M{
			"_id": visibilityDocID(string(request.NamespaceID), request.WorkflowID, request.RunID),
		}
	}

	result := s.executionsCol.FindOne(ctx, filter)
	if err := result.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNotFound("visibility execution not found")
		}
		return nil, serviceerror.NewUnavailablef("failed to load visibility execution: %v", err)
	}

	var doc visibilityExecutionDocument
	if err := result.Decode(&doc); err != nil {
		return nil, serviceerror.NewUnavailablef("failed to decode visibility execution: %v", err)
	}

	saTypeMap, err := s.getSearchAttributes(ctx)
	if err != nil {
		return nil, err
	}

	info, err := documentToExecutionInfo(&doc, saTypeMap)
	if err != nil {
		return nil, err
	}

	return &store.InternalGetWorkflowExecutionResponse{Execution: info}, nil
}

func (s *visibilityStore) AddSearchAttributes(ctx context.Context, request *manager.AddSearchAttributesRequest) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("AddSearchAttributes request is nil")
	}
	if len(request.SearchAttributes) == 0 {
		return nil
	}
	if s.searchAttributesCol == nil {
		return serviceerror.NewUnavailable("search attributes collection not configured")
	}

	now := time.Now().UTC()
	updateOpts := options.Update().SetUpsert(true)

	for name, valueType := range request.SearchAttributes {
		if name == "" {
			return serviceerror.NewInvalidArgument("search attribute name is empty")
		}

		update := bson.M{
			"$set": bson.M{
				"name":       name,
				"value_type": int32(valueType),
				"updated_at": now,
			},
		}

		if _, err := s.searchAttributesCol.UpdateOne(ctx, bson.M{"name": name}, update, updateOpts); err != nil {
			if isDuplicateIndexError(err) {
				continue
			}
			return serviceerror.NewUnavailablef("failed to upsert search attribute %q: %v", name, err)
		}
	}

	return nil
}

func (s *visibilityStore) getSearchAttributes(ctx context.Context) (searchattribute.NameTypeMap, error) {
	typeMap, err := s.searchAttributesProvider.GetSearchAttributes(s.GetIndexName(), false)
	if err != nil {
		return searchattribute.NameTypeMap{}, serviceerror.NewUnavailablef("unable to read search attribute types: %v", err)
	}

	if s.searchAttributesCol == nil {
		return typeMap, nil
	}

	cursor, err := s.searchAttributesCol.Find(ctx, bson.M{})
	if err != nil {
		return searchattribute.NameTypeMap{}, serviceerror.NewUnavailablef("failed to read custom search attributes from mongodb: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var results []struct {
		Name      string `bson:"name"`
		ValueType int32  `bson:"value_type"`
	}
	if err := cursor.All(ctx, &results); err != nil {
		return searchattribute.NameTypeMap{}, serviceerror.NewUnavailablef("failed to decode custom search attributes from mongodb: %v", err)
	}

	if len(results) == 0 {
		return typeMap, nil
	}

	customAttributes := typeMap.Custom()
	newCustomAttributes := make(map[string]enumspb.IndexedValueType, len(customAttributes)+len(results))
	for k, v := range customAttributes {
		newCustomAttributes[k] = v
	}

	for _, res := range results {
		newCustomAttributes[res.Name] = enumspb.IndexedValueType(res.ValueType)
	}

	return searchattribute.NewNameTypeMap(newCustomAttributes), nil
}

func (s *visibilityStore) buildQueryFilter(
	ctx context.Context,
	nsID namespace.ID,
	nsName namespace.Name,
	queryString string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) (bson.M, error) {
	params, err := s.convertQueryParams(ctx, nsID, nsName, queryString, chasmMapper, archetypeID)
	if err != nil {
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}
	if params == nil {
		return nil, nil
	}
	return params.QueryExpr, nil
}

func (s *visibilityStore) convertQueryParams(
	ctx context.Context,
	_ namespace.ID,
	nsName namespace.Name,
	queryString string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) (*query.QueryParams[bson.M], error) {
	saTypeMap, err := s.getSearchAttributes(ctx)
	if err != nil {
		return nil, err
	}
	combinedTypeMap := store.CombineTypeMaps(saTypeMap, chasmMapper)

	mapper, err := s.searchAttributesMapperProvider.GetMapper(nsName)
	if err != nil {
		return nil, err
	}

	converter := query.NewQueryConverter[bson.M](
		newMongoQueryConverter(),
		nsName,
		combinedTypeMap,
		mapper,
	).WithChasmMapper(chasmMapper).WithArchetypeID(archetypeID)

	params, err := converter.Convert(queryString)
	if err != nil {
		return nil, err
	}
	return params, nil
}

func encodeGroupValuePayload(col *query.SAColumn, raw any) (*commonpb.Payload, error) {
	var value any
	switch col.FieldName {
	case sadefs.ExecutionStatus:
		status, err := toWorkflowExecutionStatus(raw)
		if err != nil {
			return nil, err
		}
		value = status.String()
	default:
		value = raw
	}

	switch col.ValueType {
	case enumspb.INDEXED_VALUE_TYPE_INT:
		switch v := value.(type) {
		case int32:
			value = int64(v)
		case int64:
			value = v
		case float64:
			value = int64(v)
		}
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		switch v := value.(type) {
		case int32:
			value = float64(v)
		case int64:
			value = float64(v)
		case float64:
			value = v
		}
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD, enumspb.INDEXED_VALUE_TYPE_TEXT:
		switch v := value.(type) {
		case fmt.Stringer:
			value = v.String()
		case int32:
			value = fmt.Sprint(v)
		case int64:
			value = fmt.Sprint(v)
		case float64:
			value = fmt.Sprint(int64(v))
		}
	default:
		// No additional normalization required for other types.
	}

	payload, err := searchattribute.EncodeValue(value, col.ValueType)
	if err != nil {
		return nil, serviceerror.NewInternalf("failed to encode aggregation value for %s: %v", col.FieldName, err)
	}
	return payload, nil
}

func toWorkflowExecutionStatus(raw any) (enumspb.WorkflowExecutionStatus, error) {
	switch v := raw.(type) {
	case int32:
		return enumspb.WorkflowExecutionStatus(v), nil
	case int64:
		return enumspb.WorkflowExecutionStatus(v), nil
	case float64:
		return enumspb.WorkflowExecutionStatus(int32(v)), nil
	case string:
		if val, ok := enumspb.WorkflowExecutionStatus_value[v]; ok {
			return enumspb.WorkflowExecutionStatus(val), nil
		}
	case nil:
		return 0, serviceerror.NewInternal("group aggregation returned nil execution status")
	}
	return 0, serviceerror.NewInternalf("unsupported aggregation key type %T for ExecutionStatus", raw)
}

func (s *visibilityStore) buildVisibilityDocument(ctx context.Context, base *store.InternalVisibilityRequestBase) (*visibilityExecutionDocument, error) {
	if base == nil {
		return nil, serviceerror.NewInvalidArgument("visibility request base is nil")
	}

	startTime := base.StartTime.UTC()
	execTime := base.ExecutionTime.UTC()
	if execTime.IsZero() {
		execTime = startTime
	}

	doc := &visibilityExecutionDocument{
		ID:               visibilityDocID(base.NamespaceID, base.WorkflowID, base.RunID),
		NamespaceID:      base.NamespaceID,
		WorkflowID:       base.WorkflowID,
		RunID:            base.RunID,
		WorkflowTypeName: base.WorkflowTypeName,
		TaskQueue:        base.TaskQueue,
		StartTime:        startTime,
		ExecutionTime:    execTime,
		VisibilityTime:   startTime,
		Status:           int32(base.Status),
		RootWorkflowID:   base.RootWorkflowID,
		RootRunID:        base.RootRunID,
		Version:          base.TaskID,
	}

	if base.ParentWorkflowID != nil {
		doc.ParentWorkflowID = base.ParentWorkflowID
	}
	if base.ParentRunID != nil {
		doc.ParentRunID = base.ParentRunID
	}

	if base.Memo != nil {
		doc.Memo = append([]byte(nil), base.Memo.GetData()...)
		doc.MemoEncoding = base.Memo.GetEncodingType().String()
	}

	if base.SearchAttributes != nil {
		encoded, err := proto.Marshal(base.SearchAttributes)
		if err != nil {
			return nil, serviceerror.NewInternalf("failed to marshal search attributes: %v", err)
		}
		doc.SearchAttributes = encoded
		doc.SearchAttributesEnc = "proto"

		if saMap, err := s.renderSearchAttributes(ctx, base.SearchAttributes); err != nil {
			return nil, err
		} else if len(saMap) > 0 {
			doc.SearchAttributesMap = saMap
		}
	}

	return doc, nil
}

func (s *visibilityStore) renderSearchAttributes(ctx context.Context, sa *commonpb.SearchAttributes) (map[string]any, error) {
	if sa == nil || len(sa.GetIndexedFields()) == 0 {
		return nil, nil
	}

	typeMap, err := s.getSearchAttributes(ctx)
	if err != nil {
		return nil, err
	}

	result := make(map[string]any, len(sa.GetIndexedFields()))
	for name, payload := range sa.GetIndexedFields() {
		valueType := enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
		if vType, lookupErr := typeMap.GetType(name); lookupErr == nil {
			valueType = vType
		}

		decoded, err := searchattribute.DecodeValue(payload, valueType, true)
		if err != nil {
			return nil, serviceerror.NewInternalf("failed to decode search attribute %q: %v", name, err)
		}
		if decoded == nil {
			delete(result, name)
			continue
		}

		switch v := decoded.(type) {
		case time.Time:
			result[name] = v.UTC()
		case []string:
			values := make([]any, len(v))
			for i, item := range v {
				values[i] = item
			}
			result[name] = values
		default:
			result[name] = v
		}
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func (s *visibilityStore) stripTransientSearchAttributes(doc *visibilityExecutionDocument) (bool, error) {
	if doc == nil {
		return false, serviceerror.NewInternal("visibility document is nil")
	}

	if enumspb.WorkflowExecutionStatus(doc.Status) == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return false, nil
	}

	removed := false

	// Remove pause info from rendered map to prevent closed executions from matching pause queries.
	if len(doc.SearchAttributesMap) > 0 {
		if _, ok := doc.SearchAttributesMap[sadefs.TemporalPauseInfo]; ok {
			delete(doc.SearchAttributesMap, sadefs.TemporalPauseInfo)
			if len(doc.SearchAttributesMap) == 0 {
				doc.SearchAttributesMap = nil
			}
			removed = true
		}
	}

	if len(doc.SearchAttributes) == 0 {
		return removed, nil
	}

	var sa commonpb.SearchAttributes
	if err := proto.Unmarshal(doc.SearchAttributes, &sa); err != nil {
		return false, serviceerror.NewInternalf("failed to unmarshal search attributes: %v", err)
	}

	if sa.IndexedFields == nil {
		return removed, nil
	}

	if _, ok := sa.IndexedFields[sadefs.TemporalPauseInfo]; !ok {
		return removed, nil
	}

	delete(sa.IndexedFields, sadefs.TemporalPauseInfo)
	if len(sa.IndexedFields) == 0 {
		doc.SearchAttributes = nil
		doc.SearchAttributesEnc = ""
		return true, nil
	}

	encoded, err := proto.Marshal(&sa)
	if err != nil {
		return false, serviceerror.NewInternalf("failed to marshal search attributes: %v", err)
	}
	doc.SearchAttributes = encoded
	return true, nil
}

func versionMatchClauses(version int64) []bson.M {
	return []bson.M{
		{"version": bson.M{"$lt": version}},
		{"version": version},
		{"version": bson.M{"$exists": false}},
	}
}

func visibilityDocID(namespaceID, workflowID, runID string) string {
	return fmt.Sprintf("%s|%s|%s", namespaceID, workflowID, runID)
}

func mongoAnd(filters ...bson.M) bson.M {
	flattened := make([]bson.M, 0, len(filters))
	for _, f := range filters {
		if len(f) == 0 {
			continue
		}
		flattened = append(flattened, f)
	}
	if len(flattened) == 0 {
		return bson.M{}
	}
	if len(flattened) == 1 {
		return flattened[0]
	}
	return bson.M{"$and": flattened}
}

func visibilityDocumentUpdate(doc *visibilityExecutionDocument) (bson.M, error) {
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, serviceerror.NewInternalf("failed to marshal visibility document: %v", err)
	}

	var m bson.M
	if err := bson.Unmarshal(raw, &m); err != nil {
		return nil, serviceerror.NewInternalf("failed to unmarshal visibility document: %v", err)
	}
	delete(m, "_id")
	return m, nil
}

func documentToExecutionInfo(doc *visibilityExecutionDocument, saTypeMap searchattribute.NameTypeMap) (*store.InternalExecutionInfo, error) {
	if doc == nil {
		return nil, serviceerror.NewInternal("visibility document is nil")
	}

	info := &store.InternalExecutionInfo{
		WorkflowID:       doc.WorkflowID,
		RunID:            doc.RunID,
		TypeName:         doc.WorkflowTypeName,
		StartTime:        doc.StartTime,
		ExecutionTime:    doc.ExecutionTime,
		Status:           enumspb.WorkflowExecutionStatus(doc.Status),
		TaskQueue:        doc.TaskQueue,
		RootWorkflowID:   doc.RootWorkflowID,
		RootRunID:        doc.RootRunID,
		ParentWorkflowID: stringOrEmpty(doc.ParentWorkflowID),
		ParentRunID:      stringOrEmpty(doc.ParentRunID),
	}

	if doc.CloseTime != nil {
		info.CloseTime = *doc.CloseTime
	}
	if doc.ExecutionDuration != nil {
		info.ExecutionDuration = *doc.ExecutionDuration
	}
	if doc.HistoryLength != nil {
		info.HistoryLength = *doc.HistoryLength
	}
	if doc.HistorySizeBytes != nil {
		info.HistorySizeBytes = *doc.HistorySizeBytes
	}
	if doc.StateTransitionCount != nil {
		info.StateTransitionCount = *doc.StateTransitionCount
	}

	if len(doc.Memo) > 0 {
		info.Memo = persistence.NewDataBlob(append([]byte(nil), doc.Memo...), doc.MemoEncoding)
	}

	if len(doc.SearchAttributes) > 0 {
		var attrs commonpb.SearchAttributes
		if err := proto.Unmarshal(doc.SearchAttributes, &attrs); err != nil {
			return nil, serviceerror.NewInternalf("failed to unmarshal search attributes: %v", err)
		}
		// Apply type metadata to search attributes so they can be decoded without a typeMap.
		searchattribute.ApplyTypeMap(&attrs, saTypeMap)
		info.SearchAttributes = &attrs
	}

	return info, nil
}

func stringOrEmpty(ptr *string) string {
	if ptr == nil {
		return ""
	}
	return *ptr
}

func encodeVisibilityPageToken(token *visibilityPageToken) ([]byte, error) {
	if token == nil {
		return nil, nil
	}
	data, err := json.Marshal(token)
	if err != nil {
		return nil, serviceerror.NewInternalf("failed to marshal visibility page token: %v", err)
	}
	return data, nil
}

func decodeVisibilityPageToken(token []byte) (*visibilityPageToken, error) {
	if len(token) == 0 {
		return nil, nil
	}
	var decoded visibilityPageToken
	if err := json.Unmarshal(token, &decoded); err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid page token: %v", err))
	}
	return &decoded, nil
}
