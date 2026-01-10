package mongodb

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	storepkg "go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"
)

func TestVisibilityStoreConstructs(t *testing.T) {
	db := newFakeVisibilityDatabase(t)
	execCol := newFakeVisibilityCollection(t)
	saCol := newFakeVisibilityCollection(t)
	db.collections[collectionVisibilityExecutions] = execCol
	db.collections[collectionVisibilitySearchAttributes] = saCol

	vs, err := NewVisibilityStore(db, &fakeMongoClient{}, config.MongoDB{}, log.NewTestLogger(), metrics.NoopMetricsHandler, nil, nil, nil, true)
	require.NoError(t, err)
	require.NotNil(t, vs)
	require.Equal(t, "mongodb", vs.GetName())

	fakeCol := execCol.indexView
	require.NotNil(t, fakeCol)
	require.Len(t, fakeCol.created, 4)
}

func TestVisibilityStoreRecordWorkflowExecutionStarted(t *testing.T) {
	store := buildVisibilityStore(t)
	col := store.executionsCol.(*fakeVisibilityCollection)

	startTime := time.Now().UTC()
	execTime := startTime.Add(5 * time.Second)
	memo := persistence.NewDataBlob([]byte("memo"), enumspb.ENCODING_TYPE_JSON.String())
	payload, encodeErr := searchattribute.EncodeValue("value", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	require.NoError(t, encodeErr)
	sa := &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
		"CustomKeyword": payload,
	}}
	parentID := "parent"
	parentRun := "parentRun"

	req := &storepkg.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: &storepkg.InternalVisibilityRequestBase{
			NamespaceID:      "ns",
			WorkflowID:       "wf",
			RunID:            "run",
			WorkflowTypeName: "type",
			StartTime:        startTime,
			ExecutionTime:    execTime,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			TaskID:           42,
			TaskQueue:        "queue",
			Memo:             memo,
			SearchAttributes: sa,
			ParentWorkflowID: &parentID,
			ParentRunID:      &parentRun,
			RootWorkflowID:   "root",
			RootRunID:        "rootRun",
		},
	}

	err := store.RecordWorkflowExecutionStarted(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, col.insertDocs, 1)

	doc, ok := col.insertDocs[0].(*visibilityExecutionDocument)
	require.True(t, ok)
	require.Equal(t, "ns", doc.NamespaceID)
	require.Equal(t, "wf", doc.WorkflowID)
	require.Equal(t, "run", doc.RunID)
	require.Equal(t, startTime, doc.StartTime)
	require.Equal(t, execTime, doc.ExecutionTime)
	require.Equal(t, doc.VisibilityTime, doc.StartTime)
	require.Equal(t, int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING), doc.Status)
	require.Equal(t, int64(42), doc.Version)
	require.Equal(t, "queue", doc.TaskQueue)
	require.NotEmpty(t, doc.Memo)
	require.Equal(t, memo.GetEncodingType().String(), doc.MemoEncoding)
	require.NotEmpty(t, doc.SearchAttributes)
}

func TestVisibilityStoreRecordWorkflowExecutionClosed(t *testing.T) {
	store := buildVisibilityStore(t)
	col := store.executionsCol.(*fakeVisibilityCollection)

	closeTime := time.Now().UTC()
	req := &storepkg.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: &storepkg.InternalVisibilityRequestBase{
			NamespaceID:      "ns",
			WorkflowID:       "wf",
			RunID:            "run",
			WorkflowTypeName: "type",
			StartTime:        closeTime.Add(-10 * time.Minute),
			ExecutionTime:    closeTime.Add(-10 * time.Minute),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			TaskID:           99,
			TaskQueue:        "queue",
			RootWorkflowID:   "root",
			RootRunID:        "rootRun",
		},
		CloseTime:            closeTime,
		HistoryLength:        55,
		HistorySizeBytes:     1234,
		ExecutionDuration:    10 * time.Minute,
		StateTransitionCount: 12,
	}

	err := store.RecordWorkflowExecutionClosed(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, col.updateFilters, 1)
	require.Equal(t, visibilityDocID("ns", "wf", "run"), col.updateFilters[0].(bson.M)["_id"])

	setMap := col.extractSet(0)
	var stored visibilityExecutionDocument
	raw, err := bson.Marshal(setMap)
	require.NoError(t, err)
	require.NoError(t, bson.Unmarshal(raw, &stored))
	require.NotNil(t, stored.CloseTime)
	require.WithinDuration(t, closeTime, stored.CloseTime.UTC(), time.Millisecond)
	require.NotNil(t, stored.ExecutionDuration)
	require.Equal(t, int64(55), *stored.HistoryLength)
	require.Equal(t, int64(1234), *stored.HistorySizeBytes)
}

func TestVisibilityStoreListWorkflowExecutions(t *testing.T) {
	store := buildVisibilityStore(t)
	col := store.executionsCol.(*fakeVisibilityCollection)

	now := time.Now().UTC()
	doc1 := &visibilityExecutionDocument{
		ID:               visibilityDocID("ns", "wf1", "run1"),
		NamespaceID:      "ns",
		WorkflowID:       "wf1",
		RunID:            "run1",
		WorkflowTypeName: "type",
		StartTime:        now,
		ExecutionTime:    now,
		VisibilityTime:   now,
		Status:           int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
	}
	doc2 := &visibilityExecutionDocument{
		ID:               visibilityDocID("ns", "wf2", "run2"),
		NamespaceID:      "ns",
		WorkflowID:       "wf2",
		RunID:            "run2",
		WorkflowTypeName: "type",
		StartTime:        now.Add(-time.Minute),
		ExecutionTime:    now.Add(-time.Minute),
		VisibilityTime:   now.Add(-time.Minute),
		Status:           int32(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED),
	}
	col.enqueueFind(doc1, doc2)

	resp, err := store.ListWorkflowExecutions(context.Background(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespace.ID("ns"),
		Namespace:   namespace.Name("ns"),
		PageSize:    1,
	})
	require.NoError(t, err)
	require.Len(t, resp.Executions, 1)
	require.NotNil(t, resp.NextPageToken)

	token, err := decodeVisibilityPageToken(resp.NextPageToken)
	require.NoError(t, err)
	require.WithinDuration(t, doc1.VisibilityTime, token.VisibilityTime, time.Millisecond)
	require.Equal(t, doc1.RunID, token.RunID)
}

func TestVisibilityStoreListWorkflowExecutions_WithQuery(t *testing.T) {
	store := buildVisibilityStore(t)
	col := store.executionsCol.(*fakeVisibilityCollection)
	col.enqueueFind()

	_, err := store.ListWorkflowExecutions(context.Background(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespace.ID("ns"),
		Namespace:   namespace.Name("ns"),
		Query:       "WorkflowId = 'wf1'",
	})
	require.NoError(t, err)
	require.Len(t, col.findFilters, 1)

	filter := col.findFilters[0].(bson.M)
	andExpr, ok := filter["$and"].([]bson.M)
	require.True(t, ok)
	require.Len(t, andExpr, 2)
	require.Equal(t, "ns", andExpr[0]["namespace_id"])
	require.True(t, filterContains(andExpr[1], "workflow_id", "wf1"))
}

func TestVisibilityStoreListWorkflowExecutions_WithStatusQuery(t *testing.T) {
	store := buildVisibilityStore(t)
	col := store.executionsCol.(*fakeVisibilityCollection)
	col.enqueueFind()

	_, err := store.ListWorkflowExecutions(context.Background(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespace.ID("ns"),
		Namespace:   namespace.Name("ns"),
		Query:       "ExecutionStatus = 'Completed'",
	})
	require.NoError(t, err)
	filter := col.findFilters[0].(bson.M)
	andExpr := filter["$and"].([]bson.M)
	require.Len(t, andExpr, 2)
	statusClause := andExpr[1]
	require.True(t, filterContains(statusClause, "status", int32(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)))
}

func TestVisibilityStoreListChasmExecutions(t *testing.T) {
	registry := newFakeChasmRegistry()
	component := chasm.NewRegistrableComponent[*testChasmComponent](
		"dummy",
		chasm.WithSearchAttributes(chasm.NewSearchAttributeKeyword("status", chasm.SearchAttributeFieldKeyword01)),
	)
	const archetypeID = chasm.ArchetypeID(123)
	registry.components[archetypeID] = component

	store := buildVisibilityStoreWithChasm(t, registry)
	col := store.executionsCol.(*fakeVisibilityCollection)
	now := time.Now().UTC()
	col.enqueueFind(&visibilityExecutionDocument{
		NamespaceID:      "ns",
		WorkflowID:       "wf",
		RunID:            "run",
		WorkflowTypeName: "type",
		StartTime:        now,
		ExecutionTime:    now,
		VisibilityTime:   now,
		Status:           int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
	})

	resp, err := store.ListChasmExecutions(context.Background(), &manager.ListChasmExecutionsRequest{
		ArchetypeID: archetypeID,
		NamespaceID: namespace.ID("ns"),
		Namespace:   namespace.Name("ns"),
		PageSize:    1,
		Query:       "status = 'READY'",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Executions, 1)
	require.Len(t, col.findFilters, 1)

	filter := col.findFilters[0].(bson.M)
	fieldName, fieldErr := component.SearchAttributesMapper().Field("status")
	require.NoError(t, fieldErr)
	require.True(t, filterContains(filter, "search_attributes."+fieldName, "READY"))
}

func TestVisibilityStoreListChasmExecutionsUnknownArchetype(t *testing.T) {
	store := buildVisibilityStoreWithChasm(t, newFakeChasmRegistry())
	_, err := store.ListChasmExecutions(context.Background(), &manager.ListChasmExecutionsRequest{
		ArchetypeID: chasm.ArchetypeID(999),
		NamespaceID: namespace.ID("ns"),
		Namespace:   namespace.Name("ns"),
	})
	require.Error(t, err)
	_, isInvalid := err.(*serviceerror.InvalidArgument)
	require.True(t, isInvalid)

}

func TestVisibilityStoreCountChasmExecutions(t *testing.T) {
	registry := newFakeChasmRegistry()
	component := chasm.NewRegistrableComponent[*testChasmComponent](
		"dummy",
		chasm.WithSearchAttributes(chasm.NewSearchAttributeKeyword("status", chasm.SearchAttributeFieldKeyword01)),
	)
	const archetypeID = chasm.ArchetypeID(456)
	registry.components[archetypeID] = component

	store := buildVisibilityStoreWithChasm(t, registry)
	col := store.executionsCol.(*fakeVisibilityCollection)
	col.countResult = 9

	resp, err := store.CountChasmExecutions(context.Background(), &manager.CountChasmExecutionsRequest{
		ArchetypeID: archetypeID,
		NamespaceID: namespace.ID("ns"),
		Namespace:   namespace.Name("ns"),
		Query:       "status = 'READY'",
	})
	require.NoError(t, err)
	require.Equal(t, int64(9), resp.Count)
	require.Len(t, col.countFilters, 1)

	filter := col.countFilters[0].(bson.M)
	fieldName, fieldErr := component.SearchAttributesMapper().Field("status")
	require.NoError(t, fieldErr)
	require.True(t, filterContains(filter, "search_attributes."+fieldName, "READY"))
}

func TestVisibilityStoreGetWorkflowExecution(t *testing.T) {
	store := buildVisibilityStore(t)
	col := store.executionsCol.(*fakeVisibilityCollection)

	doc := &visibilityExecutionDocument{
		ID:               visibilityDocID("ns", "wf", "run"),
		NamespaceID:      "ns",
		WorkflowID:       "wf",
		RunID:            "run",
		WorkflowTypeName: "type",
		StartTime:        time.Now().UTC(),
		ExecutionTime:    time.Now().UTC(),
		VisibilityTime:   time.Now().UTC(),
		Status:           int32(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED),
		Memo:             []byte("memo"),
		MemoEncoding:     enumspb.ENCODING_TYPE_JSON.String(),
	}
	col.findOneDoc = doc

	resp, err := store.GetWorkflowExecution(context.Background(), &manager.GetWorkflowExecutionRequest{
		NamespaceID: namespace.ID("ns"),
		WorkflowID:  "wf",
		RunID:       "run",
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Execution)
	require.Equal(t, "wf", resp.Execution.WorkflowID)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, resp.Execution.Status)
}

func TestVisibilityStoreDeleteWorkflowExecution(t *testing.T) {
	store := buildVisibilityStore(t)
	col := store.executionsCol.(*fakeVisibilityCollection)

	err := store.DeleteWorkflowExecution(context.Background(), &manager.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: namespace.ID("ns"),
		WorkflowID:  "wf",
		RunID:       "run",
	})
	require.NoError(t, err)
	require.Len(t, col.deleteFilters, 1)
	filter := col.deleteFilters[0].(bson.M)
	require.Equal(t, visibilityDocID("ns", "wf", "run"), filter["_id"])
}

func TestVisibilityStoreAddSearchAttributes(t *testing.T) {
	store := buildVisibilityStore(t)
	saCol := store.searchAttributesCol.(*fakeVisibilityCollection)

	err := store.AddSearchAttributes(context.Background(), &manager.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomKeyword": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	require.NoError(t, err)
	require.Len(t, saCol.updateFilters, 1)

	filter := saCol.updateFilters[0].(bson.M)
	require.Equal(t, "CustomKeyword", filter["name"])

	update := saCol.updateDocs[0].(bson.M)
	set := update["$set"].(bson.M)
	require.Equal(t, int32(enumspb.INDEXED_VALUE_TYPE_KEYWORD), set["value_type"])
	require.NotEmpty(t, set["updated_at"])
}

func TestVisibilityStoreCountWorkflowExecutions_WithQuery(t *testing.T) {
	store := buildVisibilityStore(t)
	col := store.executionsCol.(*fakeVisibilityCollection)
	col.countResult = 10

	_, err := store.CountWorkflowExecutions(context.Background(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID("ns"),
		Namespace:   namespace.Name("ns"),
		Query:       "WorkflowId = 'wf1'",
	})
	require.NoError(t, err)
	require.Len(t, col.countFilters, 1)
	filter := col.countFilters[0].(bson.M)
	andExpr := filter["$and"].([]bson.M)
	require.Len(t, andExpr, 2)
	require.True(t, filterContains(andExpr[1], "workflow_id", "wf1"))
}

func TestVisibilityStoreCountWorkflowExecutions_GroupBy(t *testing.T) {
	store := buildVisibilityStore(t)
	col := store.executionsCol.(*fakeVisibilityCollection)

	col.enqueueAggregate(bson.M{"_id": int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING), "count": int64(5)})

	resp, err := store.CountWorkflowExecutions(context.Background(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID("ns"),
		Namespace:   namespace.Name("ns"),
		Query:       "GROUP BY ExecutionStatus",
	})
	require.NoError(t, err)
	require.Equal(t, int64(5), resp.Count)
	require.Len(t, resp.Groups, 1)
	require.Equal(t, int64(5), resp.Groups[0].Count)

	decoded, err := searchattribute.DecodeValue(resp.Groups[0].GroupValues[0], enumspb.INDEXED_VALUE_TYPE_KEYWORD, false)
	require.NoError(t, err)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(), decoded)

	require.Empty(t, col.countFilters)
	require.Len(t, col.aggregatePipelines, 1)
}

func buildVisibilityStore(t *testing.T) *visibilityStore {
	db := newFakeVisibilityDatabase(t)
	execCol := newFakeVisibilityCollection(t)
	saCol := newFakeVisibilityCollection(t)
	db.collections[collectionVisibilityExecutions] = execCol
	db.collections[collectionVisibilitySearchAttributes] = saCol

	saProvider := searchattribute.NewTestProvider()
	saMapperProvider := searchattribute.NewTestMapperProvider(nil)
	storeIface, err := NewVisibilityStore(db, &fakeMongoClient{}, config.MongoDB{}, log.NewTestLogger(), metrics.NoopMetricsHandler, saProvider, saMapperProvider, nil, true)
	require.NoError(t, err)
	return storeIface.(*visibilityStore)
}

func buildVisibilityStoreWithChasm(t *testing.T, registry *fakeChasmRegistry) *visibilityStore {
	db := newFakeVisibilityDatabase(t)
	execCol := newFakeVisibilityCollection(t)
	saCol := newFakeVisibilityCollection(t)
	db.collections[collectionVisibilityExecutions] = execCol
	db.collections[collectionVisibilitySearchAttributes] = saCol

	saProvider := searchattribute.NewTestProvider()
	saMapperProvider := searchattribute.NewTestMapperProvider(nil)
	storeIface, err := NewVisibilityStore(db, &fakeMongoClient{}, config.MongoDB{}, log.NewTestLogger(), metrics.NoopMetricsHandler, saProvider, saMapperProvider, registry, true)
	require.NoError(t, err)
	return storeIface.(*visibilityStore)
}

type fakeChasmRegistry struct {
	components map[chasm.ArchetypeID]*chasm.RegistrableComponent
}

func newFakeChasmRegistry() *fakeChasmRegistry {
	return &fakeChasmRegistry{
		components: make(map[chasm.ArchetypeID]*chasm.RegistrableComponent),
	}
}

func (r *fakeChasmRegistry) ComponentByID(id chasm.ArchetypeID) (*chasm.RegistrableComponent, bool) {
	if r == nil {
		return nil, false
	}
	comp, ok := r.components[id]
	return comp, ok
}

type testChasmComponent struct {
	chasm.UnimplementedComponent
}

func (testChasmComponent) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func filterContains(val interface{}, key string, expected interface{}) bool {
	switch typed := val.(type) {
	case bson.M:
		if candidate, ok := typed[key]; ok && valueMatches(candidate, expected) {
			return true
		}
		for _, child := range typed {
			if filterContains(child, key, expected) {
				return true
			}
		}
	case []bson.M:
		for _, child := range typed {
			if filterContains(child, key, expected) {
				return true
			}
		}
	case []interface{}:
		for _, child := range typed {
			if filterContains(child, key, expected) {
				return true
			}
		}
	}
	return false
}

func valueMatches(candidate interface{}, expected interface{}) bool {
	switch c := candidate.(type) {
	case bson.M:
		if eq, ok := c["$eq"]; ok {
			return reflect.DeepEqual(eq, expected)
		}
		if in, ok := c["$in"]; ok {
			if list, ok := in.([]interface{}); ok {
				for _, item := range list {
					if reflect.DeepEqual(item, expected) {
						return true
					}
				}
			}
		}
	default:
		return reflect.DeepEqual(candidate, expected)
	}
	return false
}

type fakeVisibilityDatabase struct {
	t           *testing.T
	collections map[string]client.Collection
}

func newFakeVisibilityDatabase(t *testing.T) *fakeVisibilityDatabase {
	return &fakeVisibilityDatabase{
		t:           t,
		collections: make(map[string]client.Collection),
	}
}

func (d *fakeVisibilityDatabase) Collection(name string) client.Collection {
	col, ok := d.collections[name]
	if !ok {
		require.FailNow(d.t, "unexpected collection", name)
	}
	return col
}

func (d *fakeVisibilityDatabase) RunCommand(context.Context, interface{}) client.SingleResult {
	return fakeSingleResult{}
}

type fakeVisibilityCollection struct {
	t *testing.T

	insertDocs    []interface{}
	updateFilters []interface{}
	updateDocs    []interface{}
	updateOpts    []*options.UpdateOptions
	deleteFilters []interface{}
	findFilters   []interface{}
	findOpts      []*options.FindOptions

	findBatches [][]interface{}
	findOneDoc  interface{}
	findOneErr  error

	countResult  int64
	countFilters []interface{}

	aggregatePipelines []interface{}
	aggregateResults   [][]interface{}

	indexView *fakeIndexView
}

func newFakeVisibilityCollection(t *testing.T) *fakeVisibilityCollection {
	return &fakeVisibilityCollection{t: t}
}

func (c *fakeVisibilityCollection) enqueueFind(docs ...*visibilityExecutionDocument) {
	batch := make([]interface{}, len(docs))
	for i, doc := range docs {
		batch[i] = doc
	}
	c.findBatches = append(c.findBatches, batch)
}

func (c *fakeVisibilityCollection) enqueueAggregate(results ...interface{}) {
	c.aggregateResults = append(c.aggregateResults, results)
}

func (c *fakeVisibilityCollection) extractSet(idx int) bson.M {
	update := c.updateDocs[idx].(bson.M)
	set, _ := update["$set"].(bson.M)
	return set
}

func (c *fakeVisibilityCollection) FindOne(context.Context, interface{}, ...*options.FindOneOptions) client.SingleResult {
	return fakeSingleResult{doc: c.findOneDoc, err: c.findOneErr}
}

func (c *fakeVisibilityCollection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (client.Cursor, error) {
	c.findFilters = append(c.findFilters, filter)
	if len(opts) > 0 {
		c.findOpts = append(c.findOpts, opts[0])
	} else {
		c.findOpts = append(c.findOpts, nil)
	}
	if len(c.findBatches) == 0 {
		return &fakeVisibilityCursor{}, nil
	}
	batch := c.findBatches[0]
	c.findBatches = c.findBatches[1:]
	return &fakeVisibilityCursor{items: batch}, nil
}

func (c *fakeVisibilityCollection) InsertOne(_ context.Context, document interface{}, _ ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	c.insertDocs = append(c.insertDocs, document)
	return &mongo.InsertOneResult{}, nil
}

func (c *fakeVisibilityCollection) InsertMany(context.Context, []interface{}, ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	return &mongo.InsertManyResult{}, nil
}

func (c *fakeVisibilityCollection) UpdateOne(_ context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	c.updateFilters = append(c.updateFilters, filter)
	c.updateDocs = append(c.updateDocs, update)
	if len(opts) > 0 {
		c.updateOpts = append(c.updateOpts, opts[0])
	} else {
		c.updateOpts = append(c.updateOpts, nil)
	}
	return &mongo.UpdateResult{}, nil
}

func (c *fakeVisibilityCollection) UpdateMany(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	return &mongo.UpdateResult{}, nil
}

func (c *fakeVisibilityCollection) DeleteOne(_ context.Context, filter interface{}, _ ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	c.deleteFilters = append(c.deleteFilters, filter)
	return &mongo.DeleteResult{}, nil
}

func (c *fakeVisibilityCollection) DeleteMany(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	return &mongo.DeleteResult{}, nil
}

func (c *fakeVisibilityCollection) CountDocuments(_ context.Context, filter interface{}, _ ...*options.CountOptions) (int64, error) {
	c.countFilters = append(c.countFilters, filter)
	return c.countResult, nil
}

func (c *fakeVisibilityCollection) Aggregate(_ context.Context, pipeline interface{}, _ ...*options.AggregateOptions) (client.Cursor, error) {
	c.aggregatePipelines = append(c.aggregatePipelines, pipeline)
	var items []interface{}
	if len(c.aggregateResults) > 0 {
		items = c.aggregateResults[0]
		c.aggregateResults = c.aggregateResults[1:]
	}
	return &fakeVisibilityCursor{items: items}, nil
}

func (c *fakeVisibilityCollection) Indexes() client.IndexView {
	if c.indexView == nil {
		c.indexView = &fakeIndexView{t: c.t}
	}
	return c.indexView
}

type fakeIndexView struct {
	t       *testing.T
	created []mongo.IndexModel
}

func (v *fakeIndexView) List(context.Context, ...*options.ListIndexesOptions) (client.Cursor, error) {
	return nil, nil
}

func (v *fakeIndexView) CreateOne(_ context.Context, model mongo.IndexModel, _ ...*options.CreateIndexesOptions) (string, error) {
	v.created = append(v.created, model)
	if model.Options != nil && model.Options.Name != nil {
		return *model.Options.Name, nil
	}
	return "", nil
}

func (v *fakeIndexView) DropOne(context.Context, string, ...*options.DropIndexesOptions) error {
	return nil
}

type fakeVisibilityCursor struct {
	items []interface{}
	idx   int
}

func (c *fakeVisibilityCursor) Next(context.Context) bool {
	if c.idx >= len(c.items) {
		return false
	}
	c.idx++
	return true
}

func (c *fakeVisibilityCursor) Decode(v interface{}) error {
	if c.idx == 0 || c.idx > len(c.items) {
		return mongo.ErrNoDocuments
	}
	current := c.items[c.idx-1]
	raw, err := bson.Marshal(current)
	if err != nil {
		return err
	}
	return bson.Unmarshal(raw, v)
}

func (c *fakeVisibilityCursor) All(_ context.Context, results interface{}) error {
	rv := reflect.ValueOf(results)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Slice {
		return errors.New("results must be pointer to slice")
	}
	slice := rv.Elem()
	sliceType := slice.Type().Elem()
	slice = slice.Slice(0, 0)

	for _, item := range c.items {
		raw, err := bson.Marshal(item)
		if err != nil {
			return err
		}
		newElem := reflect.New(sliceType)
		if err := bson.Unmarshal(raw, newElem.Interface()); err != nil {
			return err
		}
		slice = reflect.Append(slice, newElem.Elem())
	}

	rv.Elem().Set(slice)
	return nil
}

func (c *fakeVisibilityCursor) Close(context.Context) error {
	return nil
}

func (c *fakeVisibilityCursor) Err() error {
	return nil
}
