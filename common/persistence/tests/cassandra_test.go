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

package tests

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/common/persistence/serialization"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
)

type (
	// failingSession is a [gocql.Session] which fails any query whose template matches a string in failingQueries.
	failingSession struct {
		gocql.Session
		failingQueries []string
	}
	// failingQuery is a [gocql.Query] which fails when executed.
	failingQuery struct {
		gocql.Query
	}
	// recordingSession is a [gocql.Session] which records all queries executed on it.
	recordingSession struct {
		gocql.Session
		statements []statement
	}
	statement struct {
		query string
		args  []interface{}
	}
	// failingIter is a [gocql.Iter] which fails when iterated.
	failingIter struct{}
	// blockingSession is a [gocql.Session] designed for testing concurrent inserts.
	// See cassandra.ErrQueueMessageIDConflict for more.
	blockingSession struct {
		gocql.Session
		queryToBlockOn   string
		queryStarted     chan struct{}
		queryCanContinue chan struct{}
	}
	// enqueueMessageResult contains the result of a call to persistence.QueueV2.EnqueueMessage.
	enqueueMessageResult struct {
		// id of the inserted message
		id int
		// err if the call failed
		err error
	}
	testQueueParams struct {
		logger log.Logger
	}
	testLogger struct {
		log.Logger
		warningMsgs []string
	}
	rangeDeleteTestQueue struct {
		persistence.QueueV2
		session       *blockingSession
		deleteErrs    chan error
		maxIDToDelete int
	}
)

func (f failingIter) Scan(...interface{}) bool {
	return false
}

func (f failingIter) MapScan(map[string]interface{}) bool {
	return false
}

func (f failingIter) PageState() []byte {
	return nil
}

func (f failingIter) Close() error {
	return assert.AnError
}

func (q failingQuery) Iter() gocql.Iter {
	return failingIter{}
}

func (q failingQuery) Scan(...interface{}) error {
	return assert.AnError
}

func (q failingQuery) Exec() error {
	return assert.AnError
}

func (l *testLogger) Warn(msg string, _ ...tag.Tag) {
	l.warningMsgs = append(l.warningMsgs, msg)
}

func (s *recordingSession) Query(query string, args ...interface{}) gocql.Query {
	s.statements = append(s.statements, statement{
		query: query,
		args:  args,
	})

	return s.Session.Query(query, args...)
}

func TestCassandraShardStoreSuite(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewShardSuite(
		t,
		shardStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestCassandraExecutionMutableStateStoreSuite(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewExecutionMutableStateSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		&persistence.HistoryBranchUtilImpl{},
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestCassandraExecutionMutableStateTaskStoreSuite(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewExecutionMutableStateTaskSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

// TODO: Merge persistence-tests into the tests directory.

func TestCassandraHistoryStoreSuite(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewHistoryEventsSuite(t, store, testData.Logger)
	suite.Run(t, s)
}

func TestCassandraTaskQueueSuite(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewTaskQueueSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestCassandraTaskQueueTaskSuite(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewTaskQueueTaskSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestCassandraHistoryV2Persistence(t *testing.T) {
	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestCassandraMetadataPersistenceV2(t *testing.T) {
	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestCassandraClusterMetadataPersistence(t *testing.T) {
	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestCassandraQueuePersistence(t *testing.T) {
	s := new(persistencetests.QueuePersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestCassandraQueueV2Persistence(t *testing.T) {
	// This test function is split up into two parts:
	// 1. Test the generic queue functionality, which is independent of the database choice (Cassandra here).
	//   This is done by calling the generic RunQueueV2TestSuite function.
	// 2. Test the Cassandra-specific implementation of the queue. For example, things like queue message ID conflicts
	//   can only happen in Cassandra due to its lack of transactions, so we need to test those here.

	t.Parallel()

	cluster := persistencetests.NewTestClusterForCassandra(&persistencetests.TestBaseOptions{}, log.NewNoopLogger())
	cluster.SetupTestDatabase()
	t.Cleanup(cluster.TearDownTestDatabase)

	t.Run("Generic", func(t *testing.T) {
		t.Parallel()
		RunQueueV2TestSuite(t, newQueueV2Store(cluster.GetSession()))
	})
	t.Run("CassandraSpecific", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2(t, cluster)
	})
}

func TestCassandraNexusEndpointPersistence(t *testing.T) {
	cluster := persistencetests.NewTestClusterForCassandra(&persistencetests.TestBaseOptions{}, log.NewNoopLogger())
	cluster.SetupTestDatabase()
	t.Cleanup(cluster.TearDownTestDatabase)

	tableVersion := atomic.Int64{}

	// NB: These tests cannot be run in parallel because of concurrent updates to the table version by different tests
	t.Run("Generic", func(t *testing.T) {
		RunNexusEndpointTestSuite(t, newNexusEndpointStore(cluster.GetSession()), &tableVersion)
	})
	t.Run("CassandraSpecific", func(t *testing.T) {
		testCassandraNexusEndpointStore(t, cluster, &tableVersion)
	})
}

func testCassandraQueueV2DataCorruption(t *testing.T, cluster *cassandra.TestCluster) {
	t.Run("ErrInvalidQueueMessageEncodingType", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrInvalidQueueMessageEncodingType(t, cluster)
	})
	t.Run("ErrInvalidPayloadEncodingType", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrInvalidPayloadEncodingType(t, cluster)
	})
	t.Run("ErrInvalidPayload", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrInvalidPayload(t, cluster)
	})
}

func testCassandraQueueV2(t *testing.T, cluster *cassandra.TestCluster) {
	t.Run("DataCorruption", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2DataCorruption(t, cluster)
	})
	t.Run("QueryErrors", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2QueryErrors(t, cluster)
	})
	t.Run("RangeDeleteUpperBoundHigherThanMaxMessageID", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2RangeDeleteUpperBoundHigherThanMaxMessageID(t, cluster)
	})
	t.Run("RepeatedRangeDelete", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2RepeatedRangeDelete(t, cluster)
	})
	t.Run("MinMessageIDOptimization", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2MinMessageIDOptimization(t, cluster)
	})
	t.Run("ConcurrentConflicts", func(t *testing.T) {
		testCassandraQueueV2ConcurrentConflicts(t, cluster)
	})
	t.Run("MultiplePartitions", func(t *testing.T) {
		testCassandraQueueV2MultiplePartitions(t, cluster)
	})
}

func testCassandraQueueV2ConcurrentConflicts(t *testing.T, cluster *cassandra.TestCluster) {
	t.Run("EnqueueMessage", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2EnqueueErrEnqueueMessageConflict(t, cluster)
	})
	t.Run("RangeDeleteMessages", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ConcurrentRangeDeleteMessages(t, cluster)
	})
}

func testCassandraQueueV2QueryErrors(t *testing.T, cluster *cassandra.TestCluster) {
	t.Run("GetQueueQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrGetQueueQuery(t, cluster)
	})
	t.Run("CreateQueueQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrCreateQueueQuery(t, cluster)
	})
	t.Run("RangeDeleteMessagesQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrRangeDeleteMessagesQuery(t, cluster)
	})
	t.Run("ErrReadMessagesQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrGetMessagesQuery(t, cluster)
	})
	t.Run("ErrEnqueueMessageQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrEnqueueMessageQuery(t, cluster)
	})
	t.Run("EnqueueMessageGetMaxMessageIDQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrEnqueueMessageGetMaxMessageIDQuery(t, cluster)
	})
	t.Run("ListQueuesGetMaxMessageIDQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrListQueuesGetMaxMessageIDQuery(t, cluster)
	})
	t.Run("RangeDeleteMessagesGetMaxMessageIDQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrRangeDeleteMessagesGetMaxMessageIDQuery(t, cluster)
	})
	t.Run("RangeDeleteMessagesUpdateQueueQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrRangeDeleteMessagesUpdateQueueQuery(t, cluster)
	})
}

func testCassandraQueueV2ErrRangeDeleteMessagesUpdateQueueQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateUpdateQueueMetadataQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	persistencetest.EnqueueMessagesForDelete(t, q, queueName, queueType)
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID)
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2UpdateQueueMetadata")
}

func testCassandraQueueV2ErrRangeDeleteMessagesGetMaxMessageIDQuery(t *testing.T, cluster *cassandra.TestCluster) {
	session := &failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{},
	}
	q := newQueueV2Store(session)
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	persistencetest.EnqueueMessagesForDelete(t, q, queueName, queueType)
	session.failingQueries = []string{cassandra.TemplateGetMaxMessageIDQuery}
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID)
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2GetMaxMessageID")
}

func testCassandraQueueV2ErrEnqueueMessageGetMaxMessageIDQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateGetMaxMessageIDQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = persistencetest.EnqueueMessage(ctx, q, queueType, queueName)
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2GetMaxMessageID")
}

func testCassandraQueueV2ErrListQueuesGetMaxMessageIDQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateGetMaxMessageIDQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryDLQ
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = q.ListQueues(ctx, &persistence.InternalListQueuesRequest{
		QueueType: queueType,
		PageSize:  100,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2GetMaxMessageID")
}

func testCassandraQueueV2MultiplePartitions(t *testing.T, cluster *cassandra.TestCluster) {
	t.Run("RangeDeleteMessages", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2MultiplePartitionsRangeDelete(t, cluster)
	})
	t.Run("ReadMessages", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2MultiplePartitionsReadMessages(t, cluster)
	})
	t.Run("ListQueues", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2MultiplePartitionsListQueues(t, cluster)
	})
}

// Query checks if the query matches queryToBlockOn, and, if so, it notifies the test and then blocks until the test
// unblocks it.
func (f *blockingSession) Query(query string, args ...interface{}) gocql.Query {
	if query == f.queryToBlockOn {
		f.queryStarted <- struct{}{}
		<-f.queryCanContinue
	}

	return f.Session.Query(query, args...)
}

// testCassandraQueueV2EnqueueErrEnqueueMessageConflict tests that when there are concurrent inserts to the queue, only one of
// them is accepted if they try to enqueue a message with the same ID, and the other clients are given the correct
// error.
func testCassandraQueueV2EnqueueErrEnqueueMessageConflict(t *testing.T, cluster *cassandra.TestCluster) {
	const numConcurrentWrites = 3

	session := &blockingSession{
		Session:          cluster.GetSession(),
		queryToBlockOn:   cassandra.TemplateEnqueueMessageQuery,
		queryStarted:     make(chan struct{}, numConcurrentWrites),
		queryCanContinue: make(chan struct{}),
	}

	q := newQueueV2Store(session)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	t.Cleanup(cancel)

	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()

	results := make(chan enqueueMessageResult, numConcurrentWrites)

	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	for i := 0; i < numConcurrentWrites; i++ {
		go func() {
			res, err := persistencetest.EnqueueMessage(ctx, q, queueType, queueName)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case results <- enqueueMessageResult{err: err}:
				}
			} else {
				select {
				case <-ctx.Done():
					return
				case results <- enqueueMessageResult{id: int(res.Metadata.ID)}:
				}
			}
		}()
	}

	for i := 0; i < numConcurrentWrites; i++ {
		select {
		case <-ctx.Done():
			printResults(t, results)
			t.Fatal("timed out waiting for enqueue to be called")
		case <-session.queryStarted:
		}
	}
	close(session.queryCanContinue)

	numConflicts := 0
	writtenMessageIDs := make([]int, 0, 1)

	for i := 0; i < numConcurrentWrites; i++ {
		var res enqueueMessageResult
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for enqueue to return")
		case res = <-results:
		}
		if res.err != nil {
			assert.ErrorIs(t, res.err, cassandra.ErrEnqueueMessageConflict)

			numConflicts++
		} else {
			writtenMessageIDs = append(writtenMessageIDs, res.id)
		}
	}

	assert.Equal(t, numConcurrentWrites-1, numConflicts,
		"every query other than the accepted one should have failed")
	assert.Len(t, writtenMessageIDs, 1,
		"only one message should have been written")

	messages, err := q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType:     queueType,
		QueueName:     queueName,
		PageSize:      numConcurrentWrites,
		NextPageToken: nil,
	})

	require.NoError(t, err)
	require.Len(t, messages.Messages, 1,
		"there should only be one message in the queue")
	assert.Equal(t, writtenMessageIDs[0], int(messages.Messages[0].MetaData.ID),
		"the message in the queue should be the one that Cassandra told us was accepted")
}

func printResults(t *testing.T, results chan enqueueMessageResult) {
	for {
		select {
		case res := <-results:
			if res.err != nil {
				t.Error("got unexpected error:", res.err)
			}
		default:
			return
		}
	}
}

func testCassandraQueueV2ErrInvalidQueueMessageEncodingType(t *testing.T, cluster *cassandra.TestCluster) {
	session := cluster.GetSession()
	q := newQueueV2Store(session)
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(context.Background(), &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	err = session.Query(
		cassandra.TemplateEnqueueMessageQuery,
		queueType,
		queueName,
		0, // partition
		1, // messageID
		[]byte("test"),
		"bad-encoding-type",
	).Exec()
	require.NoError(t, err)
	_, err = q.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serialization.UnknownEncodingTypeError))
}

func (q failingQuery) MapScanCAS(map[string]interface{}) (bool, error) {
	return false, assert.AnError
}

func (q failingQuery) WithContext(context.Context) gocql.Query {
	return q
}

func (f failingSession) Query(query string, args ...interface{}) gocql.Query {
	for _, q := range f.failingQueries {
		if q == query {
			return failingQuery{}
		}
	}
	return f.Session.Query(query, args...)
}

func testCassandraQueueV2ErrGetMessagesQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateGetMessagesQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2ReadMessages")
}

func testCassandraQueueV2ErrEnqueueMessageQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateEnqueueMessageQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = persistencetest.EnqueueMessage(context.Background(), q, queueType, queueName)
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2EnqueueMessage")
}

func testCassandraQueueV2ErrInvalidPayloadEncodingType(t *testing.T, cluster *cassandra.TestCluster) {
	// Manually insert a row into the queues table that has an invalid metadata payload encoding type and then verify
	// that we gracefully handle the error when we try to read from this queue.

	session := cluster.GetSession()
	q := newQueueV2Store(session)
	// Using a different QueueType so that ListQueue tests are not failing because of corrupt queue metadata.
	queueType := persistence.QueueV2Type(3)
	queueName := "test-queue-" + t.Name()
	err := session.Query(
		cassandra.TemplateCreateQueueQuery,
		queueType,
		queueName,
		[]byte("test"),      // payload
		"bad-encoding-type", // payload encoding type
		0,                   // version
	).Exec()
	require.NoError(t, err)
	_, err = q.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serialization.UnknownEncodingTypeError))
	assert.ErrorContains(t, err, "bad-encoding-type")
	assert.ErrorContains(t, err, strconv.Itoa(int(queueType)))
	assert.ErrorContains(t, err, queueName)

	_, err = q.ListQueues(context.Background(), &persistence.InternalListQueuesRequest{
		QueueType: queueType,
		PageSize:  100,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serialization.UnknownEncodingTypeError))
	assert.ErrorContains(t, err, "bad-encoding-type")
	assert.ErrorContains(t, err, strconv.Itoa(int(queueType)))
	assert.ErrorContains(t, err, queueName)
}

func testCassandraQueueV2ErrInvalidPayload(t *testing.T, cluster *cassandra.TestCluster) {
	// Manually insert a row into the queues table that has some invalid bytes for the queue metadata proto payload and
	// then verify that we gracefully handle the error when we try to read from this queue.

	session := cluster.GetSession()
	q := newQueueV2Store(session)
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	err := session.Query(
		cassandra.TemplateCreateQueueQuery,
		queueType,
		queueName,
		[]byte("invalid-payload"),           // payload
		enums.ENCODING_TYPE_PROTO3.String(), // payload encoding type
		0,                                   // version
	).Exec()
	require.NoError(t, err)
	_, err = q.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serialization.DeserializationError))
	assert.ErrorContains(t, err, "unmarshal")
	assert.ErrorContains(t, err, strconv.Itoa(int(queueType)))
	assert.ErrorContains(t, err, queueName)
}

func testCassandraQueueV2ErrGetQueueQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateGetQueueQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2GetQueue")
}

func testCassandraQueueV2ErrCreateQueueQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateCreateQueueQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2CreateQueue")
}

func testCassandraQueueV2ErrRangeDeleteMessagesQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(failingSession{
		Session:        cluster.GetSession(),
		failingQueries: []string{cassandra.TemplateRangeDeleteMessagesQuery},
	})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	persistencetest.EnqueueMessagesForDelete(t, q, queueName, queueType)
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID)
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2RangeDeleteMessages")
}

func testCassandraQueueV2MinMessageIDOptimization(t *testing.T, cluster *cassandra.TestCluster) {
	session := &recordingSession{
		Session: cluster.GetSession(),
	}
	q := newQueueV2Store(session)
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		_, err = persistencetest.EnqueueMessage(context.Background(), q, queueType, queueName)
		require.NoError(t, err)
	}
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID)
	require.NoError(t, err)
	pageSize := 10
	response, err := q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  pageSize,
	})
	require.NoError(t, err)
	require.Len(t, response.Messages, 1)
	assert.Equal(t, int64(persistence.FirstQueueMessageID+1), response.Messages[0].MetaData.ID)
	var lastReadStmt *statement
	for _, stmt := range session.statements {
		if stmt.query == cassandra.TemplateGetMessagesQuery {
			stmt := stmt
			lastReadStmt = &stmt
		}
	}
	require.NotNil(t, lastReadStmt, "expected to find a query to get messages")
	args := lastReadStmt.args
	require.Len(t, args, 5)
	assert.Equal(t, queueType, args[0])
	assert.Equal(t, queueName, args[1])
	assert.Equal(t, 0, args[2])
	assert.Equal(t, persistence.FirstQueueMessageID+1, args[3], "We should skip the first "+
		"message ID because we deleted it")
	assert.Equal(t, pageSize, args[4])
}

func testCassandraQueueV2ConcurrentRangeDeleteMessages(t *testing.T, cluster *cassandra.TestCluster) {
	// This test simulates a race condition between two RangeDeleteMessages calls. First, we enqueue 3 messages, then we
	// start a request to delete the first message, and then we start another request to delete the first two messages.
	// We have two cases, one where the first request to delete the first message is the leader and one where the second
	// request is the leader. Both requests rendezvous at the query to update queue metadata, but the leader query goes
	// first, and the follower query goes only after the leader query has completely finished. In the first case, the
	// first request should succeed and the second request should fail with a conflict error. In the second case, the
	// second request should succeed and the first request should fail with a conflict error. In both cases, only the
	// third message should be in the queue. More importantly, after we retry the failing request, it should succeed,
	// and the queue metadata should now record the min_message_id as 2.

	for _, tc := range []struct {
		name                  string
		smallerDeleteIsLeader bool
	}{
		{
			name:                  "smaller delete goes first",
			smallerDeleteIsLeader: true,
		},
		{
			name:                  "larger delete goes first",
			smallerDeleteIsLeader: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Make two queue stores to simulate the leader and follower queries. We use two separate queue stores
			// because it makes it easier to control which query goes first when they have separate blockingSession
			// instances.
			qs := make([]rangeDeleteTestQueue, 2)
			for i, q := range qs {
				// We need to use a blocking session here because we need to block the query to update the queue
				// metadata.
				q.session = &blockingSession{
					Session:          cluster.GetSession(),
					queryToBlockOn:   cassandra.TemplateUpdateQueueMetadataQuery,
					queryStarted:     make(chan struct{}, 1),
					queryCanContinue: make(chan struct{}, 1),
				}
				q.QueueV2 = newQueueV2Store(q.session)
				q.deleteErrs = make(chan error, 1)
				q.maxIDToDelete = persistence.FirstQueueMessageID + i
				qs[i] = q
			}

			// Create the queue
			ctx := context.Background()
			queueType := persistence.QueueTypeHistoryNormal
			queueName := "test-queue-" + t.Name()
			_, err := qs[0].CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
				QueueType: queueType,
				QueueName: queueName,
			})
			require.NoError(t, err)

			// Enqueue 3 messages
			for i := 0; i < 3; i++ {
				_, err := persistencetest.EnqueueMessage(ctx, qs[0].QueueV2, queueType, queueName)
				require.NoError(t, err)
			}

			// Start both RangeDeleteMessages call
			for _, q := range qs {
				q := q
				go func() {
					err := deleteMessages(ctx, q.QueueV2, queueType, queueName, q.maxIDToDelete)
					q.deleteErrs <- err
				}()
			}

			// Wait for both queries to start
			for i := 0; i < 2; i++ {
				<-qs[i].session.queryStarted
			}

			// Choose which query will be the leader and which will be the follower
			var leader, follower *rangeDeleteTestQueue
			if tc.smallerDeleteIsLeader {
				leader = &qs[0]
				follower = &qs[1]
			} else {
				leader = &qs[1]
				follower = &qs[0]
			}

			// Let the leader query finish
			close(leader.session.queryCanContinue)
			err = <-leader.deleteErrs
			require.NoError(t, err)

			// Let the follower query finish
			close(follower.session.queryCanContinue)
			err = <-follower.deleteErrs

			// Verify that the follower query failed
			require.Error(t, err)
			assert.ErrorIs(t, err, cassandra.ErrUpdateQueueConflict)
			assert.ErrorContains(t, err, strconv.Itoa(int(queueType)))
			assert.ErrorContains(t, err, queueName)

			// Verify that the queue metadata was updated by the leader query
			q, err := cassandra.GetQueue(ctx, qs[0].session, queueName, queueType)
			require.NoError(t, err)
			require.Len(t, q.Metadata.Partitions, 1)
			if tc.smallerDeleteIsLeader {
				// The smaller delete should have updated the min message ID to 1 because it succeeded, and the follower
				// query received a conflict. However, the follower query will get retried, so the min message ID will
				// eventually be updated correctly.
				assert.Equal(t, int64(persistence.FirstQueueMessageID+1), q.Metadata.Partitions[0].MinMessageId)
			} else {
				// The bigger delete should have updated the min message ID to 2 because it succeeded, so the min
				// message ID is already correct. However, the follower query will still get retried, so we need to
				// verify later that this retry does not change the min message ID to 1.
				assert.Equal(t, int64(persistence.FirstQueueMessageID+2), q.Metadata.Partitions[0].MinMessageId)
			}

			// Retry the follower query. Note that this would fail if it actually tried to update the queue metadata
			// since we haven't unblocked it, so this implicitly tests that both operations are idempotent.
			err = deleteMessages(ctx, follower.QueueV2, queueType, queueName, follower.maxIDToDelete)
			require.NoError(t, err)

			// Verify that the queue metadata was updated to reflect the new min message ID
			q, err = cassandra.GetQueue(ctx, qs[0].session, queueName, queueType)
			require.NoError(t, err)
			require.Len(t, q.Metadata.Partitions, 1)
			assert.Equal(t, int64(persistence.FirstQueueMessageID+2), q.Metadata.Partitions[0].MinMessageId)

			// Verify that the first two messages were deleted no matter which query was the leader
			response, err := qs[0].ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
				QueueType: queueType,
				QueueName: queueName,
				PageSize:  10,
			})
			require.NoError(t, err)
			require.Len(t, response.Messages, 1)
			assert.Equal(t, int64(persistence.FirstQueueMessageID+2), response.Messages[0].MetaData.ID)
		})
	}
}

func testCassandraQueueV2MultiplePartitionsRangeDelete(t *testing.T, cluster *cassandra.TestCluster) {
	// Manually insert a row into the queues table that has multiple partitions and then verify that we gracefully
	// handle the error when we try to range delete messages from this queue.

	session := cluster.GetSession()
	logger := &testLogger{}
	q := newQueueV2Store(session, func(params *testQueueParams) {
		params.logger = logger
	})
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	insertQueueMetadataWithMultiplePartitions(t, session, queueType, queueName)
	err := deleteMessages(context.Background(), q, queueType, queueName, 1)
	require.Error(t, err)
	assert.ErrorContains(t, err, "partitions")
}

func testCassandraQueueV2MultiplePartitionsReadMessages(t *testing.T, cluster *cassandra.TestCluster) {
	// Manually insert a row into the queues table that has multiple partitions and then verify that we gracefully
	// handle the error when we try to read messages from this queue.

	session := cluster.GetSession()
	logger := &testLogger{}
	q := newQueueV2Store(session, func(params *testQueueParams) {
		params.logger = logger
	})
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	insertQueueMetadataWithMultiplePartitions(t, session, queueType, queueName)
	_, err := q.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "partitions")
}

func testCassandraQueueV2MultiplePartitionsListQueues(t *testing.T, cluster *cassandra.TestCluster) {
	// Manually insert a row into the queues table that has multiple partitions and then verify that we gracefully
	// handle the error when we try to list queues.

	session := cluster.GetSession()
	logger := &testLogger{}
	q := newQueueV2Store(session, func(params *testQueueParams) {
		params.logger = logger
	})
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	insertQueueMetadataWithMultiplePartitions(t, session, queueType, queueName)
	_, err := q.ListQueues(context.Background(), &persistence.InternalListQueuesRequest{
		QueueType: queueType,
		PageSize:  100,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "partitions")
}

func testCassandraQueueV2RangeDeleteUpperBoundHigherThanMaxMessageID(t *testing.T, cluster *cassandra.TestCluster) {
	q := newQueueV2Store(cluster.GetSession())
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = persistencetest.EnqueueMessage(ctx, q, queueType, queueName)
	require.NoError(t, err)
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID+2)
	require.NoError(t, err)
	res, err := persistencetest.EnqueueMessage(ctx, q, queueType, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(persistence.FirstQueueMessageID+1), res.Metadata.ID)
}

func testCassandraQueueV2RepeatedRangeDelete(t *testing.T, cluster *cassandra.TestCluster) {
	// We never delete the last message from the queue. However, on a subsequent delete, the previous message with the
	// min_message_id of the queue should be deleted. This test verifies that.

	q := newQueueV2Store(cluster.GetSession())
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	numMessages := 3
	for i := 0; i < numMessages; i++ {
		_, err := persistencetest.EnqueueMessage(ctx, q, queueType, queueName)
		require.NoError(t, err)
	}
	numRemainingMessages := getNumMessages(t, cluster, queueType, queueName, numMessages)
	assert.Equal(t, 3, numRemainingMessages)
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID)
	require.NoError(t, err)
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID+1)
	require.NoError(t, err)
	err = deleteMessages(ctx, q, queueType, queueName, persistence.FirstQueueMessageID+2)
	require.NoError(t, err)
	numRemainingMessages = getNumMessages(t, cluster, queueType, queueName, numMessages)
	assert.Equal(t, 1, numRemainingMessages, "expected only one message to remain in the queue"+
		" because we never delete the last message, but the first two messages should have been deleted")
}

func getNumMessages(
	t *testing.T,
	cluster *cassandra.TestCluster,
	queueType persistence.QueueV2Type,
	queueName string,
	numMessages int,
) int {
	// We query the database directly here to get the actual count of messages deleted. If we just rely on the store
	// method to read messages, that would indicate that we're updating min_message_id correctly, but it wouldn't verify
	// that any messages less than min_message_id are actually deleted from the database.

	iter := cluster.GetSession().Query(
		cassandra.TemplateGetMessagesQuery,
		queueType,
		queueName,
		0,                               // partition
		persistence.FirstQueueMessageID, // minMessageID
		numMessages,                     // limit
	).Iter()
	numRemainingMessages := 0
	for iter.MapScan(map[string]interface{}{}) {
		numRemainingMessages++
	}
	require.NoError(t, iter.Close())
	return numRemainingMessages
}

func newQueueV2Store(session gocql.Session, opts ...func(params *testQueueParams)) persistence.QueueV2 {
	p := testQueueParams{
		logger: log.NewTestLogger(),
	}
	for _, opt := range opts {
		opt(&p)
	}
	return cassandra.NewQueueV2Store(session, p.logger)
}

func insertQueueMetadataWithMultiplePartitions(
	t *testing.T,
	session gocql.Session,
	queueType persistence.QueueV2Type,
	queueName string,
) {
	t.Helper()

	queuePB := persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {},
			1: {},
		},
	}
	bytes, _ := queuePB.Marshal()
	err := session.Query(
		cassandra.TemplateCreateQueueQuery,
		queueType,
		queueName,
		bytes,                               // payload
		enums.ENCODING_TYPE_PROTO3.String(), // payload encoding type
		0,                                   // version
	).Exec()
	require.NoError(t, err)
}

func deleteMessages(
	ctx context.Context,
	q persistence.QueueV2,
	queueType persistence.QueueV2Type,
	queueName string,
	maxID int,
) error {
	_, err := q.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{
			ID: int64(maxID),
		},
	})

	return err
}

func testCassandraNexusEndpointStore(t *testing.T, cluster *cassandra.TestCluster, tableVersion *atomic.Int64) {
	store := newNexusEndpointStore(cluster.GetSession())
	t.Run("ConcurrentCreate", func(t *testing.T) {
		testCassandraNexusEndpointStoreConcurrentCreate(t, store, tableVersion)
	})
	t.Run("ConcurrentUpdate", func(t *testing.T) {
		testCassandraNexusEndpointStoreConcurrentUpdate(t, store, tableVersion)
	})
	t.Run("ConcurrentCreateAndUpdate", func(t *testing.T) {
		testCassandraNexusEndpointStoreConcurrentCreateAndUpdate(t, store, tableVersion)
	})
	t.Run("ConcurrentUpdateAndDelete", func(t *testing.T) {
		testCassandraNexusEndpointStoreConcurrentUpdateAndDelete(t, store, tableVersion)
	})
	t.Run("DeleteWhilePaging", func(t *testing.T) {
		testCassandraNexusEndpointStoreDeleteWhilePaging(t, store, tableVersion)
	})
}

func testCassandraNexusEndpointStoreConcurrentCreate(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	numConcurrentRequests := 4

	wg := sync.WaitGroup{}
	wg.Add(numConcurrentRequests)
	starter := make(chan struct{})

	endpointID := uuid.NewString()
	createErrors := make(chan error, numConcurrentRequests)
	defer close(createErrors)

	requestTableVersion := tableVersion.Load()

	for i := 0; i < numConcurrentRequests; i++ {
		go func() {
			<-starter
			err := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
				LastKnownTableVersion: requestTableVersion,
				Endpoint: persistence.InternalNexusEndpoint{
					ID:      endpointID,
					Version: 0,
					Data: &commonpb.DataBlob{
						Data:         []byte("some dummy endpoint data"),
						EncodingType: enums.ENCODING_TYPE_PROTO3,
					}},
			})
			if err != nil {
				createErrors <- err
			} else {
				tableVersion.Add(1)
			}
			wg.Done()
		}()
	}

	close(starter)
	wg.Wait()

	require.Len(t, createErrors, numConcurrentRequests-1, "exactly 1 create request should succeed")
	assertNexusEndpointsTableVersion(t, tableVersion.Load(), store)
}

func testCassandraNexusEndpointStoreConcurrentUpdate(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	endpoint := persistence.InternalNexusEndpoint{
		ID:      uuid.NewString(),
		Version: 0,
		Data: &commonpb.DataBlob{
			Data:         []byte("some dummy endpoint data"),
			EncodingType: enums.ENCODING_TYPE_PROTO3,
		}}

	// Create an endpoint
	createErr := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: tableVersion.Load(),
		Endpoint:              endpoint,
	})
	require.NoError(t, createErr)
	tableVersion.Add(1)
	endpoint.Version++

	numConcurrentRequests := 4
	wg := sync.WaitGroup{}
	wg.Add(numConcurrentRequests)
	starter := make(chan struct{})

	updateErrors := make(chan error, numConcurrentRequests)
	defer close(updateErrors)

	for i := 0; i < numConcurrentRequests; i++ {
		go func() {
			<-starter
			err := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
				LastKnownTableVersion: tableVersion.Load(),
				Endpoint:              endpoint,
			})
			if err != nil {
				updateErrors <- err
			} else {
				tableVersion.Add(1)
			}
			wg.Done()
		}()
	}

	close(starter)
	wg.Wait()

	require.Len(t, updateErrors, numConcurrentRequests-1)
	assertNexusEndpointsTableVersion(t, tableVersion.Load(), store)
}

func testCassandraNexusEndpointStoreConcurrentCreateAndUpdate(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	firstEndpoint := persistence.InternalNexusEndpoint{
		ID:      uuid.NewString(),
		Version: 0,
		Data: &commonpb.DataBlob{
			Data:         []byte("some dummy endpoint data"),
			EncodingType: enums.ENCODING_TYPE_PROTO3,
		}}

	// Create an endpoint
	err := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: tableVersion.Load(),
		Endpoint:              firstEndpoint,
	})
	require.NoError(t, err)
	tableVersion.Add(1)
	firstEndpoint.Version++

	wg := sync.WaitGroup{}
	wg.Add(2)
	starter := make(chan struct{})
	var createErr, updateErr error

	requestTableVersion := tableVersion.Load()

	// Concurrently create an endpoint
	go func() {
		<-starter
		createErr = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: requestTableVersion,
			Endpoint: persistence.InternalNexusEndpoint{
				ID:      uuid.NewString(),
				Version: 0,
				Data: &commonpb.DataBlob{
					Data:         []byte("some dummy endpoint data"),
					EncodingType: enums.ENCODING_TYPE_PROTO3,
				}},
		})
		if createErr != nil {
			tableVersion.Add(1)
		}
		wg.Done()
	}()
	// Concurrently update the first endpoint
	go func() {
		<-starter
		updateErr = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: requestTableVersion,
			Endpoint:              firstEndpoint,
		})
		if updateErr != nil {
			tableVersion.Add(1)
		}
		wg.Done()
	}()

	close(starter)
	wg.Wait()

	if createErr == nil {
		require.ErrorContains(t, updateErr, "nexus endpoints table version mismatch")
	} else {
		require.ErrorContains(t, createErr, "nexus endpoints table version mismatch")
	}
	assertNexusEndpointsTableVersion(t, tableVersion.Load(), store)
}

func testCassandraNexusEndpointStoreConcurrentUpdateAndDelete(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	endpoint := persistence.InternalNexusEndpoint{
		ID:      uuid.NewString(),
		Version: 0,
		Data: &commonpb.DataBlob{
			Data:         []byte("some dummy endpoint data"),
			EncodingType: enums.ENCODING_TYPE_PROTO3,
		}}

	// Create an endpoint
	err := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: tableVersion.Load(),
		Endpoint:              endpoint,
	})
	require.NoError(t, err)
	tableVersion.Add(1)
	endpoint.Version++

	wg := sync.WaitGroup{}
	wg.Add(2)
	starter := make(chan struct{})
	var updateErr, deleteErr error

	requestTableVersion := tableVersion.Load()

	// Concurrently update the endpoint
	go func() {
		<-starter
		updateErr = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: requestTableVersion,
			Endpoint:              endpoint,
		})
		if updateErr != nil {
			tableVersion.Add(1)
		}
		wg.Done()
	}()
	// Concurrently delete the endpoint
	go func() {
		<-starter
		deleteErr = store.DeleteNexusEndpoint(ctx, &persistence.DeleteNexusEndpointRequest{
			LastKnownTableVersion: requestTableVersion,
			ID:                    endpoint.ID,
		})
		if deleteErr != nil {
			tableVersion.Add(1)
		}
		wg.Done()
	}()

	close(starter)
	wg.Wait()

	if updateErr == nil {
		require.ErrorContains(t, deleteErr, "nexus endpoints table version mismatch")
	} else {
		require.ErrorContains(t, updateErr, "nexus endpoints table version mismatch")
	}
	assertNexusEndpointsTableVersion(t, tableVersion.Load(), store)
}

func testCassandraNexusEndpointStoreDeleteWhilePaging(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create some endpoints
	numEndpoints := 3
	for i := 0; i < numEndpoints; i++ {
		err := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Endpoint: persistence.InternalNexusEndpoint{
				ID:      uuid.NewString(),
				Version: 0,
				Data: &commonpb.DataBlob{
					Data:         []byte("some dummy endpoint data"),
					EncodingType: enums.ENCODING_TYPE_PROTO3,
				}},
		})
		require.NoError(t, err)
		tableVersion.Add(1)
	}

	// List first page
	resp1, err := store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{
		LastKnownTableVersion: 0,
		PageSize:              2,
	})
	require.NoError(t, err)
	require.NotNil(t, resp1)
	require.NotNil(t, resp1.NextPageToken)
	require.Equal(t, tableVersion.Load(), resp1.TableVersion)

	// Delete last endpoint in first page
	err = store.DeleteNexusEndpoint(ctx, &persistence.DeleteNexusEndpointRequest{
		LastKnownTableVersion: tableVersion.Load(),
		ID:                    resp1.Endpoints[1].ID,
	})
	require.NoError(t, err)
	tableVersion.Add(1)

	// List second page
	resp2, err := store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{
		LastKnownTableVersion: 0,
		NextPageToken:         resp1.NextPageToken,
		PageSize:              2,
	})
	require.NoError(t, err)
	require.NotNil(t, resp2)
	require.Equal(t, tableVersion.Load(), resp2.TableVersion)
}

func newNexusEndpointStore(session gocql.Session, opts ...func(params *testQueueParams)) persistence.NexusEndpointStore {
	p := testQueueParams{
		logger: log.NewTestLogger(),
	}
	for _, opt := range opts {
		opt(&p)
	}
	return cassandra.NewNexusEndpointStore(session, p.logger)
}

func assertNexusEndpointsTableVersion(t *testing.T, expected int64, store persistence.NexusEndpointStore) {
	t.Helper()

	resp, err := store.ListNexusEndpoints(context.Background(), &persistence.ListNexusEndpointsRequest{
		LastKnownTableVersion: 0,
		PageSize:              1,
	})

	require.NoError(t, err)
	require.Equal(t, expected, resp.TableVersion)
}
