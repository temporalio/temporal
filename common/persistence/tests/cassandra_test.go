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
	"testing"
	"time"

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
	// failingIter is a [gocql.Iter] which fails when iterated.
	failingIter struct{}
	// blockingSession is a [gocql.Session] designed for testing concurrent inserts.
	// See cassandra.QueueMessageIDConflict for more.
	blockingSession struct {
		gocql.Session
		enqueueCalled     chan struct{}
		enqueueCanProceed chan struct{}
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
)

func (f failingIter) Scan(i ...interface{}) bool {
	return false
}

func (f failingIter) MapScan(m map[string]interface{}) bool {
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

func (l *testLogger) Warn(msg string, tags ...tag.Tag) {
	l.warningMsgs = append(l.warningMsgs, msg)
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

// TODO: Merge persistence-tests into the tests directory.

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

func TestCassandraVisibilityPersistence(t *testing.T) {
	s := &VisibilityPersistenceSuite{
		TestBase: persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{}),
	}
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
		runCassandraSpecificQueueV2Tests(t, cluster)
	})
}

func runCassandraSpecificQueueV2Tests(t *testing.T, cluster *cassandra.TestCluster) {
	t.Run("QueueMessageIDConflict", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrQueueMessageConflict(t, cluster)
	})
	t.Run("ErrInvalidQueueMessageEncodingType", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrInvalidQueueMessageEncodingType(t, cluster)
	})
	t.Run("ErrReadMessagesQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrGetMessagesQuery(t, cluster)
	})
	t.Run("ErrGetMaxMessageIDQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrGetMaxMessageIDQuery(t, cluster)
	})
	t.Run("ErrEnqueueMessageQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrEnqueueMessageQuery(t, cluster)
	})
	t.Run("ErrInvalidPayloadEncodingType", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrInvalidPayloadEncodingType(t, cluster)
	})
	t.Run("ErrInvalidPayload", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrInvalidPayload(t, cluster)
	})
	t.Run("ErrQueueWithMultiplePartitions", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrQueueWithMultiplePartitions(t, cluster)
	})
}

// Query checks if the query is for inserting a message, and, if so, it notifies the test and then blocks until
// the test unblocks it.
func (f *blockingSession) Query(query string, args ...interface{}) gocql.Query {
	if query == cassandra.TemplateEnqueueMessageQuery {
		f.enqueueCalled <- struct{}{}
		<-f.enqueueCanProceed
	}

	return f.Session.Query(query, args...)
}

// testCassandraQueueV2ErrQueueMessageConflict tests that when there are concurrent inserts to the queue, only one of
// them is accepted if they try to enqueue a message with the same ID, and the other clients are given the correct
// error.
func testCassandraQueueV2ErrQueueMessageConflict(t *testing.T, cluster *cassandra.TestCluster) {
	const numConcurrentWrites = 5

	session := &blockingSession{
		Session:           cluster.GetSession(),
		enqueueCalled:     make(chan struct{}, numConcurrentWrites),
		enqueueCanProceed: make(chan struct{}),
	}

	q := newQueueV2Store(session)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
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
		i := i
		go func() {
			res, err := q.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
				QueueType: queueType,
				QueueName: queueName,
				Blob: commonpb.DataBlob{
					EncodingType: enums.ENCODING_TYPE_JSON,
					Data:         []byte(strconv.Itoa(i)),
				},
			})
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
			t.Fatal("timed out waiting for enqueue to be called")
		case <-session.enqueueCalled:
		}
	}
	close(session.enqueueCanProceed)

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
			assert.ErrorContains(t, res.err, cassandra.QueueMessageIDConflict)

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
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2ReadMessages")
}

func testCassandraQueueV2ErrGetMaxMessageIDQuery(t *testing.T, cluster *cassandra.TestCluster) {
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
	encodingType := enums.ENCODING_TYPE_JSON
	_, err = q.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
		QueueType: queueType,
		QueueName: queueName,
		Blob: commonpb.DataBlob{
			EncodingType: encodingType,
			Data:         []byte("1"),
		},
	})
	require.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2GetMaxMessageID")
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
	_, err = q.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
		QueueType: queueType,
		QueueName: queueName,
		Blob: commonpb.DataBlob{
			EncodingType: enums.ENCODING_TYPE_JSON,
			Data:         []byte("1"),
		},
	})
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2EnqueueMessage")
}

func testCassandraQueueV2ErrInvalidPayloadEncodingType(t *testing.T, cluster *cassandra.TestCluster) {
	// Manually insert a row into the queues table that has an invalid metadata payload encoding type and then verify
	// that we gracefully handle the error when we try to read from this queue.

	session := cluster.GetSession()
	q := newQueueV2Store(session)
	queueType := persistence.QueueTypeHistoryNormal
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

func testCassandraQueueV2ErrQueueWithMultiplePartitions(t *testing.T, cluster *cassandra.TestCluster) {
	// If there's a queue created with multiple partitions in a later release and then the user downgrades to a version
	// with code that only supports a single partition, we should still handle that case.

	session := cluster.GetSession()
	logger := &testLogger{}
	q := newQueueV2Store(session, func(params *testQueueParams) {
		params.logger = logger
	})
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
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
	_, err = q.EnqueueMessage(context.Background(), &persistence.InternalEnqueueMessageRequest{
		QueueType: queueType,
		QueueName: queueName,
		Blob: commonpb.DataBlob{
			EncodingType: enums.ENCODING_TYPE_JSON,
			Data:         []byte("1"),
		},
	})
	require.NoError(t, err)
	_, err = q.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "partitions")
	assert.ErrorContains(t, err, "2")
	assert.ErrorContains(t, err, "downgrade")
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
