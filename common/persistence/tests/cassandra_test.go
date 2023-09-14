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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
)

type (
	// failingSession is a [gocql.Session] which fails queries to enqueue or read messages.
	failingSession struct {
		gocql.Session
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
	t.Parallel()

	cluster := persistencetests.NewTestClusterForCassandra(&persistencetests.TestBaseOptions{}, log.NewNoopLogger())
	cluster.SetupTestDatabase()
	t.Cleanup(cluster.TearDownTestDatabase)

	t.Run("Generic", func(t *testing.T) {
		t.Parallel()
		RunQueueV2TestSuite(t, cassandra.NewQueueV2Store(cluster.GetSession()))
	})
	t.Run("QueueMessageIDConflict", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrQueueMessageConflict(t, cluster)
	})
	t.Run("ErrInvalidQueueMessageEncodingType", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrInvalidQueueMessageEncodingType(t, cluster)
	})
	t.Run("ErrReadQueueMessagesQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrReadQueueMessagesQuery(t, cluster)
	})
	t.Run("ErrEnqueueMessageQuery", func(t *testing.T) {
		t.Parallel()
		testCassandraQueueV2ErrEnqueueMessageQuery(t, cluster)
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

	q := cassandra.NewQueueV2Store(session)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()

	results := make(chan enqueueMessageResult, numConcurrentWrites)

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
				results <- enqueueMessageResult{err: err}
			} else {
				results <- enqueueMessageResult{id: int(res.Metadata.ID)}
			}
		}()
	}

	for i := 0; i < numConcurrentWrites; i++ {
		<-session.enqueueCalled
	}
	close(session.enqueueCanProceed)

	numConflicts := 0
	writtenMessageIDs := make([]int, 0, 1)

	for i := 0; i < numConcurrentWrites; i++ {
		res := <-results
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
	q := cassandra.NewQueueV2Store(session)
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	err := session.Query(
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
	assert.ErrorIs(t, err, cassandra.ErrInvalidQueueMessageEncodingType)
	assert.ErrorContains(t, err, "bad-encoding-type")
}

func testCassandraQueueV2ErrReadQueueMessagesQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := cassandra.NewQueueV2Store(failingSession{Session: cluster.GetSession()})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
	assert.ErrorContains(t, err, assert.AnError.Error())
	assert.ErrorContains(t, err, "QueueV2ReadMessages")
}

func (q failingQuery) MapScanCAS(map[string]interface{}) (bool, error) {
	return false, assert.AnError
}

func (q failingQuery) WithContext(context.Context) gocql.Query {
	return q
}

func (f failingSession) Query(query string, args ...interface{}) gocql.Query {
	if query == cassandra.TemplateEnqueueMessageQuery || query == cassandra.TemplateGetMessagesQuery {
		return failingQuery{}
	}
	return f.Session.Query(query, args...)
}

func testCassandraQueueV2ErrEnqueueMessageQuery(t *testing.T, cluster *cassandra.TestCluster) {
	q := cassandra.NewQueueV2Store(failingSession{Session: cluster.GetSession()})
	ctx := context.Background()
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	_, err := q.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
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
