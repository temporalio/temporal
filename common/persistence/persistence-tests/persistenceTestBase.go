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

package persistencetests

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/stretchr/testify/suite"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/environment"
)

// TimePrecision is needed to account for database timestamp precision.
// Cassandra only provides milliseconds timestamp precision, so we need to use tolerance when doing comparison
const TimePrecision = 2 * time.Millisecond

type (
	// TransferTaskIDGenerator generates IDs for transfer tasks written by helper methods
	TransferTaskIDGenerator interface {
		GenerateTransferTaskID() (int64, error)
	}

	// TestBaseOptions options to configure workflow test base.
	TestBaseOptions struct {
		SQLDBPluginName   string
		DBName            string
		DBUsername        string
		DBPassword        string
		DBHost            string
		DBPort            int `yaml:"-"`
		ConnectAttributes map[string]string
		StoreType         string                 `yaml:"-"`
		SchemaDir         string                 `yaml:"-"`
		FaultInjection    *config.FaultInjection `yaml:"faultinjection"`
	}

	// TestBase wraps the base setup needed to create workflows over persistence layer.
	TestBase struct {
		suite.Suite
		ShardMgr                  persistence.ShardManager
		AbstractDataStoreFactory  client.AbstractDataStoreFactory
		FaultInjection            *client.FaultInjectionDataStoreFactory
		Factory                   client.Factory
		ExecutionManager          persistence.ExecutionManager
		TaskMgr                   persistence.TaskManager
		ClusterMetadataManager    persistence.ClusterMetadataManager
		MetadataManager           persistence.MetadataManager
		NamespaceReplicationQueue persistence.NamespaceReplicationQueue
		ShardInfo                 *persistencespb.ShardInfo
		TaskIDGenerator           TransferTaskIDGenerator
		ClusterMetadata           cluster.Metadata
		SearchAttributesManager   searchattribute.Manager
		ReadLevel                 int64
		ReplicationReadLevel      int64
		DefaultTestCluster        PersistenceTestCluster
		Logger                    log.Logger
	}

	// PersistenceTestCluster exposes management operations on a database
	PersistenceTestCluster interface {
		SetupTestDatabase()
		TearDownTestDatabase()
		Config() config.Persistence
	}

	// TestTransferTaskIDGenerator helper
	TestTransferTaskIDGenerator struct {
		seqNum int64
	}
)

// NewTestBaseWithCassandra returns a persistence test base backed by cassandra datastore
func NewTestBaseWithCassandra(options *TestBaseOptions) TestBase {
	if options.DBName == "" {
		options.DBName = "test_" + GenerateRandomDBName(3)
	}
	logger := log.NewTestLogger()
	testCluster := cassandra.NewTestCluster(options.DBName, options.DBUsername, options.DBPassword, options.DBHost, options.DBPort, options.SchemaDir, options.FaultInjection, logger)
	return NewTestBaseForCluster(testCluster, logger)
}

// NewTestBaseWithSQL returns a new persistence test base backed by SQL
func NewTestBaseWithSQL(options *TestBaseOptions) TestBase {
	if options.DBName == "" {
		options.DBName = "test_" + GenerateRandomDBName(3)
	}
	logger := log.NewTestLogger()

	if options.DBPort == 0 {
		switch options.SQLDBPluginName {
		case mysql.PluginName:
			options.DBPort = environment.GetMySQLPort()
		case postgresql.PluginName:
			options.DBPort = environment.GetPostgreSQLPort()
		case sqlite.PluginName:
			options.DBPort = 0
		default:
			panic(fmt.Sprintf("unknown sql store drier: %v", options.SQLDBPluginName))
		}
	}
	if options.DBHost == "" {
		switch options.SQLDBPluginName {
		case mysql.PluginName:
			options.DBHost = environment.GetMySQLAddress()
		case postgresql.PluginName:
			options.DBHost = environment.GetPostgreSQLAddress()
		case sqlite.PluginName:
			options.DBHost = environment.Localhost
		default:
			panic(fmt.Sprintf("unknown sql store drier: %v", options.SQLDBPluginName))
		}
	}
	testCluster := sql.NewTestCluster(options.SQLDBPluginName, options.DBName, options.DBUsername, options.DBPassword, options.DBHost, options.DBPort, options.ConnectAttributes, options.SchemaDir, options.FaultInjection, logger)
	return NewTestBaseForCluster(testCluster, logger)
}

// NewTestBase returns a persistence test base backed by either cassandra or sql
func NewTestBase(options *TestBaseOptions) TestBase {
	switch options.StoreType {
	case config.StoreTypeSQL:
		return NewTestBaseWithSQL(options)
	case config.StoreTypeNoSQL:
		return NewTestBaseWithCassandra(options)
	default:
		panic("invalid storeType " + options.StoreType)
	}
}

func NewTestBaseForCluster(testCluster PersistenceTestCluster, logger log.Logger) TestBase {
	return TestBase{
		DefaultTestCluster: testCluster,
		Logger:             logger,
	}
}

// Setup sets up the test base, must be called as part of SetupSuite
func (s *TestBase) Setup(clusterMetadataConfig *cluster.Config) {
	var err error
	shardID := int32(10)
	if clusterMetadataConfig == nil {
		clusterMetadataConfig = cluster.NewTestClusterMetadataConfig(false, false)
	}

	clusterName := clusterMetadataConfig.CurrentClusterName

	s.DefaultTestCluster.SetupTestDatabase()

	cfg := s.DefaultTestCluster.Config()
	dataStoreFactory, faultInjection := client.DataStoreFactoryProvider(
		client.ClusterName(clusterName),
		resolver.NewNoopResolver(),
		&cfg,
		s.AbstractDataStoreFactory,
		s.Logger,
		metrics.NoopClient,
	)
	factory := client.NewFactory(dataStoreFactory, &cfg, nil, serialization.NewSerializer(), clusterName, metrics.NoopClient, s.Logger)

	s.TaskMgr, err = factory.NewTaskManager()
	s.fatalOnError("NewTaskManager", err)

	s.ClusterMetadataManager, err = factory.NewClusterMetadataManager()
	s.fatalOnError("NewClusterMetadataManager", err)

	s.ClusterMetadata = cluster.NewMetadataFromConfig(clusterMetadataConfig, s.ClusterMetadataManager, dynamicconfig.NewNoopCollection(), s.Logger)
	s.SearchAttributesManager = searchattribute.NewManager(clock.NewRealTimeSource(), s.ClusterMetadataManager, dynamicconfig.GetBoolPropertyFn(true))

	s.MetadataManager, err = factory.NewMetadataManager()
	s.fatalOnError("NewMetadataManager", err)

	s.ShardMgr, err = factory.NewShardManager()
	s.fatalOnError("NewShardManager", err)

	s.ExecutionManager, err = factory.NewExecutionManager()
	s.fatalOnError("NewExecutionManager", err)

	s.Factory = factory
	s.FaultInjection = faultInjection

	s.ReadLevel = 0
	s.ReplicationReadLevel = 0
	s.ShardInfo = &persistencespb.ShardInfo{
		ShardId: shardID,
		RangeId: 0,
	}

	s.TaskIDGenerator = &TestTransferTaskIDGenerator{}
	_, err = s.ShardMgr.GetOrCreateShard(context.Background(), &persistence.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: s.ShardInfo,
	})
	s.fatalOnError("CreateShard", err)

	queue, err := factory.NewNamespaceReplicationQueue()
	s.fatalOnError("Create NamespaceReplicationQueue", err)
	s.NamespaceReplicationQueue = queue
}

func (s *TestBase) fatalOnError(msg string, err error) {
	if err != nil {
		s.Logger.Fatal(msg, tag.Error(err))
	}
}

// GetOrCreateShard is a utility method to get/create the shard using persistence layer
func (s *TestBase) GetOrCreateShard(ctx context.Context, shardID int32, owner string, rangeID int64) (*persistencespb.ShardInfo, error) {
	info := &persistencespb.ShardInfo{
		ShardId: shardID,
		Owner:   owner,
		RangeId: rangeID,
	}
	resp, err := s.ShardMgr.GetOrCreateShard(ctx, &persistence.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: info,
	})
	if err != nil {
		return nil, err
	}
	return resp.ShardInfo, nil
}

// UpdateShard is a utility method to update the shard using persistence layer
func (s *TestBase) UpdateShard(ctx context.Context, updatedInfo *persistencespb.ShardInfo, previousRangeID int64) error {
	return s.ShardMgr.UpdateShard(ctx, &persistence.UpdateShardRequest{
		ShardInfo:       updatedInfo,
		PreviousRangeID: previousRangeID,
	})
}

// TearDownWorkflowStore to cleanup
func (s *TestBase) TearDownWorkflowStore() {
	s.TaskMgr.Close()
	s.ClusterMetadataManager.Close()
	s.MetadataManager.Close()
	s.ExecutionManager.Close()
	s.ShardMgr.Close()
	s.ExecutionManager.Close()
	s.NamespaceReplicationQueue.Stop()
	s.Factory.Close()
	s.DefaultTestCluster.TearDownTestDatabase()
}

// EqualTimesWithPrecision assertion that two times are equal within precision
func (s *TestBase) EqualTimesWithPrecision(t1, t2 time.Time, precision time.Duration) {
	s.True(timeComparator(t1, t2, precision),
		"Not equal: \n"+
			"expected: %s\n"+
			"actual  : %s%s", t1, t2,
	)
}

// EqualTimes assertion that two times are equal within two millisecond precision
func (s *TestBase) EqualTimes(t1, t2 time.Time) {
	s.EqualTimesWithPrecision(t1, t2, TimePrecision)
}

// GenerateTransferTaskID helper
func (g *TestTransferTaskIDGenerator) GenerateTransferTaskID() (int64, error) {
	return atomic.AddInt64(&g.seqNum, 1), nil
}

// Publish is a utility method to add messages to the queue
func (s *TestBase) Publish(ctx context.Context, task *replicationspb.ReplicationTask) error {
	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetBackoffCoefficient(1.5)
	retryPolicy.SetMaximumAttempts(5)

	return backoff.ThrottleRetry(
		func() error {
			return s.NamespaceReplicationQueue.Publish(ctx, task)
		},
		retryPolicy,
		func(e error) bool {
			return common.IsPersistenceTransientError(e) || isMessageIDConflictError(e)
		})
}

func isMessageIDConflictError(err error) bool {
	_, ok := err.(*persistence.ConditionFailedError)
	return ok
}

// GetReplicationMessages is a utility method to get messages from the queue
func (s *TestBase) GetReplicationMessages(
	ctx context.Context,
	lastMessageID int64,
	pageSize int,
) ([]*replicationspb.ReplicationTask, int64, error) {
	return s.NamespaceReplicationQueue.GetReplicationMessages(ctx, lastMessageID, pageSize)
}

// UpdateAckLevel updates replication queue ack level
func (s *TestBase) UpdateAckLevel(
	ctx context.Context,
	lastProcessedMessageID int64,
	clusterName string,
) error {
	return s.NamespaceReplicationQueue.UpdateAckLevel(ctx, lastProcessedMessageID, clusterName)
}

// GetAckLevels returns replication queue ack levels
func (s *TestBase) GetAckLevels(
	ctx context.Context,
) (map[string]int64, error) {
	return s.NamespaceReplicationQueue.GetAckLevels(ctx)
}

// PublishToNamespaceDLQ is a utility method to add messages to the namespace DLQ
func (s *TestBase) PublishToNamespaceDLQ(ctx context.Context, task *replicationspb.ReplicationTask) error {
	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetBackoffCoefficient(1.5)
	retryPolicy.SetMaximumAttempts(5)

	return backoff.ThrottleRetryContext(
		ctx,
		func(ctx context.Context) error {
			return s.NamespaceReplicationQueue.PublishToDLQ(ctx, task)
		},
		retryPolicy,
		func(e error) bool {
			return common.IsPersistenceTransientError(e) || isMessageIDConflictError(e)
		})
}

// GetMessagesFromNamespaceDLQ is a utility method to get messages from the namespace DLQ
func (s *TestBase) GetMessagesFromNamespaceDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*replicationspb.ReplicationTask, []byte, error) {
	return s.NamespaceReplicationQueue.GetMessagesFromDLQ(
		ctx,
		firstMessageID,
		lastMessageID,
		pageSize,
		pageToken,
	)
}

// UpdateNamespaceDLQAckLevel updates namespace dlq ack level
func (s *TestBase) UpdateNamespaceDLQAckLevel(
	ctx context.Context,
	lastProcessedMessageID int64,
) error {
	return s.NamespaceReplicationQueue.UpdateDLQAckLevel(ctx, lastProcessedMessageID)
}

// GetNamespaceDLQAckLevel returns namespace dlq ack level
func (s *TestBase) GetNamespaceDLQAckLevel(
	ctx context.Context,
) (int64, error) {
	return s.NamespaceReplicationQueue.GetDLQAckLevel(ctx)
}

// DeleteMessageFromNamespaceDLQ deletes one message from namespace DLQ
func (s *TestBase) DeleteMessageFromNamespaceDLQ(
	ctx context.Context,
	messageID int64,
) error {
	return s.NamespaceReplicationQueue.DeleteMessageFromDLQ(ctx, messageID)
}

// RangeDeleteMessagesFromNamespaceDLQ deletes messages from namespace DLQ
func (s *TestBase) RangeDeleteMessagesFromNamespaceDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	return s.NamespaceReplicationQueue.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
}

func randString(length int) string {
	const lowercaseSet = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, length)
	for i := range b {
		b[i] = lowercaseSet[rand.Int63()%int64(len(lowercaseSet))]
	}
	return string(b)
}

// GenerateRandomDBName helper
// Format: MMDDHHMMSS_abc
func GenerateRandomDBName(n int) string {
	now := time.Now().UTC()
	rand.Seed(now.UnixNano())
	var prefix strings.Builder
	prefix.WriteString(now.Format("0102150405"))
	prefix.WriteRune('_')
	prefix.WriteString(randString(n))
	return prefix.String()
}

func timeComparator(t1, t2 time.Time, timeTolerance time.Duration) bool {
	diff := t2.Sub(t1)
	return diff.Nanoseconds() <= timeTolerance.Nanoseconds()
}
