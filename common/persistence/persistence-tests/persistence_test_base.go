package persistencetests

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"go.opentelemetry.io/otel/trace"
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
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/temporal/environment"
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
		StoreType         string `yaml:"-"`
		SchemaDir         string `yaml:"-"`
		FaultInjection    *config.FaultInjection
		Logger            log.Logger `yaml:"-"`
		ReuseDatabase     bool       `yaml:"-"`
	}
)

// ApplyDefaults copies database configuration from src, preserving any non-zero values already set.
func (o *TestBaseOptions) ApplyDefaults(src *TestBaseOptions) {
	o.StoreType = cmp.Or(o.StoreType, src.StoreType)
	o.SQLDBPluginName = cmp.Or(o.SQLDBPluginName, src.SQLDBPluginName)
	o.DBName = cmp.Or(o.DBName, src.DBName)
	o.DBUsername = cmp.Or(o.DBUsername, src.DBUsername)
	o.DBPassword = cmp.Or(o.DBPassword, src.DBPassword)
	o.DBHost = cmp.Or(o.DBHost, src.DBHost)
	o.DBPort = cmp.Or(o.DBPort, src.DBPort)
	o.SchemaDir = cmp.Or(o.SchemaDir, src.SchemaDir)
	if o.ConnectAttributes == nil {
		o.ConnectAttributes = src.ConnectAttributes
	}
}

type (
	// TestBase wraps the base setup needed to create workflows over persistence layer.
	TestBase struct {
		suite.Suite
		ShardMgr                  persistence.ShardManager
		AbstractDataStoreFactory  client.AbstractDataStoreFactory
		VisibilityStoreFactory    visibility.VisibilityStoreFactory
		Factory                   client.Factory
		ExecutionManager          persistence.ExecutionManager
		TaskMgr                   persistence.TaskManager
		FairTaskMgr               persistence.FairTaskManager
		ClusterMetadataManager    persistence.ClusterMetadataManager
		MetadataManager           persistence.MetadataManager
		NamespaceReplicationQueue persistence.NamespaceReplicationQueue
		NexusEndpointManager      persistence.NexusEndpointManager
		ShardInfo                 *persistencespb.ShardInfo
		TaskIDGenerator           TransferTaskIDGenerator
		ClusterMetadata           cluster.Metadata
		SearchAttributesManager   searchattribute.Manager
		PersistenceRateLimiter    quotas.RequestRateLimiter
		PersistenceHealthSignals  persistence.HealthSignalAggregator
		ReadLevel                 int64
		ReplicationReadLevel      int64
		DefaultTestCluster        PersistenceTestCluster
		Logger                    log.Logger
		TracerProvider            trace.TracerProvider
		databaseLeases            []sqlplugin.DatabaseLease
	}

	// PersistenceTestCluster exposes management operations on a database
	PersistenceTestCluster interface {
		SetupTestDatabase()
		TearDownTestDatabase()
		Config() config.Persistence
		StoreType() string
	}

	// TestTransferTaskIDGenerator helper
	TestTransferTaskIDGenerator struct {
		seqNum int64
	}
)

// NewTestBaseWithCassandra returns a persistence test base backed by cassandra datastore
func NewTestBaseWithCassandra(options *TestBaseOptions) *TestBase {
	logger := log.NewTestLogger()
	if options.ReuseDatabase {
		return NewTestBaseForCluster(newReusableCassandraDatabase(options, logger), logger)
	}
	testCluster := NewTestClusterForCassandra(options, logger)
	return NewTestBaseForCluster(testCluster, logger)
}

func NewTestClusterForCassandra(options *TestBaseOptions, logger log.Logger) *cassandra.TestCluster {
	if options.DBName == "" {
		options.DBName = GenerateRandomDBName()
	}
	testCluster := cassandra.NewTestCluster(
		options.DBName,
		options.DBUsername,
		options.DBPassword,
		options.DBHost,
		options.DBPort,
		options.SchemaDir,
		options.FaultInjection,
		logger,
	)
	return testCluster
}

// NewTestBaseWithSQL returns a new persistence test base backed by SQL
func NewTestBaseWithSQL(options *TestBaseOptions) *TestBase {
	logger := options.Logger
	if logger == nil {
		logger = log.NewTestLogger()
	}

	if options.DBPort == 0 {
		switch options.SQLDBPluginName {
		case mysql.PluginName:
			options.DBPort = environment.GetMySQLPort()
		case postgresql.PluginName, postgresql.PluginNamePGX:
			options.DBPort = environment.GetPostgreSQLPort()
		case sqlite.PluginName:
			options.DBPort = 0
		default:
			panic(fmt.Sprintf("unknown sql store driver: %v", options.SQLDBPluginName))
		}
	}
	if options.DBHost == "" {
		switch options.SQLDBPluginName {
		case mysql.PluginName:
			options.DBHost = environment.GetMySQLAddress()
		case postgresql.PluginName, postgresql.PluginNamePGX:
			options.DBHost = environment.GetPostgreSQLAddress()
		case sqlite.PluginName:
			options.DBHost = environment.GetLocalhostIP()
		default:
			panic(fmt.Sprintf("unknown sql store driver: %v", options.SQLDBPluginName))
		}
	}
	testCluster := sql.NewTestCluster(
		options.SQLDBPluginName,
		options.DBName,
		options.DBUsername,
		options.DBPassword,
		options.DBHost,
		options.DBPort,
		options.ConnectAttributes,
		options.SchemaDir,
		options.FaultInjection,
		logger,
	)
	return NewTestBaseForCluster(testCluster, logger)
}

func NewTestBaseWithEs(options *TestBaseOptions) *TestBase {
	logger := options.Logger
	if logger == nil {
		logger = log.NewTestLogger()
	}

	if options.DBHost == "" {
		options.DBHost = environment.GetESAddress()
	}
	if options.DBPort == 0 {
		options.DBPort = environment.GetESPort()
	}

	testCluster := newEsTestCluster(
		options.DBHost,
		options.DBPort,
		options.DBUsername,
		options.DBPassword,
		options.DBName,
		logger,
	)
	return NewTestBaseForCluster(testCluster, logger)
}

// NewTestBase returns a persistence test base backed by either cassandra or sql
func NewTestBase(options *TestBaseOptions) *TestBase {
	switch options.StoreType {
	case config.StoreTypeSQL:
		return NewTestBaseWithSQL(options)
	case config.StoreTypeNoSQL:
		return NewTestBaseWithCassandra(options)
	default:
		panic("invalid storeType " + options.StoreType)
	}
}

func NewTestBaseForCluster(testCluster PersistenceTestCluster, logger log.Logger) *TestBase {
	return &TestBase{
		DefaultTestCluster: testCluster,
		Logger:             logger,
		TracerProvider:     telemetry.NoopTracerProvider,
	}
}

// Setup sets up the test base, must be called as part of SetupSuite
func (s *TestBase) Setup(clusterMetadataConfig *cluster.Config) {
	var err error
	shardID := int32(10)
	if clusterMetadataConfig == nil {
		clusterMetadataConfig = cluster.NewTestClusterMetadataConfig(false, false)
	}
	if s.PersistenceHealthSignals == nil {
		s.PersistenceHealthSignals = persistence.NoopHealthSignalAggregator
	}

	clusterName := clusterMetadataConfig.CurrentClusterName

	s.DefaultTestCluster.SetupTestDatabase()

	cfg := s.DefaultTestCluster.Config()
	if err := s.acquireDatabaseLeases(cfg); err != nil {
		s.Logger.Fatal("Acquire database leases", tag.Error(err))
	}
	serializer := serialization.NewSerializer()
	dataStoreFactory := client.DataStoreFactoryProvider(
		client.ClusterName(clusterName),
		resolver.NewNoopResolver(),
		&cfg,
		s.AbstractDataStoreFactory,
		s.Logger,
		metrics.NoopMetricsHandler,
		s.TracerProvider,
		serializer,
	)
	factory := client.NewFactory(
		dataStoreFactory,
		&cfg,
		s.PersistenceRateLimiter,
		quotas.NoopRequestRateLimiter,
		quotas.NoopRequestRateLimiter,
		serializer,
		nil,
		clusterName,
		metrics.NoopMetricsHandler,
		s.Logger,
		s.PersistenceHealthSignals,
		func() bool { return false },
		func() bool { return false },
	)

	s.TaskMgr, err = factory.NewTaskManager()
	s.fatalOnError("NewTaskManager", err)

	s.FairTaskMgr, err = factory.NewFairTaskManager()
	// TODO: re-enable error check after FairTaskManager is implemented for sql
	// s.fatalOnError("NewFairTaskManager", err)
	_ = err

	s.ClusterMetadataManager, err = factory.NewClusterMetadataManager()
	s.fatalOnError("NewClusterMetadataManager", err)

	s.ClusterMetadata = cluster.NewMetadataFromConfig(clusterMetadataConfig, s.ClusterMetadataManager, dynamicconfig.NewNoopCollection(), s.Logger)
	s.SearchAttributesManager = searchattribute.NewManager(
		clock.NewRealTimeSource(),
		s.ClusterMetadataManager,
		s.Logger,
		dynamicconfig.GetBoolPropertyFn(true),
	)

	s.MetadataManager, err = factory.NewMetadataManager()
	s.fatalOnError("NewMetadataManager", err)

	s.ShardMgr, err = factory.NewShardManager()
	s.fatalOnError("NewShardManager", err)

	s.ExecutionManager, err = factory.NewExecutionManager()
	s.fatalOnError("NewExecutionManager", err)

	s.NexusEndpointManager, err = factory.NewNexusEndpointManager()
	s.fatalOnError("NewNexusEndpointManager", err)

	s.Factory = factory

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

func (s *TestBase) acquireDatabaseLeases(cfg config.Persistence) error {
	seenStores := make(map[string]struct{}, 2)
	for _, storeName := range []string{cfg.DefaultStore, cfg.VisibilityStore} {
		if _, seen := seenStores[storeName]; seen {
			continue
		}
		seenStores[storeName] = struct{}{}
		store, ok := cfg.DataStores[storeName]
		if !ok || store.SQL == nil {
			continue
		}
		lease, err := sql.AcquireDatabaseLease(
			store.SQL,
			resolver.NewNoopResolver(),
			s.Logger,
			metrics.NoopMetricsHandler,
		)
		if err != nil {
			return errors.Join(err, s.releaseDatabaseLeases())
		}
		s.databaseLeases = append(s.databaseLeases, lease)
	}
	return nil
}

func (s *TestBase) releaseDatabaseLeases() error {
	var err error
	for i := len(s.databaseLeases) - 1; i >= 0; i-- {
		err = errors.Join(err, s.databaseLeases[i].Close())
	}
	s.databaseLeases = nil
	return err
}

// TearDownWorkflowStore to cleanup
func (s *TestBase) TearDownWorkflowStore() {
	if s.TaskMgr != nil {
		s.TaskMgr.Close()
	}
	if s.FairTaskMgr != nil {
		s.FairTaskMgr.Close()
	}
	if s.ClusterMetadataManager != nil {
		s.ClusterMetadataManager.Close()
	}
	if s.MetadataManager != nil {
		s.MetadataManager.Close()
	}
	if s.ExecutionManager != nil {
		s.ExecutionManager.Close()
	}
	if s.ShardMgr != nil {
		s.ShardMgr.Close()
	}
	if s.NexusEndpointManager != nil {
		s.NexusEndpointManager.Close()
	}
	if s.NamespaceReplicationQueue != nil {
		s.NamespaceReplicationQueue.Close()
	}
	if s.Factory != nil {
		s.Factory.Close()
	}
	if err := s.releaseDatabaseLeases(); err != nil {
		s.Logger.Error("Release database leases", tag.Error(err))
	}
	if s.DefaultTestCluster != nil {
		s.DefaultTestCluster.TearDownTestDatabase()
	}
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
	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond).
		WithBackoffCoefficient(1.5).
		WithMaximumAttempts(20)

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
	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond).
		WithBackoffCoefficient(1.5).
		WithMaximumAttempts(20)

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

func GenerateRandomDBName() string {
	uuidPart := strings.ReplaceAll(uuid.NewString(), "-", "")
	// Keep generated DB names short enough for Cassandra keyspaces after XDC tests append cluster suffixes.
	return "test_" + uuidPart[:24]
}

func timeComparator(t1, t2 time.Time, timeTolerance time.Duration) bool {
	diff := t2.Sub(t1)
	return diff.Nanoseconds() <= timeTolerance.Nanoseconds()
}
