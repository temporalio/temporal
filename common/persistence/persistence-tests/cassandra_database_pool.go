package persistencetests

import (
	"errors"
	"fmt"
	"sync"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/cassandra"
)

type cassandraTestDatabase interface {
	PersistenceTestCluster
	DatabaseName() string
	OpenTestDatabase() error
	ResetTestDatabase() error
	DropTestDatabase() error
	CloseSession()
}

type cassandraDatabasePoolKey struct {
	host      string
	username  string
	password  string
	schemaDir string
	port      int
}

type cassandraDatabasePool struct {
	sync.Mutex
	available map[cassandraDatabasePoolKey][]string
	all       map[cassandraDatabasePoolKey]map[string]struct{}
}

type reusableCassandraTestCluster struct {
	cassandraTestDatabase
	key    cassandraDatabasePoolKey
	pool   *cassandraDatabasePool
	logger log.Logger
	reused bool
	once   sync.Once
}

var reusableCassandraDatabases = newCassandraDatabasePool()

func newCassandraDatabasePool() *cassandraDatabasePool {
	return &cassandraDatabasePool{
		available: make(map[cassandraDatabasePoolKey][]string),
		all:       make(map[cassandraDatabasePoolKey]map[string]struct{}),
	}
}

func (p *cassandraDatabasePool) acquire(key cassandraDatabasePoolKey, candidate string) (string, bool) {
	p.Lock()
	defer p.Unlock()

	available := p.available[key]
	if len(available) > 0 {
		last := len(available) - 1
		name := available[last]
		p.available[key] = available[:last]
		return name, true
	}
	if p.all[key] == nil {
		p.all[key] = make(map[string]struct{})
	}
	p.all[key][candidate] = struct{}{}
	return candidate, false
}

func (p *cassandraDatabasePool) release(key cassandraDatabasePoolKey, name string) {
	p.Lock()
	defer p.Unlock()
	p.available[key] = append(p.available[key], name)
}

func (p *cassandraDatabasePool) forget(key cassandraDatabasePoolKey, name string) {
	p.Lock()
	defer p.Unlock()
	delete(p.all[key], name)
}

func (p *cassandraDatabasePool) drain() map[cassandraDatabasePoolKey][]string {
	p.Lock()
	defer p.Unlock()

	databases := make(map[cassandraDatabasePoolKey][]string, len(p.all))
	for key, names := range p.all {
		for name := range names {
			databases[key] = append(databases[key], name)
		}
	}
	p.available = make(map[cassandraDatabasePoolKey][]string)
	p.all = make(map[cassandraDatabasePoolKey]map[string]struct{})
	return databases
}

func newReusableCassandraTestCluster(
	database cassandraTestDatabase,
	key cassandraDatabasePoolKey,
	reused bool,
	pool *cassandraDatabasePool,
	logger log.Logger,
) *reusableCassandraTestCluster {
	return &reusableCassandraTestCluster{
		cassandraTestDatabase: database,
		key:                   key,
		pool:                  pool,
		logger:                logger,
		reused:                reused,
	}
}

func (c *reusableCassandraTestCluster) SetupTestDatabase() {
	if !c.reused {
		c.cassandraTestDatabase.SetupTestDatabase()
		return
	}
	if err := c.OpenTestDatabase(); err != nil {
		c.logger.Fatal("Open reusable Cassandra database", tag.Error(err))
	}
}

func (c *reusableCassandraTestCluster) TearDownTestDatabase() {
	c.once.Do(func() {
		if err := c.ResetTestDatabase(); err != nil {
			dropErr := c.DropTestDatabase()
			c.CloseSession()
			if dropErr == nil {
				c.pool.forget(c.key, c.DatabaseName())
			}
			c.logger.Error(
				"Discard reusable Cassandra database after reset failure",
				tag.Error(errors.Join(err, dropErr)),
			)
			return
		}
		c.CloseSession()
		c.pool.release(c.key, c.DatabaseName())
	})
}

func newReusableCassandraDatabase(
	options *TestBaseOptions,
	logger log.Logger,
) PersistenceTestCluster {
	database := NewTestClusterForCassandra(options, logger)
	persistenceConfig := database.Config()
	cassandraConfig := persistenceConfig.DataStores[persistenceConfig.DefaultStore].Cassandra
	key := cassandraDatabasePoolKey{
		host:      cassandraConfig.Hosts,
		username:  cassandraConfig.User,
		password:  cassandraConfig.Password,
		schemaDir: options.SchemaDir,
		port:      cassandraConfig.Port,
	}
	name, reused := reusableCassandraDatabases.acquire(key, database.DatabaseName())
	if name != database.DatabaseName() {
		database = cassandra.NewTestCluster(
			name,
			cassandraConfig.User,
			cassandraConfig.Password,
			cassandraConfig.Hosts,
			cassandraConfig.Port,
			options.SchemaDir,
			options.FaultInjection,
			logger,
		)
	}
	return newReusableCassandraTestCluster(database, key, reused, reusableCassandraDatabases, logger)
}

// CloseReusableCassandraDatabases drops all keyspaces retained for reuse by functional tests.
func CloseReusableCassandraDatabases() error {
	var result error
	for key, names := range reusableCassandraDatabases.drain() {
		admin := cassandra.NewTestCluster(
			"",
			key.username,
			key.password,
			key.host,
			key.port,
			key.schemaDir,
			nil,
			log.NewNoopLogger(),
		)
		if err := admin.OpenSession("system"); err != nil {
			result = errors.Join(result, fmt.Errorf("connect to Cassandra to drop reusable databases: %w", err))
			continue
		}
		for _, name := range names {
			if err := cassandra.DropCassandraKeyspace(admin.GetSession(), name, log.NewNoopLogger()); err != nil {
				result = errors.Join(result, fmt.Errorf("drop reusable Cassandra database %q: %w", name, err))
			}
		}
		admin.CloseSession()
	}
	return result
}

var _ PersistenceTestCluster = (*reusableCassandraTestCluster)(nil)
