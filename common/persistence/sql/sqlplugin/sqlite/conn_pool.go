package sqlite

import (
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/resolver"
)

// This pool properly enabled the support for SQLite in the temporal server.
// Internal Temporal services are highly isolated, each will create at least a single connection to the database violating
// the SQLite concept of safety only within a single thread.
type connPool struct {
	mu   sync.Mutex
	pool map[string]entry
}

type entry struct {
	db         *sqlx.DB
	refCount   int
	closeTimer *time.Timer
}

// Temporal starts and stops logical SQLite DB handles during startup. Closing
// the shared database immediately can make the next startup step fail with
// "db is closed", so real close is delayed and canceled when the DSN is reused.
const closeDelay = 100 * time.Millisecond

func newConnPool() *connPool {
	return &connPool{
		pool: make(map[string]entry),
	}
}

// Allocate allocates the shared database in the pool or returns already exists instance with the same DSN. If instance
// for such DSN already exists, it will be returned instead. Each request counts as reference until Close.
func (cp *connPool) Allocate(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
	logger log.Logger,
	create func(*config.SQL, resolver.ServiceResolver, log.Logger) (*sqlx.DB, error),
) (db *sqlx.DB, err error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, err
	}

	if entry, ok := cp.pool[dsn]; ok {
		if entry.closeTimer != nil {
			entry.closeTimer.Stop()
			entry.closeTimer = nil
		}
		entry.refCount++
		cp.pool[dsn] = entry
		return entry.db, nil
	}

	db, err = create(cfg, resolver, logger)
	if err != nil {
		return nil, err
	}

	cp.pool[dsn] = entry{db: db, refCount: 1}

	return db, nil
}

// Close virtual connection to database. Only closes for real once no references left.
func (cp *connPool) Close(cfg *config.SQL) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	dsn, err := buildDSN(cfg)
	if err != nil {
		return
	}

	e, ok := cp.pool[dsn]
	if !ok {
		// no such database
		return
	}

	e.refCount--
	if e.refCount == 0 {
		e.closeTimer = time.AfterFunc(closeDelay, func() {
			cp.closeIdle(dsn, e.db)
		})
		cp.pool[dsn] = e
		return
	}
	cp.pool[dsn] = e
}

func (cp *connPool) closeIdle(dsn string, db *sqlx.DB) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	e, ok := cp.pool[dsn]
	if !ok || e.db != db || e.refCount != 0 {
		return
	}

	_ = e.db.Close()
	delete(cp.pool, dsn)
}
