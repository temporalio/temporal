# SQLite database lifetime leases

## Problem

Temporal creates several persistence wrappers for one SQLite database. Those
wrappers are transient: during a server restart or staged teardown, every wrapper
may close before a later wrapper opens. A wrapper reference count can therefore
legitimately reach zero while the database must remain alive.

Closing the underlying `sql.DB` whenever wrapper references reach zero breaks
that restart path, especially for a shared in-memory SQLite database: the schema
and contents disappear with the last connection. Never closing at zero avoids
that failure, but retains every per-test database for the life of the process.
With a fresh database per test, that becomes an unbounded resource leak.

The ownership signal must come from the cluster/server lifetime, not from its
temporary persistence wrappers.

## Design

The SQLite connection-pool entry tracks two independent counts:

- `references`: logical persistence wrappers currently using the database;
- `leases`: cluster/server owners that require the database to remain available.

Acquiring a database lease creates the pool entry if necessary, marks it as
managed, and increments `leases`. Closing a persistence wrapper decrements only
`references`. The pool closes and removes a managed database only when both
counts are zero.

Conceptually:

```text
cluster setup:       leases 0 -> 1
manager setup:       references 0 -> N
server stop/restart: references N -> 0 -> N   (database remains pinned)
manager teardown:    references N -> 0
cluster teardown:    leases 1 -> 0            (database closes and is removed)
```

`TestBase` owns the leases because its lifetime matches the test cluster. It
acquires one lease for each distinct SQL data store after the test database is
created and before persistence managers are constructed. Teardown closes all
managers and factories first, releases leases in reverse order, and only then
removes the SQLite file.

Plugins that do not support explicit database lifetime management return a no-op
lease. Existing SQLite callers that never acquire a lease keep the legacy
process-lifetime behavior; this avoids changing standalone server restart
semantics accidentally. Test clusters use managed entries and therefore receive
deterministic cleanup.

## Invariants

1. A managed pool entry remains open while either `references > 0` or
   `leases > 0`.
2. Wrapper churn cannot destroy a cluster-owned database.
3. Releasing the final lease does not close a database that still has wrapper
   references.
4. Closing the final reference and final lease closes the underlying `sql.DB`
   exactly once and removes the pool entry.
5. Lease close is idempotent.
6. Main and visibility stores receive distinct leases when they use distinct
   configurations, and one lease when they name the same store.
7. Partial setup failure releases any leases already acquired.

## Verification

Unit tests cover zero-reference restart preservation, lease/reference close
ordering, multiple leases, concurrent reference and lease churn, idempotent
close, failed acquisition, distinct main and visibility stores, repeated test
database teardown, and file removal after lease release.

The functional leak check additionally builds, runs and tears down 18 fresh
worker-enabled clusters. It passes with no unexpected goroutines and no
unexpected retained objects after removing the former
`database/sql.(*DB).connectionOpener` ignore. Named pre-existing gRPC and SDK
worker ignores remain outside the SQLite lifetime fix.

## Non-goals

- A lease does not pool or reuse a test cluster.
- A lease does not keep a persistence wrapper usable after that wrapper closes.
- A lease is not a transaction or a concurrency lock for SQL operations.
- This design does not require other SQL plugins to share SQLite's connection
  pool; their default lease is intentionally a no-op unless they need equivalent
  lifetime management.
