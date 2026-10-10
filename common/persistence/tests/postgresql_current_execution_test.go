package tests

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/testing/await"
)

func (p *PostgreSQLSuite) newCurrentExecutionTestDB(t *testing.T) sqlplugin.DB {
	cfg := NewPostgreSQLConfig(p.pluginName, p.connectAttrs)
	SetupPostgreSQLDatabase(t, cfg)
	t.Cleanup(func() { TearDownPostgreSQLDatabase(t, cfg) })
	SetupPostgreSQLSchema(t, cfg)
	db, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func (p *PostgreSQLSuite) TestPostgreSQLCurrentExecutionJoinPlan() {
	if p.pluginName != "postgres12_pgx" || p.connectAttrs["default_query_exec_mode"] == "simple_protocol" {
		p.T().Skip("requires pgx prepared statement caching")
	}
	db := p.newCurrentExecutionTestDB(p.T())
	conn := db.(sqlplugin.Conn)
	ctx := p.T().Context()
	// Many single-use IDs make the average run count misleading for a recurring ID.
	for _, query := range []string{
		`INSERT INTO executions
		SELECT i%512, decode(repeat('01',16),'hex'), 'single-use-'||i,
		decode(lpad(to_hex(i),32,'0'),'hex'), 2, 1,
		decode(repeat(md5(i::text),32),'hex'), 'Proto3', decode('01','hex'), 'Proto3', 1
		FROM generate_series(1,100000) AS i`,
		`INSERT INTO executions
		SELECT 7, decode(repeat('01',16),'hex'), 'recurring-workflow',
		decode(lpad(to_hex(i),32,'0'),'hex'), 2, 1,
		decode(repeat(md5(i::text),32),'hex'), 'Proto3', decode('01','hex'), 'Proto3', 1
		FROM generate_series(1,30000) AS i`,
		`INSERT INTO current_executions
		(shard_id, namespace_id, workflow_id, run_id, create_request_id, state, status,
		start_time, last_write_version, data, data_encoding)
		SELECT shard_id, namespace_id, workflow_id, run_id, 'request', 2, 1,
		now(), last_write_version, state, state_encoding FROM executions
		WHERE workflow_id LIKE 'single-use-%'
		OR run_id=decode(lpad(to_hex(30000),32,'0'),'hex')`,
		`INSERT INTO current_chasm_executions
		(shard_id, namespace_id, business_id, archetype_id, run_id, create_request_id, state,
		status, start_time, last_write_version, data, data_encoding)
		SELECT shard_id, namespace_id, workflow_id, 2, run_id, create_request_id, state,
		status, start_time, last_write_version, data, data_encoding FROM current_executions`,
		`ANALYZE executions`,
		`ANALYZE current_executions`,
		`ANALYZE current_chasm_executions`,
	} {
		_, err := conn.ExecContext(ctx, query)
		p.Require().NoError(err)
	}

	for _, table := range []struct {
		name      string
		archetype chasm.ArchetypeID
	}{
		{"current_executions", chasm.WorkflowArchetypeID},
		{"current_chasm_executions", 2},
	} {
		for _, mode := range []string{"force_generic_plan", "force_custom_plan"} {
			for _, workflow := range []string{"recurring-workflow", "single-use-7"} {
				p.Run(table.name+"/"+mode+"/"+workflow, func() {
					t := p.T()
					tx, err := db.BeginTx(ctx)
					require.NoError(t, err)
					defer func() { require.NoError(t, tx.Rollback()) }()
					conn := tx.(sqlplugin.Conn)
					_, err = conn.ExecContext(ctx, "SET LOCAL plan_cache_mode="+mode)
					require.NoError(t, err)
					rows, err := tx.LockCurrentExecutionsJoinExecutions(ctx, sqlplugin.CurrentExecutionsFilter{
						ShardID: 7, NamespaceID: primitives.MustParseUUID("01010101-0101-0101-0101-010101010101"),
						WorkflowID: workflow, ArchetypeID: table.archetype,
					})
					require.NoError(t, err)
					require.Len(t, rows, 1)

					// Explain the actual plugin query on the same connection, not a copy of its SQL.
					var statement string
					err = conn.GetContext(ctx, &statement, `SELECT name FROM pg_prepared_statements
					WHERE statement LIKE '%INNER JOIN executions e%' AND statement LIKE $1`, "%FROM "+table.name+"%")
					require.NoError(t, err)
					args := "7,decode(repeat('01',16),'hex'),'" + workflow + "'"
					if table.archetype != chasm.WorkflowArchetypeID {
						args += ",2"
					}
					var rawPlan string
					err = conn.GetContext(ctx, &rawPlan, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE "+pq.QuoteIdentifier(statement)+"("+args+")")
					require.NoError(t, err)
					var plan []struct{ Plan currentExecutionQueryPlan }
					require.NoError(t, json.Unmarshal([]byte(rawPlan), &plan))
					require.Len(t, plan, 1)
					execution := plan[0].Plan.executionScan()
					require.NotNil(t, execution, rawPlan)
					t.Logf("executions rows=%d; shared buffers=%d", execution.ActualRows, plan[0].Plan.SharedHitBlocks+plan[0].Plan.SharedReadBlocks)
					require.Contains(t, execution.IndexCond, "run_id", rawPlan)
					require.Equal(t, 1, execution.ActualRows, rawPlan)
				})
			}
		}
	}
}

type currentExecutionQueryPlan struct {
	RelationName     string `json:"Relation Name"`
	IndexCond        string `json:"Index Cond"`
	ActualRows       int    `json:"Actual Rows"`
	SharedHitBlocks  int    `json:"Shared Hit Blocks"`
	SharedReadBlocks int    `json:"Shared Read Blocks"`
	Plans            []currentExecutionQueryPlan
}

func (p *currentExecutionQueryPlan) executionScan() *currentExecutionQueryPlan {
	if p.RelationName == "executions" {
		return p
	}
	for i := range p.Plans {
		if found := p.Plans[i].executionScan(); found != nil {
			return found
		}
	}
	return nil
}

func seedCurrentExecutionJoin(t *testing.T, db sqlplugin.DB, archetype chasm.ArchetypeID) (sqlplugin.CurrentExecutionsFilter, sqlplugin.CurrentExecutionsRow, sqlplugin.ExecutionsRow) {
	t.Helper()
	ctx := t.Context()
	execution := sqlplugin.ExecutionsRow{
		ShardID: 7, NamespaceID: primitives.NewUUID(), WorkflowID: "current-run-test",
		RunID: primitives.NewUUID(), NextEventID: 2, LastWriteVersion: 11,
		Data: []byte("execution"), DataEncoding: "Proto3", State: []byte("state"), StateEncoding: "Proto3",
	}
	_, err := db.InsertIntoExecutions(ctx, &execution)
	require.NoError(t, err)
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	current := sqlplugin.CurrentExecutionsRow{
		ShardID: execution.ShardID, NamespaceID: execution.NamespaceID, WorkflowID: execution.WorkflowID,
		RunID: execution.RunID, ArchetypeID: archetype, CreateRequestID: "request", StartTime: &start,
		State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		LastWriteVersion: 99, Data: []byte("current"), DataEncoding: "Proto3",
	}
	_, err = db.InsertIntoCurrentExecutions(ctx, &current)
	require.NoError(t, err)
	current.LastWriteVersion = execution.LastWriteVersion
	execution.RunID = primitives.NewUUID()
	execution.LastWriteVersion = 22
	_, err = db.InsertIntoExecutions(ctx, &execution)
	require.NoError(t, err)
	return sqlplugin.CurrentExecutionsFilter{
		ShardID: current.ShardID, NamespaceID: current.NamespaceID, WorkflowID: current.WorkflowID, ArchetypeID: archetype,
	}, current, execution
}

func currentExecutionTestTx(ctx context.Context, t *testing.T, db sqlplugin.DB) sqlplugin.Tx {
	t.Helper()
	tx, err := db.BeginTx(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil {
			require.ErrorIs(t, err, stdsql.ErrTxDone)
		}
	})
	return tx
}

func currentExecutionBackendPID(ctx context.Context, t *testing.T, tx sqlplugin.Tx) int {
	t.Helper()
	var pid int
	require.NoError(t, tx.(sqlplugin.Conn).GetContext(ctx, &pid, "SELECT pg_backend_pid()"))
	return pid
}

func waitForCurrentExecutionLock(ctx context.Context, t *testing.T, db sqlplugin.DB, waiter, holder int) {
	t.Helper()
	await.Require(ctx, t, func(c *await.T) {
		var blocked bool
		err := db.(sqlplugin.Conn).GetContext(c.Context(), &blocked, "SELECT $1=ANY(pg_blocking_pids($2))", holder, waiter)
		require.NoError(c, err)
		require.True(c, blocked)
	}, 5*time.Second, 10*time.Millisecond)
}

func requireCurrentExecutionRows(t *testing.T, expected, actual []sqlplugin.CurrentExecutionsRow) {
	t.Helper()
	for i := range actual {
		if actual[i].StartTime != nil {
			start := actual[i].StartTime.UTC()
			actual[i].StartTime = &start
		}
	}
	require.Equal(t, expected, actual)
}

func (p *PostgreSQLSuite) TestPostgreSQLCurrentExecutionJoinRows() {
	db := p.newCurrentExecutionTestDB(p.T())
	for _, archetype := range []chasm.ArchetypeID{chasm.WorkflowArchetypeID, 2} {
		p.Run(fmt.Sprint(archetype), func() {
			t := p.T()
			filter, expected, other := seedCurrentExecutionJoin(t, db, archetype)
			rows, err := db.LockCurrentExecutionsJoinExecutions(t.Context(), filter)
			require.NoError(t, err)
			requireCurrentExecutionRows(t, []sqlplugin.CurrentExecutionsRow{expected}, rows)
			for _, missing := range []sqlplugin.CurrentExecutionsFilter{
				{ShardID: filter.ShardID + 1, NamespaceID: filter.NamespaceID, WorkflowID: filter.WorkflowID, ArchetypeID: archetype},
				{ShardID: filter.ShardID, NamespaceID: primitives.NewUUID(), WorkflowID: filter.WorkflowID, ArchetypeID: archetype},
				{ShardID: filter.ShardID, NamespaceID: filter.NamespaceID, WorkflowID: "missing", ArchetypeID: archetype},
				{ShardID: filter.ShardID, NamespaceID: filter.NamespaceID, WorkflowID: filter.WorkflowID, ArchetypeID: 3},
			} {
				rows, err := db.LockCurrentExecutionsJoinExecutions(t.Context(), missing)
				require.NoError(t, err)
				require.Empty(t, rows)
			}
			if archetype != chasm.WorkflowArchetypeID {
				otherCurrent := expected
				otherCurrent.ArchetypeID = 3
				otherCurrent.RunID = other.RunID
				_, err = db.InsertIntoCurrentExecutions(t.Context(), &otherCurrent)
				require.NoError(t, err)
				rows, err = db.LockCurrentExecutionsJoinExecutions(t.Context(), filter)
				require.NoError(t, err)
				requireCurrentExecutionRows(t, []sqlplugin.CurrentExecutionsRow{expected}, rows)
			}
			_, err = db.DeleteFromExecutions(t.Context(), sqlplugin.ExecutionsFilter{
				ShardID: filter.ShardID, NamespaceID: filter.NamespaceID, WorkflowID: filter.WorkflowID, RunID: expected.RunID,
			})
			require.NoError(t, err)
			rows, err = db.LockCurrentExecutionsJoinExecutions(t.Context(), filter)
			require.NoError(t, err)
			require.Empty(t, rows, "another run must not substitute for the missing current execution")

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			t.Cleanup(cancel)
			holder := currentExecutionTestTx(ctx, t, db)
			holderPID := currentExecutionBackendPID(ctx, t, holder)
			rows, err = holder.LockCurrentExecutionsJoinExecutions(ctx, filter)
			require.NoError(t, err)
			require.Empty(t, rows)
			waiter := currentExecutionTestTx(ctx, t, db)
			waiterPID := currentExecutionBackendPID(ctx, t, waiter)
			done := make(chan error, 1)
			go func() {
				_, err := waiter.LockCurrentExecutions(ctx, filter)
				done <- err
			}()
			// The current row remains locked even when its execution is missing.
			waitForCurrentExecutionLock(ctx, t, db, waiterPID, holderPID)
			require.NoError(t, holder.Rollback())
			require.NoError(t, <-done)
		})
	}
}

func (p *PostgreSQLSuite) TestPostgreSQLCurrentExecutionJoinLocks() {
	db := p.newCurrentExecutionTestDB(p.T())
	for _, archetype := range []chasm.ArchetypeID{chasm.WorkflowArchetypeID, 2} {
		for _, commit := range []bool{false, true} {
			p.Run(fmt.Sprintf("%d/commit=%t", archetype, commit), func() {
				t := p.T()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				t.Cleanup(cancel)
				filter, expected, other := seedCurrentExecutionJoin(t, db, archetype)
				holder := currentExecutionTestTx(ctx, t, db)
				holderPID := currentExecutionBackendPID(ctx, t, holder)
				rows, err := holder.LockCurrentExecutionsJoinExecutions(ctx, filter)
				require.NoError(t, err)
				requireCurrentExecutionRows(t, []sqlplugin.CurrentExecutionsRow{expected}, rows)
				currentWaiter := currentExecutionTestTx(ctx, t, db)
				executionWaiter := currentExecutionTestTx(ctx, t, db)
				currentPID := currentExecutionBackendPID(ctx, t, currentWaiter)
				executionPID := currentExecutionBackendPID(ctx, t, executionWaiter)
				currentDone, executionDone := make(chan error, 1), make(chan error, 1)
				go func() {
					_, err := currentWaiter.LockCurrentExecutions(ctx, filter)
					currentDone <- err
				}()
				go func() {
					_, _, err := executionWaiter.WriteLockExecutions(ctx, sqlplugin.ExecutionsFilter{
						ShardID: filter.ShardID, NamespaceID: filter.NamespaceID, WorkflowID: filter.WorkflowID, RunID: expected.RunID,
					})
					executionDone <- err
				}()
				waitForCurrentExecutionLock(ctx, t, db, currentPID, holderPID)
				waitForCurrentExecutionLock(ctx, t, db, executionPID, holderPID)
				unrelated := currentExecutionTestTx(ctx, t, db)
				_, _, err = unrelated.WriteLockExecutions(ctx, sqlplugin.ExecutionsFilter{
					ShardID: filter.ShardID, NamespaceID: filter.NamespaceID, WorkflowID: filter.WorkflowID, RunID: other.RunID,
				})
				require.NoError(t, err, "other runs must remain writable")
				if commit {
					require.NoError(t, holder.Commit())
				} else {
					require.NoError(t, holder.Rollback())
				}
				require.NoError(t, <-currentDone)
				require.NoError(t, <-executionDone)
			})
		}
	}
}

func (p *PostgreSQLSuite) TestPostgreSQLCurrentExecutionJoinConcurrentChange() {
	db := p.newCurrentExecutionTestDB(p.T())
	for _, archetype := range []chasm.ArchetypeID{chasm.WorkflowArchetypeID, 2} {
		for _, change := range []string{"existing_run", "new_run", "rollback", "delete_current", "update_execution", "delete_execution"} {
			p.Run(fmt.Sprintf("%d/%s", archetype, change), func() {
				t := p.T()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				t.Cleanup(cancel)
				filter, current, other := seedCurrentExecutionJoin(t, db, archetype)
				executionFilter := sqlplugin.ExecutionsFilter{
					ShardID: filter.ShardID, NamespaceID: filter.NamespaceID, WorkflowID: filter.WorkflowID, RunID: current.RunID,
				}
				if change == "new_run" {
					otherFilter := executionFilter
					otherFilter.RunID = other.RunID
					_, err := db.DeleteFromExecutions(ctx, otherFilter)
					require.NoError(t, err)
				}
				writer := currentExecutionTestTx(ctx, t, db)
				writerPID := currentExecutionBackendPID(ctx, t, writer)
				expected := []sqlplugin.CurrentExecutionsRow{current}
				switch change {
				case "existing_run", "new_run", "rollback":
					current.RunID = other.RunID
					current.LastWriteVersion = other.LastWriteVersion
					_, err := writer.UpdateCurrentExecutions(ctx, &current)
					require.NoError(t, err)
					switch change {
					case "existing_run":
						expected = []sqlplugin.CurrentExecutionsRow{current}
					case "new_run":
						_, err := writer.InsertIntoExecutions(ctx, &other)
						require.NoError(t, err)
						// Waiting for the current row does not refresh the statement snapshot.
						expected = nil
					default:
						// A rollback leaves the original run current.
					}
				case "delete_current":
					filter.RunID = current.RunID
					_, err := writer.DeleteFromCurrentExecutions(ctx, filter)
					require.NoError(t, err)
					expected = nil
				case "update_execution":
					row, err := writer.SelectFromExecutions(ctx, executionFilter)
					require.NoError(t, err)
					row.LastWriteVersion = 33
					_, err = writer.UpdateExecutions(ctx, row)
					require.NoError(t, err)
					expected[0].LastWriteVersion = 33
				case "delete_execution":
					_, err := writer.DeleteFromExecutions(ctx, executionFilter)
					require.NoError(t, err)
					expected = nil
				default:
					t.Fatalf("unknown change %q", change)
				}
				reader := currentExecutionTestTx(ctx, t, db)
				readerPID := currentExecutionBackendPID(ctx, t, reader)
				type result struct {
					rows []sqlplugin.CurrentExecutionsRow
					err  error
				}
				done := make(chan result, 1)
				go func() {
					rows, err := reader.LockCurrentExecutionsJoinExecutions(ctx, filter)
					done <- result{rows, err}
				}()
				waitForCurrentExecutionLock(ctx, t, db, readerPID, writerPID)
				if change == "rollback" {
					require.NoError(t, writer.Rollback())
				} else {
					require.NoError(t, writer.Commit())
				}
				got := <-done
				require.NoError(t, got.err)
				requireCurrentExecutionRows(t, expected, got.rows)
			})
		}
	}
}
