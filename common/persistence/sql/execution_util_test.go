package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

type currentExecutionsTx struct {
	sqlplugin.Tx
	current *sqlplugin.CurrentExecutionsRow
	updates int
}

func (t *currentExecutionsTx) LockCurrentExecutions(
	_ context.Context,
	_ sqlplugin.CurrentExecutionsFilter,
) (*sqlplugin.CurrentExecutionsRow, error) {
	return t.current, nil
}

func (t *currentExecutionsTx) UpdateCurrentExecutions(
	_ context.Context,
	_ *sqlplugin.CurrentExecutionsRow,
) (sql.Result, error) {
	t.updates++
	return driver.RowsAffected(1), nil
}

func TestCurrentExecutionsEqual(t *testing.T) {
	startTime := time.Unix(123, 456789123).UTC()
	row := sqlplugin.CurrentExecutionsRow{
		ShardID:          1,
		NamespaceID:      primitives.NewUUID(),
		WorkflowID:       "workflow-id",
		RunID:            primitives.NewUUID(),
		ArchetypeID:      chasm.WorkflowArchetypeID,
		CreateRequestID:  "request-id",
		StartTime:        &startTime,
		LastWriteVersion: 2,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		Data:             []byte("data"),
		DataEncoding:     "proto3",
	}
	equal := row
	equal.NamespaceID = append(primitives.UUID(nil), row.NamespaceID...)
	equal.RunID = append(primitives.UUID(nil), row.RunID...)
	equal.Data = append([]byte(nil), row.Data...)
	equalStartTime := time.Unix(123, 456789999).UTC()
	equal.StartTime = &equalStartTime

	require.True(t, currentExecutionsEqual(&row, &equal))

	tests := []struct {
		name   string
		mutate func(*sqlplugin.CurrentExecutionsRow)
	}{
		{name: "shard ID", mutate: func(r *sqlplugin.CurrentExecutionsRow) { r.ShardID++ }},
		{name: "namespace ID", mutate: func(r *sqlplugin.CurrentExecutionsRow) { r.NamespaceID = primitives.NewUUID() }},
		{name: "workflow ID", mutate: func(r *sqlplugin.CurrentExecutionsRow) { r.WorkflowID += "-other" }},
		{name: "run ID", mutate: func(r *sqlplugin.CurrentExecutionsRow) { r.RunID = primitives.NewUUID() }},
		{name: "archetype ID", mutate: func(r *sqlplugin.CurrentExecutionsRow) { r.ArchetypeID++ }},
		{name: "create request ID", mutate: func(r *sqlplugin.CurrentExecutionsRow) { r.CreateRequestID += "-other" }},
		{name: "start time", mutate: func(r *sqlplugin.CurrentExecutionsRow) {
			value := r.StartTime.Add(time.Microsecond)
			r.StartTime = &value
		}},
		{name: "missing start time", mutate: func(r *sqlplugin.CurrentExecutionsRow) { r.StartTime = nil }},
		{name: "last write version", mutate: func(r *sqlplugin.CurrentExecutionsRow) { r.LastWriteVersion++ }},
		{name: "state", mutate: func(r *sqlplugin.CurrentExecutionsRow) { r.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED }},
		{name: "status", mutate: func(r *sqlplugin.CurrentExecutionsRow) { r.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED }},
		{name: "data", mutate: func(r *sqlplugin.CurrentExecutionsRow) { r.Data = []byte("other data") }},
		{name: "data encoding", mutate: func(r *sqlplugin.CurrentExecutionsRow) { r.DataEncoding = "json" }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := row
			test.mutate(&changed)
			require.False(t, currentExecutionsEqual(&row, &changed))
		})
	}
}

func TestAssertRunIDAndUpdateCurrentExecutionSkipsUnchangedRow(t *testing.T) {
	startTime := time.Unix(123, 456789123).UTC()
	current := sqlplugin.CurrentExecutionsRow{
		ShardID:          1,
		NamespaceID:      primitives.NewUUID(),
		WorkflowID:       "workflow-id",
		RunID:            primitives.NewUUID(),
		ArchetypeID:      chasm.WorkflowArchetypeID,
		CreateRequestID:  "request-id",
		StartTime:        &startTime,
		LastWriteVersion: 2,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		Data:             []byte("data"),
		DataEncoding:     "proto3",
	}
	tx := &currentExecutionsTx{current: &current}
	serializer := serialization.NewSerializer()

	require.NoError(t, assertRunIDAndUpdateCurrentExecution(
		context.Background(),
		tx,
		current,
		current.RunID,
		serializer,
	))
	require.Zero(t, tx.updates)

	changed := current
	changed.LastWriteVersion++
	require.NoError(t, assertRunIDAndUpdateCurrentExecution(
		context.Background(),
		tx,
		changed,
		current.RunID,
		serializer,
	))
	require.Equal(t, 1, tx.updates)
}
