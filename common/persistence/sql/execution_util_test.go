package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
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
	// TestCurrentExecutionsEqualCoversEveryField already proves every field is observed by the
	// comparison; these cases cover what a generic per-field mutation can't: value equality
	// (not identity) for the byte-slice fields, and the microsecond-precision boundary and nil
	// transition specific to StartTime.
	t.Run("value equality, not identity", func(t *testing.T) {
		equal := row
		equal.NamespaceID = append(primitives.UUID(nil), row.NamespaceID...)
		equal.RunID = append(primitives.UUID(nil), row.RunID...)
		equal.Data = append([]byte(nil), row.Data...)
		equalStartTime := time.Unix(123, 456789999).UTC() // same microsecond, different nanos
		equal.StartTime = &equalStartTime

		require.True(t, currentExecutionsEqual(&row, &equal))
	})

	t.Run("start time change at microsecond precision is detected", func(t *testing.T) {
		changed := row
		value := row.StartTime.Add(time.Microsecond)
		changed.StartTime = &value
		require.False(t, currentExecutionsEqual(&row, &changed))
	})

	t.Run("missing start time", func(t *testing.T) {
		changed := row
		changed.StartTime = nil
		require.False(t, currentExecutionsEqual(&row, &changed))
	})
}

// mutateFieldForCoverage changes field to a different value based on its kind, generically enough
// to cover any field currentExecutionsEqual's field list might grow. Extend this switch, don't
// special-case the field, if a future field kind isn't handled.
func mutateFieldForCoverage(t *testing.T, field reflect.Value) {
	t.Helper()
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		field.SetInt(field.Int() + 1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		field.SetUint(field.Uint() + 1)
	case reflect.String:
		field.SetString(field.String() + "-other")
	case reflect.Bool:
		field.SetBool(!field.Bool())
	case reflect.Slice:
		if field.Type().Elem().Kind() != reflect.Uint8 {
			t.Fatalf("mutateFieldForCoverage: add support for slice element kind %s", field.Type().Elem().Kind())
		}
		mutated := append(append([]byte(nil), field.Bytes()...), 0xFF)
		field.Set(reflect.ValueOf(mutated).Convert(field.Type()))
	case reflect.Ptr:
		if field.Type().Elem() != reflect.TypeOf(time.Time{}) {
			t.Fatalf("mutateFieldForCoverage: add support for pointer element type %s", field.Type().Elem())
		}
		next := time.Now()
		if !field.IsNil() {
			// currentExecutionsEqual truncates start time to microseconds, so a smaller delta
			// would make this a false positive rather than a real coverage gap.
			next = field.Interface().(*time.Time).Add(time.Second)
		}
		field.Set(reflect.ValueOf(&next))
	default:
		t.Fatalf("mutateFieldForCoverage: add support for field kind %s", field.Kind())
	}
}

// TestCurrentExecutionsEqualCoversEveryField is a tripwire for CurrentExecutionsRow growing a
// field that currentExecutionsEqual forgets to compare: it mutates each field in turn via
// reflection and requires the comparison to notice, so a new field fails this test by default
// instead of silently making assertRunIDAndUpdateCurrentExecution skip a real change.
func TestCurrentExecutionsEqualCoversEveryField(t *testing.T) {
	startTime := time.Unix(123, 456789123).UTC()
	base := sqlplugin.CurrentExecutionsRow{
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

	typ := reflect.TypeOf(base)
	for i := 0; i < typ.NumField(); i++ {
		fieldName := typ.Field(i).Name
		t.Run(fieldName, func(t *testing.T) {
			mutated := base
			mutateFieldForCoverage(t, reflect.ValueOf(&mutated).Elem().Field(i))
			require.False(t, currentExecutionsEqual(&base, &mutated),
				"currentExecutionsEqual did not notice a change to CurrentExecutionsRow.%s; add it to the comparison",
				fieldName)
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
