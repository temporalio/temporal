package tests

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
)

type (
	historyExecutionSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.DB
	}
)

const (
	testHistoryExecutionWorkflowID = "random workflow ID"

	testHistoryExecutionEncoding      = "random encoding"
	testHistoryExecutionStateEncoding = "random encoding"
)

var (
	testHistoryExecutionData      = []byte("random history execution data")
	testHistoryExecutionStateData = []byte("random history execution state data")
)

func NewHistoryExecutionSuite(
	t *testing.T,
	store sqlplugin.DB,
) *historyExecutionSuite {
	return &historyExecutionSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyExecutionSuite) SetupSuite() {

}

func (s *historyExecutionSuite) TearDownSuite() {

}

func (s *historyExecutionSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyExecutionSuite) TearDownTest() {

}

func (s *historyExecutionSuite) TestInsert_Success() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	nextEventID := rand.Int63()
	lastWriteVersion := rand.Int63()

	execution := s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, nextEventID, lastWriteVersion)
	result, err := s.store.InsertIntoExecutions(newExecutionContext(), &execution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyExecutionSuite) TestInsert_Fail_Duplicate() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	nextEventID := rand.Int63()
	lastWriteVersion := rand.Int63()

	execution := s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, nextEventID, lastWriteVersion)
	result, err := s.store.InsertIntoExecutions(newExecutionContext(), &execution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	execution = s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, nextEventID, lastWriteVersion)
	_, err = s.store.InsertIntoExecutions(newExecutionContext(), &execution)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyExecutionSuite) TestInsertSelect() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	nextEventID := rand.Int63()
	lastWriteVersion := rand.Int63()

	execution := s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, nextEventID, lastWriteVersion)
	result, err := s.store.InsertIntoExecutions(newExecutionContext(), &execution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.ExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	row, err := s.store.SelectFromExecutions(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(&execution, row)
}

func (s *historyExecutionSuite) TestInsertUpdate_Success() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	nextEventID := rand.Int63()
	lastWriteVersion := rand.Int63()

	execution := s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, nextEventID, lastWriteVersion)
	result, err := s.store.InsertIntoExecutions(newExecutionContext(), &execution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	condition := execution.NextEventID
	execution = s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, rand.Int63(), rand.Int63())
	result, err = s.store.UpdateExecutions(newExecutionContext(), &sqlplugin.ExecutionsUpdate{
		ExecutionsRow: execution,
		Condition:     condition,
	})
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyExecutionSuite) TestUpdate_Fail() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	nextEventID := rand.Int63()
	lastWriteVersion := rand.Int63()

	execution := s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, nextEventID, lastWriteVersion)
	result, err := s.store.UpdateExecutions(newExecutionContext(), &sqlplugin.ExecutionsUpdate{
		ExecutionsRow: execution,
		Condition:     execution.NextEventID,
	})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))
}

func (s *historyExecutionSuite) TestInsertUpdateSelect() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	nextEventID := rand.Int63()
	lastWriteVersion := rand.Int63()

	execution := s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, nextEventID, lastWriteVersion)
	execution.DBRecordVersion = 5
	result, err := s.store.InsertIntoExecutions(newExecutionContext(), &execution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	execution = s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, rand.Int63(), rand.Int63())
	execution.DBRecordVersion = 6
	result, err = s.store.UpdateExecutions(newExecutionContext(), &sqlplugin.ExecutionsUpdate{
		ExecutionsRow: execution,
	})
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.ExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	row, err := s.store.SelectFromExecutions(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(&execution, row)
}

func (s *historyExecutionSuite) TestUpdate_Fail_Condition() {
	tests := []struct {
		name                string
		storedRecordVersion int64
		updateRecordVersion int64
		conditionOffset     int64
	}{
		{
			name:            "legacy next event ID",
			conditionOffset: 1,
		},
		{
			name:                "record version",
			storedRecordVersion: 5,
			updateRecordVersion: 7,
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			shardID := rand.Int31()
			namespaceID := primitives.NewUUID()
			workflowID := shuffle.String(testHistoryExecutionWorkflowID)
			runID := primitives.NewUUID()

			stored := s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, rand.Int63(), rand.Int63())
			stored.DBRecordVersion = test.storedRecordVersion
			result, err := s.store.InsertIntoExecutions(newExecutionContext(), &stored)
			s.NoError(err)
			rowsAffected, err := result.RowsAffected()
			s.NoError(err)
			s.Equal(1, int(rowsAffected))

			updated := s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, rand.Int63(), rand.Int63())
			updated.DBRecordVersion = test.updateRecordVersion
			result, err = s.store.UpdateExecutions(newExecutionContext(), &sqlplugin.ExecutionsUpdate{
				ExecutionsRow: updated,
				Condition:     stored.NextEventID + test.conditionOffset,
			})
			s.NoError(err)
			rowsAffected, err = result.RowsAffected()
			s.NoError(err)
			s.Equal(0, int(rowsAffected))

			row, err := s.store.SelectFromExecutions(newExecutionContext(), sqlplugin.ExecutionsFilter{
				ShardID:     shardID,
				NamespaceID: namespaceID,
				WorkflowID:  workflowID,
				RunID:       runID,
			})
			s.NoError(err)
			s.Equal(&stored, row)
		})
	}
}

func (s *historyExecutionSuite) TestDeleteSelect() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	filter := sqlplugin.ExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err := s.store.DeleteFromExecutions(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	_, err = s.store.SelectFromExecutions(newExecutionContext(), filter)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyExecutionSuite) TestInsertDeleteSelect() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	nextEventID := rand.Int63()
	lastWriteVersion := rand.Int63()

	execution := s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, nextEventID, lastWriteVersion)
	result, err := s.store.InsertIntoExecutions(newExecutionContext(), &execution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.ExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteFromExecutions(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	_, err = s.store.SelectFromExecutions(newExecutionContext(), filter)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyExecutionSuite) TestReadLock() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	nextEventID := rand.Int63()
	lastWriteVersion := rand.Int63()

	execution := s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, nextEventID, lastWriteVersion)
	result, err := s.store.InsertIntoExecutions(newExecutionContext(), &execution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	tx, err := s.store.BeginTx(newExecutionContext())
	s.NoError(err)
	filter := sqlplugin.ExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rowDBVersion, rowNextEventID, err := tx.ReadLockExecutions(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(execution.DBRecordVersion, rowDBVersion)
	s.Equal(execution.NextEventID, rowNextEventID)
	s.NoError(tx.Commit())
}

func (s *historyExecutionSuite) TestWriteLock() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	nextEventID := rand.Int63()
	lastWriteVersion := rand.Int63()

	execution := s.newRandomExecutionRow(shardID, namespaceID, workflowID, runID, nextEventID, lastWriteVersion)
	result, err := s.store.InsertIntoExecutions(newExecutionContext(), &execution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	tx, err := s.store.BeginTx(newExecutionContext())
	s.NoError(err)
	filter := sqlplugin.ExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rowDBVersion, rowNextEventID, err := tx.WriteLockExecutions(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(execution.DBRecordVersion, rowDBVersion)
	s.Equal(execution.NextEventID, rowNextEventID)
	s.NoError(tx.Commit())
}

func (s *historyExecutionSuite) newRandomExecutionRow(
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	nextEventID int64,
	lastWriteVersion int64,
) sqlplugin.ExecutionsRow {
	return sqlplugin.ExecutionsRow{
		ShardID:          shardID,
		NamespaceID:      namespaceID,
		WorkflowID:       workflowID,
		RunID:            runID,
		NextEventID:      nextEventID,
		LastWriteVersion: lastWriteVersion,
		Data:             shuffle.Bytes(testHistoryExecutionData),
		DataEncoding:     testHistoryExecutionEncoding,
		State:            shuffle.Bytes(testHistoryExecutionStateData),
		StateEncoding:    testHistoryExecutionStateEncoding,
	}
}
