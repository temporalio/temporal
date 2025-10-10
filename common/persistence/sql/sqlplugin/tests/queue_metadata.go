package tests

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/shuffle"
)

const (
	testQueueMetadataEncoding = "random encoding"
)

var (
	testQueueMetadataData = []byte("random queue data")
)

type (
	queueMetadataSuite struct {
		suite.Suite

		store sqlplugin.QueueMetadata
	}
)

func NewQueueMetadataSuite(
	t *testing.T,
	store sqlplugin.QueueMetadata,
) *queueMetadataSuite {
	return &queueMetadataSuite{

		store: store,
	}
}

func (s *queueMetadataSuite) TearDownSuite() {

}

func (s *queueMetadataSuite) TearDownTest() {

}

func (s *queueMetadataSuite) TestInsert_Success() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *queueMetadataSuite) TestInsert_Fail_Duplicate() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	queueMetadata = s.newRandomQueueMetadataRow(queueType)
	_, err = s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *queueMetadataSuite) TestInsertSelect() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.QueueMetadataFilter{
		QueueType: queueType,
	}
	row, err := s.store.SelectFromQueueMetadata(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	row.QueueType = queueType
	require.Equal(s.T(), &queueMetadata, row)
}

func (s *queueMetadataSuite) TestInsertUpdate_Success() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	queueMetadata = s.newRandomQueueMetadataRow(queueType)
	result, err = s.store.UpdateQueueMetadata(newExecutionContext(), &queueMetadata)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *queueMetadataSuite) TestUpdate_Fail() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.UpdateQueueMetadata(newExecutionContext(), &queueMetadata)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))
}

func (s *queueMetadataSuite) TestInsertUpdateSelect() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	queueMetadata = s.newRandomQueueMetadataRow(queueType)
	result, err = s.store.UpdateQueueMetadata(newExecutionContext(), &queueMetadata)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.QueueMetadataFilter{
		QueueType: queueType,
	}
	row, err := s.store.SelectFromQueueMetadata(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), queueMetadata.DataEncoding, row.DataEncoding)
	require.Equal(s.T(), queueMetadata.Data, row.Data)
	require.Equal(s.T(), queueMetadata.Version+1, row.Version) // version increase by one after update
}

func (s *queueMetadataSuite) TestSelectReadLock() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	// NOTE: lock without transaction is equivalent to select
	//  this test only test the select functionality
	filter := sqlplugin.QueueMetadataFilter{
		QueueType: queueType,
	}
	row, err := s.store.LockQueueMetadata(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	row.QueueType = queueType
	require.Equal(s.T(), &queueMetadata, row)
}

func (s *queueMetadataSuite) newRandomQueueMetadataRow(
	queueType persistence.QueueType,
) sqlplugin.QueueMetadataRow {
	return sqlplugin.QueueMetadataRow{
		QueueType:    queueType,
		Data:         shuffle.Bytes(testQueueMetadataData),
		DataEncoding: testQueueMetadataEncoding,
	}
}
