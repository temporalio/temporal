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
	testQueueMessageEncoding = "random encoding"
)

var (
	testQueueMessageData = []byte("random queue data")
)

type (
	queueMessageSuite struct {
		suite.Suite

		store sqlplugin.QueueMessage
	}
)

func NewQueueMessageSuite(
	t *testing.T,
	store sqlplugin.QueueMessage,
) *queueMessageSuite {
	return &queueMessageSuite{

		store: store,
	}
}



func (s *queueMessageSuite) TearDownSuite() {

}



func (s *queueMessageSuite) TearDownTest() {

}

func (s *queueMessageSuite) TestInsert_Single_Success() {
	queueType := persistence.NamespaceReplicationQueueType
	messageID := rand.Int63()

	message := s.newRandomQueueMessageRow(queueType, messageID)
	result, err := s.store.InsertIntoMessages(newExecutionContext(), []sqlplugin.QueueMessageRow{message})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *queueMessageSuite) TestInsert_Multiple_Success() {
	queueType := persistence.NamespaceReplicationQueueType
	messageID := rand.Int63()

	message1 := s.newRandomQueueMessageRow(queueType, messageID)
	messageID++
	message2 := s.newRandomQueueMessageRow(queueType, messageID)
	result, err := s.store.InsertIntoMessages(newExecutionContext(), []sqlplugin.QueueMessageRow{message1, message2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))
}

func (s *queueMessageSuite) TestInsert_Single_Fail_Duplicate() {
	queueType := persistence.NamespaceReplicationQueueType
	messageID := rand.Int63()

	message := s.newRandomQueueMessageRow(queueType, messageID)
	result, err := s.store.InsertIntoMessages(newExecutionContext(), []sqlplugin.QueueMessageRow{message})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	message = s.newRandomQueueMessageRow(queueType, messageID)
	_, err = s.store.InsertIntoMessages(newExecutionContext(), []sqlplugin.QueueMessageRow{message})
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *queueMessageSuite) TestInsert_Multiple_Fail_Duplicate() {
	queueType := persistence.NamespaceReplicationQueueType
	messageID := rand.Int63()

	message1 := s.newRandomQueueMessageRow(queueType, messageID)
	messageID++
	message2 := s.newRandomQueueMessageRow(queueType, messageID)
	result, err := s.store.InsertIntoMessages(newExecutionContext(), []sqlplugin.QueueMessageRow{message1, message2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))

	message2 = s.newRandomQueueMessageRow(queueType, messageID)
	messageID++
	message3 := s.newRandomQueueMessageRow(queueType, messageID)
	_, err = s.store.InsertIntoMessages(newExecutionContext(), []sqlplugin.QueueMessageRow{message2, message3})
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *queueMessageSuite) TestInsertSelect() {
	queueType := persistence.NamespaceReplicationQueueType
	messageID := rand.Int63()

	message := s.newRandomQueueMessageRow(queueType, messageID)
	result, err := s.store.InsertIntoMessages(newExecutionContext(), []sqlplugin.QueueMessageRow{message})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.QueueMessagesFilter{
		QueueType: queueType,
		MessageID: messageID,
	}
	rows, err := s.store.SelectFromMessages(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].QueueType = queueType
	}
	require.Equal(s.T(), []sqlplugin.QueueMessageRow{message}, rows)
}

func (s *queueMessageSuite) TestInsertSelect_Multiple() {
	numMessages := 20

	queueType := persistence.NamespaceReplicationQueueType
	minMessageID := rand.Int63()
	messageID := minMessageID + 1
	maxMessageID := messageID + int64(numMessages)

	var messages []sqlplugin.QueueMessageRow
	for i := 0; i < numMessages; i++ {
		message := s.newRandomQueueMessageRow(queueType, messageID)
		messageID++
		messages = append(messages, message)
	}
	result, err := s.store.InsertIntoMessages(newExecutionContext(), messages)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numMessages, int(rowsAffected))

	filter := sqlplugin.QueueMessagesRangeFilter{
		QueueType:    queueType,
		MinMessageID: minMessageID,
		MaxMessageID: maxMessageID,
		PageSize:     numMessages,
	}
	rows, err := s.store.RangeSelectFromMessages(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].QueueType = queueType
	}
	require.Equal(s.T(), messages, rows)
}

func (s *queueMessageSuite) TestDeleteSelect_Single() {
	queueType := persistence.NamespaceReplicationQueueType
	messageID := rand.Int63()

	filter := sqlplugin.QueueMessagesFilter{
		QueueType: queueType,
		MessageID: messageID,
	}
	result, err := s.store.DeleteFromMessages(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	rows, err := s.store.SelectFromMessages(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].QueueType = queueType
	}
	require.Equal(s.T(), []sqlplugin.QueueMessageRow(nil), rows)
}

func (s *queueMessageSuite) TestDeleteSelect_Multiple() {
	pageSize := 100

	queueType := persistence.NamespaceReplicationQueueType
	minMessageID := rand.Int63()
	maxMessageID := minMessageID + int64(20)

	filter := sqlplugin.QueueMessagesRangeFilter{
		QueueType:    queueType,
		MinMessageID: minMessageID,
		MaxMessageID: maxMessageID,
		PageSize:     0,
	}
	result, err := s.store.RangeDeleteFromMessages(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	filter.PageSize = pageSize
	rows, err := s.store.RangeSelectFromMessages(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].QueueType = queueType
	}
	require.Equal(s.T(), []sqlplugin.QueueMessageRow(nil), rows)
}

func (s *queueMessageSuite) TestInsertDeleteSelect_Single() {
	queueType := persistence.NamespaceReplicationQueueType
	messageID := rand.Int63()

	message := s.newRandomQueueMessageRow(queueType, messageID)
	result, err := s.store.InsertIntoMessages(newExecutionContext(), []sqlplugin.QueueMessageRow{message})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.QueueMessagesFilter{
		QueueType: queueType,
		MessageID: messageID,
	}
	result, err = s.store.DeleteFromMessages(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	rows, err := s.store.SelectFromMessages(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].QueueType = queueType
	}
	require.Equal(s.T(), []sqlplugin.QueueMessageRow(nil), rows)
}

func (s *queueMessageSuite) TestInsertDeleteSelect_Multiple() {
	numMessages := 20
	pageSize := numMessages

	queueType := persistence.NamespaceReplicationQueueType
	minMessageID := rand.Int63()
	messageID := minMessageID + 1
	maxMessageID := messageID + int64(numMessages)

	var messages []sqlplugin.QueueMessageRow
	for i := 0; i < numMessages; i++ {
		message := s.newRandomQueueMessageRow(queueType, messageID)
		messageID++
		messages = append(messages, message)
	}
	result, err := s.store.InsertIntoMessages(newExecutionContext(), messages)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numMessages, int(rowsAffected))

	filter := sqlplugin.QueueMessagesRangeFilter{
		QueueType:    queueType,
		MinMessageID: minMessageID,
		MaxMessageID: maxMessageID,
		PageSize:     0,
	}
	result, err = s.store.RangeDeleteFromMessages(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numMessages, int(rowsAffected))

	filter.PageSize = pageSize
	rows, err := s.store.RangeSelectFromMessages(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].QueueType = queueType
	}
	require.Equal(s.T(), []sqlplugin.QueueMessageRow(nil), rows)
}

func (s *queueMessageSuite) newRandomQueueMessageRow(
	queueType persistence.QueueType,
	messageID int64,
) sqlplugin.QueueMessageRow {
	return sqlplugin.QueueMessageRow{
		QueueType:       queueType,
		MessageID:       messageID,
		MessagePayload:  shuffle.Bytes(testQueueMessageData),
		MessageEncoding: testQueueMessageEncoding,
	}
}
