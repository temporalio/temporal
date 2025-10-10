package tests

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/common/util"
)

const (
	testNamespaceEncoding = "random encoding"
)

var (
	testNamespaceName = "random namespace"
	testNamespaceData = []byte("random namespace data")
)

type (
	namespaceSuite struct {
		suite.Suite

		store sqlplugin.DB
	}
)

func NewNamespaceSuite(
	t *testing.T,
	store sqlplugin.DB,
) *namespaceSuite {
	return &namespaceSuite{

		store: store,
	}
}



func (s *namespaceSuite) TearDownSuite() {

}



func (s *namespaceSuite) TearDownTest() {

}

func (s *namespaceSuite) TestInsert_Success() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *namespaceSuite) TestInsert_Fail_Duplicate() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	namespace = s.newRandomNamespaceRow(id, name, notificationVersion)
	_, err = s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation

	namespace = s.newRandomNamespaceRow(id, shuffle.String(testNamespaceName), notificationVersion)
	_, err = s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation

	namespace = s.newRandomNamespaceRow(primitives.NewUUID(), name, notificationVersion)
	_, err = s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *namespaceSuite) TestInsertSelect() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.NamespaceFilter{
		ID: &id,
	}
	rows, err := s.store.SelectFromNamespace(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.NamespaceRow{namespace}, rows)

	filter = sqlplugin.NamespaceFilter{
		Name: &name,
	}
	rows, err = s.store.SelectFromNamespace(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.NamespaceRow{namespace}, rows)
}

func (s *namespaceSuite) TestInsertUpdate_Success() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	namespace = s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err = s.store.UpdateNamespace(newExecutionContext(), &namespace)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *namespaceSuite) TestUpdate_Fail() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.UpdateNamespace(newExecutionContext(), &namespace)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))
}

func (s *namespaceSuite) TestInsertUpdateSelect() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	namespace = s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err = s.store.UpdateNamespace(newExecutionContext(), &namespace)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.NamespaceFilter{
		ID: &id,
	}
	rows, err := s.store.SelectFromNamespace(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.NamespaceRow{namespace}, rows)

	filter = sqlplugin.NamespaceFilter{
		Name: &name,
	}
	rows, err = s.store.SelectFromNamespace(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.NamespaceRow{namespace}, rows)
}

func (s *namespaceSuite) TestInsertDeleteSelect_ID() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.NamespaceFilter{
		ID: &id,
	}
	result, err = s.store.DeleteFromNamespace(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter = sqlplugin.NamespaceFilter{
		ID: &id,
	}
	_, err = s.store.SelectFromNamespace(newExecutionContext(), filter)
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation

	filter = sqlplugin.NamespaceFilter{
		Name: &name,
	}
	_, err = s.store.SelectFromNamespace(newExecutionContext(), filter)
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *namespaceSuite) TestInsertDeleteSelect_Name() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.NamespaceFilter{
		Name: &name,
	}
	result, err = s.store.DeleteFromNamespace(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter = sqlplugin.NamespaceFilter{
		ID: &id,
	}
	_, err = s.store.SelectFromNamespace(newExecutionContext(), filter)
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation

	filter = sqlplugin.NamespaceFilter{
		Name: &name,
	}
	_, err = s.store.SelectFromNamespace(newExecutionContext(), filter)
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *namespaceSuite) TestInsertSelect_Pagination() {
	// cleanup the namespace for pagination test
	rowsPerPage, err := s.store.SelectFromNamespace(newExecutionContext(), sqlplugin.NamespaceFilter{
		GreaterThanID: nil,
		PageSize:      util.Ptr(1000000),
	})
	switch err {
	case nil:
		for _, row := range rowsPerPage {
			_, err := s.store.DeleteFromNamespace(newExecutionContext(), sqlplugin.NamespaceFilter{
				ID: &row.ID,
			})
			require.NoError(s.T(), err)
		}
	case sql.ErrNoRows:
		// noop
	default:
		require.NoError(s.T(), err)
	}

	namespaces := map[string]*sqlplugin.NamespaceRow{}
	numNamespace := 2
	numNamespacePerPage := 1

	for i := 0; i < numNamespace; i++ {
		id := primitives.NewUUID()
		name := shuffle.String(testNamespaceName)
		notificationVersion := int64(1)

		namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
		result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
		require.NoError(s.T(), err)
		rowsAffected, err := result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), 1, int(rowsAffected))

		namespaces[namespace.ID.String()] = &namespace
	}

	rows := map[string]*sqlplugin.NamespaceRow{}
	filter := sqlplugin.NamespaceFilter{
		GreaterThanID: nil,
		PageSize:      util.Ptr(numNamespacePerPage),
	}
	for doContinue := true; doContinue; doContinue = filter.GreaterThanID != nil {
		rowsPerPage, err := s.store.SelectFromNamespace(newExecutionContext(), filter)
		switch err {
		case nil:
			for _, row := range rowsPerPage {
				rows[row.ID.String()] = &row
			}
			length := len(rowsPerPage)
			if length == 0 {
				filter.GreaterThanID = nil
			} else {
				filter.GreaterThanID = &rowsPerPage[len(rowsPerPage)-1].ID
			}

		case sql.ErrNoRows:
			filter.GreaterThanID = nil

		default:
			require.NoError(s.T(), err)
		}
	}
	require.Equal(s.T(), namespaces, rows)
}

func (s *namespaceSuite) TestSelectLockMetadata() {
	row, err := s.store.SelectFromNamespaceMetadata(newExecutionContext())
	require.NoError(s.T(), err)

	tx, err := s.store.BeginTx(newExecutionContext())
	require.NoError(s.T(), err)
	metadata, err := tx.LockNamespaceMetadata(newExecutionContext())
	require.NoError(s.T(), err)
	require.Equal(s.T(), row, metadata)
	require.NoError(s.T(), tx.Commit())
}

func (s *namespaceSuite) TestSelectUpdateSelectMetadata_Success() {
	row, err := s.store.SelectFromNamespaceMetadata(newExecutionContext())
	require.NoError(s.T(), err)
	originalVersion := row.NotificationVersion

	result, err := s.store.UpdateNamespaceMetadata(newExecutionContext(), row)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	row, err = s.store.SelectFromNamespaceMetadata(newExecutionContext())
	require.NoError(s.T(), err)
	require.Equal(s.T(), originalVersion+1, row.NotificationVersion)
}

func (s *namespaceSuite) TestSelectUpdateSelectMetadata_Fail() {
	row, err := s.store.SelectFromNamespaceMetadata(newExecutionContext())
	require.NoError(s.T(), err)
	originalVersion := row.NotificationVersion

	namespaceMetadata := s.newRandomNamespaceMetadataRow(row.NotificationVersion + 1000)
	result, err := s.store.UpdateNamespaceMetadata(newExecutionContext(), &namespaceMetadata)
	require.NoError(s.T(), err) // TODO persistence layer should do proper error translation
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	row, err = s.store.SelectFromNamespaceMetadata(newExecutionContext())
	require.NoError(s.T(), err)
	require.Equal(s.T(), originalVersion, row.NotificationVersion)
}

func (s *namespaceSuite) newRandomNamespaceRow(
	id primitives.UUID,
	name string,
	notificationVersion int64,
) sqlplugin.NamespaceRow {
	return sqlplugin.NamespaceRow{
		ID:                  id,
		Name:                name,
		Data:                shuffle.Bytes(testNamespaceData),
		DataEncoding:        testNamespaceEncoding,
		IsGlobal:            true,
		NotificationVersion: notificationVersion,
	}
}

func (s *namespaceSuite) newRandomNamespaceMetadataRow(
	notificationVersion int64,
) sqlplugin.NamespaceMetadataRow {
	return sqlplugin.NamespaceMetadataRow{
		NotificationVersion: notificationVersion,
	}
}
