// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
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
		*require.Assertions

		store sqlplugin.Namespace
	}
)

func newNamespaceSuite(
	t *testing.T,
	store sqlplugin.Namespace,
) *namespaceSuite {
	return &namespaceSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *namespaceSuite) SetupSuite() {

}

func (s *namespaceSuite) TearDownSuite() {

}

func (s *namespaceSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *namespaceSuite) TearDownTest() {

}

func (s *namespaceSuite) TestInsert_Success() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *namespaceSuite) TestInsert_Fail_Duplicate() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	namespace = s.newRandomNamespaceRow(id, name, notificationVersion)
	_, err = s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	s.Error(err) // TODO persistence layer should do proper error translation

	namespace = s.newRandomNamespaceRow(id, shuffle.String(testNamespaceName), notificationVersion)
	_, err = s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	s.Error(err) // TODO persistence layer should do proper error translation

	namespace = s.newRandomNamespaceRow(primitives.NewUUID(), name, notificationVersion)
	_, err = s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *namespaceSuite) TestInsertSelect() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.NamespaceFilter{
		ID: &id,
	}
	rows, err := s.store.SelectFromNamespace(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.NamespaceRow{namespace}, rows)

	filter = sqlplugin.NamespaceFilter{
		Name: &name,
	}
	rows, err = s.store.SelectFromNamespace(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.NamespaceRow{namespace}, rows)
}

func (s *namespaceSuite) TestInsertUpdate_Success() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	namespace = s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err = s.store.UpdateNamespace(newExecutionContext(), &namespace)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *namespaceSuite) TestUpdate_Fail() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.UpdateNamespace(newExecutionContext(), &namespace)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))
}

func (s *namespaceSuite) TestInsertUpdateSelect() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	namespace = s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err = s.store.UpdateNamespace(newExecutionContext(), &namespace)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.NamespaceFilter{
		ID: &id,
	}
	rows, err := s.store.SelectFromNamespace(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.NamespaceRow{namespace}, rows)

	filter = sqlplugin.NamespaceFilter{
		Name: &name,
	}
	rows, err = s.store.SelectFromNamespace(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.NamespaceRow{namespace}, rows)
}

func (s *namespaceSuite) TestInsertDeleteSelect_ID() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.NamespaceFilter{
		ID: &id,
	}
	result, err = s.store.DeleteFromNamespace(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter = sqlplugin.NamespaceFilter{
		ID: &id,
	}
	_, err = s.store.SelectFromNamespace(newExecutionContext(), filter)
	s.Error(err) // TODO persistence layer should do proper error translation

	filter = sqlplugin.NamespaceFilter{
		Name: &name,
	}
	_, err = s.store.SelectFromNamespace(newExecutionContext(), filter)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *namespaceSuite) TestInsertDeleteSelect_Name() {
	id := primitives.NewUUID()
	name := shuffle.String(testNamespaceName)
	notificationVersion := int64(1)

	namespace := s.newRandomNamespaceRow(id, name, notificationVersion)
	result, err := s.store.InsertIntoNamespace(newExecutionContext(), &namespace)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.NamespaceFilter{
		Name: &name,
	}
	result, err = s.store.DeleteFromNamespace(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter = sqlplugin.NamespaceFilter{
		ID: &id,
	}
	_, err = s.store.SelectFromNamespace(newExecutionContext(), filter)
	s.Error(err) // TODO persistence layer should do proper error translation

	filter = sqlplugin.NamespaceFilter{
		Name: &name,
	}
	_, err = s.store.SelectFromNamespace(newExecutionContext(), filter)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *namespaceSuite) TestInsertSelect_Pagination() {
	// cleanup the namespace for pagination test
	rowsPerPage, err := s.store.SelectFromNamespace(newExecutionContext(), sqlplugin.NamespaceFilter{
		GreaterThanID: nil,
		PageSize:      convert.IntPtr(1000000),
	})
	switch err {
	case nil:
		for _, row := range rowsPerPage {
			_, err := s.store.DeleteFromNamespace(newExecutionContext(), sqlplugin.NamespaceFilter{
				ID: &row.ID,
			})
			s.NoError(err)
		}
	case sql.ErrNoRows:
		// noop
	default:
		s.NoError(err)
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
		s.NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.NoError(err)
		s.Equal(1, int(rowsAffected))

		namespaces[namespace.ID.String()] = &namespace
	}

	rows := map[string]*sqlplugin.NamespaceRow{}
	filter := sqlplugin.NamespaceFilter{
		GreaterThanID: nil,
		PageSize:      convert.IntPtr(numNamespacePerPage),
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
			s.NoError(err)
		}
	}
	s.Equal(namespaces, rows)
}

func (s *namespaceSuite) TestSelectLockMetadata() {
	row, err := s.store.SelectFromNamespaceMetadata(newExecutionContext())
	s.NoError(err)

	// NOTE: lock without transaction is equivalent to select
	//  this test only test the select functionality
	metadata, err := s.store.LockNamespaceMetadata(newExecutionContext())
	s.NoError(err)
	s.Equal(row, metadata)
}

func (s *namespaceSuite) TestSelectUpdateSelectMetadata_Success() {
	row, err := s.store.SelectFromNamespaceMetadata(newExecutionContext())
	s.NoError(err)
	originalVersion := row.NotificationVersion

	result, err := s.store.UpdateNamespaceMetadata(newExecutionContext(), row)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	row, err = s.store.SelectFromNamespaceMetadata(newExecutionContext())
	s.NoError(err)
	s.Equal(originalVersion+1, row.NotificationVersion)
}

func (s *namespaceSuite) TestSelectUpdateSelectMetadata_Fail() {
	row, err := s.store.SelectFromNamespaceMetadata(newExecutionContext())
	s.NoError(err)
	originalVersion := row.NotificationVersion

	namespaceMetadata := s.newRandomNamespaceMetadataRow(row.NotificationVersion + 1000)
	result, err := s.store.UpdateNamespaceMetadata(newExecutionContext(), &namespaceMetadata)
	s.NoError(err) // TODO persistence layer should do proper error translation
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	row, err = s.store.SelectFromNamespaceMetadata(newExecutionContext())
	s.NoError(err)
	s.Equal(originalVersion, row.NotificationVersion)
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
