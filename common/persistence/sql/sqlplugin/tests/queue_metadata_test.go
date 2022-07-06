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
		*require.Assertions

		store sqlplugin.QueueMetadata
	}
)

func newQueueMetadataSuite(
	t *testing.T,
	store sqlplugin.QueueMetadata,
) *queueMetadataSuite {
	return &queueMetadataSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *queueMetadataSuite) SetupSuite() {

}

func (s *queueMetadataSuite) TearDownSuite() {

}

func (s *queueMetadataSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *queueMetadataSuite) TearDownTest() {

}

func (s *queueMetadataSuite) TestInsert_Success() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *queueMetadataSuite) TestInsert_Fail_Duplicate() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	queueMetadata = s.newRandomQueueMetadataRow(queueType)
	_, err = s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *queueMetadataSuite) TestInsertSelect() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.QueueMetadataFilter{
		QueueType: queueType,
	}
	row, err := s.store.SelectFromQueueMetadata(newExecutionContext(), filter)
	s.NoError(err)
	row.QueueType = queueType
	s.Equal(&queueMetadata, row)
}

func (s *queueMetadataSuite) TestInsertUpdate_Success() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	queueMetadata = s.newRandomQueueMetadataRow(queueType)
	result, err = s.store.UpdateQueueMetadata(newExecutionContext(), &queueMetadata)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *queueMetadataSuite) TestUpdate_Fail() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.UpdateQueueMetadata(newExecutionContext(), &queueMetadata)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))
}

func (s *queueMetadataSuite) TestInsertUpdateSelect() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	queueMetadata = s.newRandomQueueMetadataRow(queueType)
	result, err = s.store.UpdateQueueMetadata(newExecutionContext(), &queueMetadata)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.QueueMetadataFilter{
		QueueType: queueType,
	}
	row, err := s.store.SelectFromQueueMetadata(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(queueMetadata.DataEncoding, row.DataEncoding)
	s.Equal(queueMetadata.Data, row.Data)
	s.Equal(queueMetadata.Version+1, row.Version) // version increase by one after update
}

func (s *queueMetadataSuite) TestSelectReadLock() {
	queueType := persistence.QueueType(rand.Int31())

	queueMetadata := s.newRandomQueueMetadataRow(queueType)
	result, err := s.store.InsertIntoQueueMetadata(newExecutionContext(), &queueMetadata)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	// NOTE: lock without transaction is equivalent to select
	//  this test only test the select functionality
	filter := sqlplugin.QueueMetadataFilter{
		QueueType: queueType,
	}
	row, err := s.store.LockQueueMetadata(newExecutionContext(), filter)
	s.NoError(err)
	row.QueueType = queueType
	s.Equal(&queueMetadata, row)
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
