// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
)

type (
	visibilitySuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.Visibility
	}
)

const (
	testVisibilityEncoding         = "random encoding"
	testVisibilityWorkflowTypeName = "random workflow type name"
	testVisibilityWorkflowID       = "random workflow ID"
)

var (
	testVisibilityData = []byte("random history execution activity data")
)

func newVisibilitySuite(
	t *testing.T,
	store sqlplugin.Visibility,
) *visibilitySuite {
	return &visibilitySuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *visibilitySuite) SetupSuite() {

}

func (s *visibilitySuite) TearDownSuite() {

}

func (s *visibilitySuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *visibilitySuite) TearDownTest() {

}

func (s *visibilitySuite) TestInsertSelect_NonExists() {
	namespaceID := primitives.NewUUID()
	runID := primitives.NewUUID()
	workflowTypeName := shuffle.String(testVisibilityWorkflowTypeName)
	workflowID := shuffle.String(testVisibilityWorkflowID)
	startTime := s.now()
	executionTime := startTime.Add(time.Second)
	status := int32(0)
	closeTime := (*time.Time)(nil)
	historyLength := (*int64)(nil)

	visibility := s.newRandomVisibilityRow(
		namespaceID,
		runID,
		workflowTypeName,
		workflowID,
		startTime,
		executionTime,
		status,
		closeTime,
		historyLength,
	)
	result, err := s.store.InsertIntoVisibility(&visibility)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.VisibilitySelectFilter{
		NamespaceID: namespaceID.String(),
		RunID:       convert.StringPtr(runID.String()),
	}
	rows, err := s.store.SelectFromVisibility(selectFilter)
	s.NoError(err)
	for index := range rows {
		rows[index].NamespaceID = namespaceID.String()
	}
	s.Equal([]sqlplugin.VisibilityRow{visibility}, rows)
}

func (s *visibilitySuite) TestInsertSelect_Exists() {
	namespaceID := primitives.NewUUID()
	runID := primitives.NewUUID()
	workflowTypeName := shuffle.String(testVisibilityWorkflowTypeName)
	workflowID := shuffle.String(testVisibilityWorkflowID)
	startTime := s.now()
	executionTime := startTime.Add(time.Second)
	status := int32(0)
	closeTime := (*time.Time)(nil)
	historyLength := (*int64)(nil)

	visibility1 := s.newRandomVisibilityRow(
		namespaceID,
		runID,
		workflowTypeName,
		workflowID,
		startTime,
		executionTime,
		status,
		closeTime,
		historyLength,
	)
	result, err := s.store.InsertIntoVisibility(&visibility1)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	visibility2 := s.newRandomVisibilityRow(
		namespaceID,
		runID,
		workflowTypeName,
		workflowID,
		startTime,
		executionTime,
		status,
		closeTime,
		historyLength,
	)
	_, err = s.store.InsertIntoVisibility(&visibility2)
	s.NoError(err)
	// NOTE: cannot do assertion on affected rows
	//  PostgreSQL will return 0
	//  MySQL will return 1: ref https://dev.mysql.com/doc/c-api/5.7/en/mysql-affected-rows.html

	selectFilter := sqlplugin.VisibilitySelectFilter{
		NamespaceID: namespaceID.String(),
		RunID:       convert.StringPtr(runID.String()),
	}
	rows, err := s.store.SelectFromVisibility(selectFilter)
	s.NoError(err)
	for index := range rows {
		rows[index].NamespaceID = namespaceID.String()
	}
	s.Equal([]sqlplugin.VisibilityRow{visibility1}, rows)
}

func (s *visibilitySuite) TestReplaceSelect_NonExists() {
	namespaceID := primitives.NewUUID()
	runID := primitives.NewUUID()
	workflowTypeName := shuffle.String(testVisibilityWorkflowTypeName)
	workflowID := shuffle.String(testVisibilityWorkflowID)
	startTime := s.now()
	executionTime := startTime.Add(time.Second)
	status := int32(0)
	closeTime := executionTime.Add(time.Second)
	historyLength := rand.Int63()

	visibility := s.newRandomVisibilityRow(
		namespaceID,
		runID,
		workflowTypeName,
		workflowID,
		startTime,
		executionTime,
		status,
		&closeTime,
		convert.Int64Ptr(historyLength),
	)
	result, err := s.store.ReplaceIntoVisibility(&visibility)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.VisibilitySelectFilter{
		NamespaceID: namespaceID.String(),
		RunID:       convert.StringPtr(runID.String()),
	}
	rows, err := s.store.SelectFromVisibility(selectFilter)
	s.NoError(err)
	for index := range rows {
		rows[index].NamespaceID = namespaceID.String()
	}
	s.Equal([]sqlplugin.VisibilityRow{visibility}, rows)
}

func (s *visibilitySuite) TestReplaceSelect_Exists() {
	namespaceID := primitives.NewUUID()
	runID := primitives.NewUUID()
	workflowTypeName := shuffle.String(testVisibilityWorkflowTypeName)
	workflowID := shuffle.String(testVisibilityWorkflowID)
	startTime := s.now()
	executionTime := startTime.Add(time.Second)
	status := int32(0)
	closeTime := executionTime.Add(time.Second)
	historyLength := rand.Int63()

	visibility := s.newRandomVisibilityRow(
		namespaceID,
		runID,
		workflowTypeName,
		workflowID,
		startTime,
		executionTime,
		status,
		&closeTime,
		convert.Int64Ptr(historyLength),
	)
	result, err := s.store.ReplaceIntoVisibility(&visibility)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	visibility = s.newRandomVisibilityRow(
		namespaceID,
		runID,
		workflowTypeName,
		workflowID,
		startTime,
		executionTime,
		status,
		&closeTime,
		convert.Int64Ptr(historyLength),
	)
	_, err = s.store.ReplaceIntoVisibility(&visibility)
	s.NoError(err)
	// NOTE: cannot do assertion on affected rows
	//  PostgreSQL will return 1
	//  MySQL will return 2: ref https://dev.mysql.com/doc/c-api/5.7/en/mysql-affected-rows.html

	selectFilter := sqlplugin.VisibilitySelectFilter{
		NamespaceID: namespaceID.String(),
		RunID:       convert.StringPtr(runID.String()),
	}
	rows, err := s.store.SelectFromVisibility(selectFilter)
	s.NoError(err)
	for index := range rows {
		rows[index].NamespaceID = namespaceID.String()
	}
	s.Equal([]sqlplugin.VisibilityRow{visibility}, rows)
}

func (s *visibilitySuite) TestDeleteSelect() {
	namespaceID := primitives.NewUUID()
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.VisibilityDeleteFilter{
		NamespaceID: namespaceID.String(),
		RunID:       runID.String(),
	}
	result, err := s.store.DeleteFromVisibility(deleteFilter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.VisibilitySelectFilter{
		NamespaceID: namespaceID.String(),
		RunID:       convert.StringPtr(runID.String()),
	}
	_, err = s.store.SelectFromVisibility(selectFilter)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *visibilitySuite) TestInsertDeleteSelect() {
	namespaceID := primitives.NewUUID()
	runID := primitives.NewUUID()
	workflowTypeName := shuffle.String(testVisibilityWorkflowTypeName)
	workflowID := shuffle.String(testVisibilityWorkflowID)
	startTime := s.now()
	executionTime := startTime.Add(time.Second)
	status := int32(0)
	closeTime := (*time.Time)(nil)
	historyLength := (*int64)(nil)

	visibility := s.newRandomVisibilityRow(
		namespaceID,
		runID,
		workflowTypeName,
		workflowID,
		startTime,
		executionTime,
		status,
		closeTime,
		historyLength,
	)
	result, err := s.store.InsertIntoVisibility(&visibility)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	deleteFilter := sqlplugin.VisibilityDeleteFilter{
		NamespaceID: namespaceID.String(),
		RunID:       runID.String(),
	}
	result, err = s.store.DeleteFromVisibility(deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.VisibilitySelectFilter{
		NamespaceID: namespaceID.String(),
		RunID:       convert.StringPtr(runID.String()),
	}
	_, err = s.store.SelectFromVisibility(selectFilter)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *visibilitySuite) TestReplaceDeleteSelect() {
	namespaceID := primitives.NewUUID()
	runID := primitives.NewUUID()
	workflowTypeName := shuffle.String(testVisibilityWorkflowTypeName)
	workflowID := shuffle.String(testVisibilityWorkflowID)
	startTime := s.now()
	executionTime := startTime.Add(time.Second)
	status := int32(0)
	closeTime := executionTime.Add(time.Second)
	historyLength := rand.Int63()

	visibility := s.newRandomVisibilityRow(
		namespaceID,
		runID,
		workflowTypeName,
		workflowID,
		startTime,
		executionTime,
		status,
		&closeTime,
		convert.Int64Ptr(historyLength),
	)
	result, err := s.store.ReplaceIntoVisibility(&visibility)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	deleteFilter := sqlplugin.VisibilityDeleteFilter{
		NamespaceID: namespaceID.String(),
		RunID:       runID.String(),
	}
	result, err = s.store.DeleteFromVisibility(deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.VisibilitySelectFilter{
		NamespaceID: namespaceID.String(),
		RunID:       convert.StringPtr(runID.String()),
	}
	_, err = s.store.SelectFromVisibility(selectFilter)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *visibilitySuite) now() time.Time {
	return time.Now().UTC().Truncate(time.Millisecond)
}

func (s *visibilitySuite) newRandomVisibilityRow(
	namespaceID primitives.UUID,
	runID primitives.UUID,
	workflowTypeName string,
	workflowID string,
	startTime time.Time,
	executionTime time.Time,
	status int32,
	closeTime *time.Time,
	historyLength *int64,
) sqlplugin.VisibilityRow {
	return sqlplugin.VisibilityRow{
		NamespaceID:      namespaceID.String(),
		RunID:            runID.String(),
		WorkflowTypeName: workflowTypeName,
		WorkflowID:       workflowID,
		StartTime:        startTime,
		ExecutionTime:    executionTime,
		Status:           status,
		CloseTime:        closeTime,
		HistoryLength:    historyLength,
		Memo:             shuffle.Bytes(testVisibilityData),
		Encoding:         testVisibilityEncoding,
	}
}
