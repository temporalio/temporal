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

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
)

type (
	historyExecutionActivitySuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryExecutionActivity
	}
)

const (
	testHistoryExecutionActivityEncoding = "random encoding"
)

var (
	testHistoryExecutionActivityData = []byte("random history execution activity data")
)

func newHistoryExecutionActivitySuite(
	t *testing.T,
	store sqlplugin.HistoryExecutionActivity,
) *historyExecutionActivitySuite {
	return &historyExecutionActivitySuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyExecutionActivitySuite) SetupSuite() {

}

func (s *historyExecutionActivitySuite) TearDownSuite() {

}

func (s *historyExecutionActivitySuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyExecutionActivitySuite) TearDownTest() {

}

func (s *historyExecutionActivitySuite) TestReplace_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	scheduledEventID := rand.Int63()

	activity := s.newRandomExecutionActivityRow(shardID, namespaceID, workflowID, runID, scheduledEventID)
	result, err := s.store.ReplaceIntoActivityInfoMaps(newExecutionContext(), []sqlplugin.ActivityInfoMapsRow{activity})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyExecutionActivitySuite) TestReplace_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	activity1 := s.newRandomExecutionActivityRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	activity2 := s.newRandomExecutionActivityRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	result, err := s.store.ReplaceIntoActivityInfoMaps(newExecutionContext(), []sqlplugin.ActivityInfoMapsRow{activity1, activity2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
}

func (s *historyExecutionActivitySuite) TestReplaceSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	scheduledEventID := rand.Int63()

	activity := s.newRandomExecutionActivityRow(shardID, namespaceID, workflowID, runID, scheduledEventID)
	result, err := s.store.ReplaceIntoActivityInfoMaps(newExecutionContext(), []sqlplugin.ActivityInfoMapsRow{activity})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	rowMap := map[int64]sqlplugin.ActivityInfoMapsRow{}
	for _, activity := range rows {
		rowMap[activity.ScheduleID] = activity
	}
	s.Equal(map[int64]sqlplugin.ActivityInfoMapsRow{
		activity.ScheduleID: activity,
	}, rowMap)
}

func (s *historyExecutionActivitySuite) TestReplaceSelect_Multiple() {
	numActivities := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var activities []sqlplugin.ActivityInfoMapsRow
	for i := 0; i < numActivities; i++ {
		activity := s.newRandomExecutionActivityRow(shardID, namespaceID, workflowID, runID, rand.Int63())
		activities = append(activities, activity)
	}
	result, err := s.store.ReplaceIntoActivityInfoMaps(newExecutionContext(), activities)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numActivities, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	activityMap := map[int64]sqlplugin.ActivityInfoMapsRow{}
	for _, activity := range activities {
		activityMap[activity.ScheduleID] = activity
	}
	rowMap := map[int64]sqlplugin.ActivityInfoMapsRow{}
	for _, activity := range rows {
		rowMap[activity.ScheduleID] = activity
	}
	s.Equal(activityMap, rowMap)
}

func (s *historyExecutionActivitySuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	scheduledEventID := rand.Int63()

	deleteFilter := sqlplugin.ActivityInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduleIDs: []int64{scheduledEventID},
	}
	result, err := s.store.DeleteFromActivityInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.ActivityInfoMapsRow(nil), rows)
}

func (s *historyExecutionActivitySuite) TestDeleteSelect_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.ActivityInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduleIDs: []int64{rand.Int63(), rand.Int63()},
	}
	result, err := s.store.DeleteFromActivityInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.ActivityInfoMapsRow(nil), rows)
}

func (s *historyExecutionActivitySuite) TestDeleteSelect_All() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err := s.store.DeleteAllFromActivityInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.ActivityInfoMapsRow(nil), rows)
}

func (s *historyExecutionActivitySuite) TestReplaceDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	scheduledEventID := rand.Int63()

	activity := s.newRandomExecutionActivityRow(shardID, namespaceID, workflowID, runID, scheduledEventID)
	result, err := s.store.ReplaceIntoActivityInfoMaps(newExecutionContext(), []sqlplugin.ActivityInfoMapsRow{activity})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	deleteFilter := sqlplugin.ActivityInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduleIDs: []int64{scheduledEventID},
	}
	result, err = s.store.DeleteFromActivityInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.ActivityInfoMapsRow(nil), rows)
}

func (s *historyExecutionActivitySuite) TestReplaceDeleteSelect_Multiple() {
	numActivities := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var activities []sqlplugin.ActivityInfoMapsRow
	var activityScheduledEventIDs []int64
	for i := 0; i < numActivities; i++ {
		activityScheduledEventID := rand.Int63()
		activity := s.newRandomExecutionActivityRow(shardID, namespaceID, workflowID, runID, activityScheduledEventID)
		activityScheduledEventIDs = append(activityScheduledEventIDs, activityScheduledEventID)
		activities = append(activities, activity)
	}
	result, err := s.store.ReplaceIntoActivityInfoMaps(newExecutionContext(), activities)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numActivities, int(rowsAffected))

	deleteFilter := sqlplugin.ActivityInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduleIDs: activityScheduledEventIDs,
	}
	result, err = s.store.DeleteFromActivityInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numActivities, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.ActivityInfoMapsRow(nil), rows)
}

func (s *historyExecutionActivitySuite) TestReplaceDeleteSelect_All() {
	numActivities := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var activities []sqlplugin.ActivityInfoMapsRow
	for i := 0; i < numActivities; i++ {
		activityScheduledEventID := rand.Int63()
		activity := s.newRandomExecutionActivityRow(shardID, namespaceID, workflowID, runID, activityScheduledEventID)
		activities = append(activities, activity)
	}
	result, err := s.store.ReplaceIntoActivityInfoMaps(newExecutionContext(), activities)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numActivities, int(rowsAffected))

	deleteFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteAllFromActivityInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numActivities, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.ActivityInfoMapsRow(nil), rows)
}

func (s *historyExecutionActivitySuite) newRandomExecutionActivityRow(
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	scheduledEventID int64,
) sqlplugin.ActivityInfoMapsRow {
	return sqlplugin.ActivityInfoMapsRow{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		ScheduleID:   scheduledEventID,
		Data:         shuffle.Bytes(testHistoryExecutionActivityData),
		DataEncoding: testHistoryExecutionActivityEncoding,
	}
}
