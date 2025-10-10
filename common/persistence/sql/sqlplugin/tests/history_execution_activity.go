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

		store sqlplugin.HistoryExecutionActivity
	}
)

const (
	testHistoryExecutionActivityEncoding = "random encoding"
)

var (
	testHistoryExecutionActivityData = []byte("random history execution activity data")
)

func NewHistoryExecutionActivitySuite(
	t *testing.T,
	store sqlplugin.HistoryExecutionActivity,
) *historyExecutionActivitySuite {
	return &historyExecutionActivitySuite{

		store: store,
	}
}

func (s *historyExecutionActivitySuite) TearDownSuite() {

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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *historyExecutionActivitySuite) TestReplace_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	activity1 := s.newRandomExecutionActivityRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	activity2 := s.newRandomExecutionActivityRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	result, err := s.store.ReplaceIntoActivityInfoMaps(newExecutionContext(), []sqlplugin.ActivityInfoMapsRow{activity1, activity2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))
}

func (s *historyExecutionActivitySuite) TestReplaceSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	scheduledEventID := rand.Int63()

	activity := s.newRandomExecutionActivityRow(shardID, namespaceID, workflowID, runID, scheduledEventID)
	result, err := s.store.ReplaceIntoActivityInfoMaps(newExecutionContext(), []sqlplugin.ActivityInfoMapsRow{activity})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	rowMap := map[int64]sqlplugin.ActivityInfoMapsRow{}
	for _, activity := range rows {
		rowMap[activity.ScheduleID] = activity
	}
	require.Equal(s.T(), map[int64]sqlplugin.ActivityInfoMapsRow{
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numActivities, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	activityMap := map[int64]sqlplugin.ActivityInfoMapsRow{}
	for _, activity := range activities {
		activityMap[activity.ScheduleID] = activity
	}
	rowMap := map[int64]sqlplugin.ActivityInfoMapsRow{}
	for _, activity := range rows {
		rowMap[activity.ScheduleID] = activity
	}
	require.Equal(s.T(), activityMap, rowMap)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.ActivityInfoMapsRow(nil), rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.ActivityInfoMapsRow(nil), rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.ActivityInfoMapsRow(nil), rows)
}

func (s *historyExecutionActivitySuite) TestReplaceDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	scheduledEventID := rand.Int63()

	activity := s.newRandomExecutionActivityRow(shardID, namespaceID, workflowID, runID, scheduledEventID)
	result, err := s.store.ReplaceIntoActivityInfoMaps(newExecutionContext(), []sqlplugin.ActivityInfoMapsRow{activity})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	deleteFilter := sqlplugin.ActivityInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduleIDs: []int64{scheduledEventID},
	}
	result, err = s.store.DeleteFromActivityInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.ActivityInfoMapsRow(nil), rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numActivities, int(rowsAffected))

	deleteFilter := sqlplugin.ActivityInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduleIDs: activityScheduledEventIDs,
	}
	result, err = s.store.DeleteFromActivityInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numActivities, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.ActivityInfoMapsRow(nil), rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numActivities, int(rowsAffected))

	deleteFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteAllFromActivityInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numActivities, int(rowsAffected))

	selectFilter := sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromActivityInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.ActivityInfoMapsRow(nil), rows)
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
