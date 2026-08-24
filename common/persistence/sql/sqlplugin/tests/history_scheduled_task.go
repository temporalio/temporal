package tests

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/shuffle"
)

type (
	historyHistoryScheduledTaskSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryScheduledTask
	}
)

const (
	testHistoryScheduledTaskEncoding = "random encoding"
)

var (
	testHistoryScheduledTaskData = []byte("random history scheduled task data")
)

func NewHistoryScheduledTaskSuite(
	t *testing.T,
	store sqlplugin.HistoryScheduledTask,
) *historyHistoryScheduledTaskSuite {
	return &historyHistoryScheduledTaskSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyHistoryScheduledTaskSuite) SetupSuite() {

}

func (s *historyHistoryScheduledTaskSuite) TearDownSuite() {

}

func (s *historyHistoryScheduledTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyHistoryScheduledTaskSuite) TearDownTest() {

}

// TestInsertSelect_CursorRowIncluded verifies that the cursor row itself
// is returned (>= semantics).
func (s *historyHistoryScheduledTaskSuite) TestInsertSelect_CursorRowIncluded() {
	shardID := rand.Int31()
	categoryID := rand.Int31()
	timestamp := s.now()

	// Insert exactly 3 tasks at the SAME timestamp
	tasks := []sqlplugin.HistoryScheduledTasksRow{
		s.newRandomScheduledTaskRow(shardID, categoryID, timestamp, 1),
		s.newRandomScheduledTaskRow(shardID, categoryID, timestamp, 2),
		s.newRandomScheduledTaskRow(shardID, categoryID, timestamp, 3),
	}
	result, err := s.store.InsertIntoHistoryScheduledTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(3, int(rowsAffected))

	// Cursor at exactly (timestamp, taskID=2) — this row MUST be included.
	// With >= : returns taskID 2, 3  (correct)
	// With >  : returns taskID 3     (wrong — misses the cursor row)
	filter := sqlplugin.HistoryScheduledTasksRangeFilter{
		ShardID:                         shardID,
		CategoryID:                      categoryID,
		InclusiveMinVisibilityTimestamp: timestamp,
		InclusiveMinTaskID:              2,
		ExclusiveMaxVisibilityTimestamp: timestamp.Add(common.ScheduledTaskMinPrecision),
		PageSize:                        10,
	}
	rows, err := s.store.RangeSelectFromHistoryScheduledTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].CategoryID = categoryID
	}
	// Must return exactly 2 rows: taskID=2 and taskID=3
	s.Equal(2, len(rows), "expected cursor row (taskID=2) to be included")
	s.Equal(tasks[1:], rows)
}

// TestInsertSelect_CursorExcludesPriorRows verifies that rows before the
// cursor at the same timestamp are excluded.
func (s *historyHistoryScheduledTaskSuite) TestInsertSelect_CursorExcludesPriorRows() {
	shardID := rand.Int31()
	categoryID := rand.Int31()
	timestamp := s.now()

	tasks := []sqlplugin.HistoryScheduledTasksRow{
		s.newRandomScheduledTaskRow(shardID, categoryID, timestamp, 1),
		s.newRandomScheduledTaskRow(shardID, categoryID, timestamp, 2),
		s.newRandomScheduledTaskRow(shardID, categoryID, timestamp, 3),
	}
	result, err := s.store.InsertIntoHistoryScheduledTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(3, int(rowsAffected))

	// Cursor at (timestamp, taskID=3) - only taskID=3 should be returned.
	// taskID=1 and taskID=2 must be excluded.
	filter := sqlplugin.HistoryScheduledTasksRangeFilter{
		ShardID:                         shardID,
		CategoryID:                      categoryID,
		InclusiveMinVisibilityTimestamp: timestamp,
		InclusiveMinTaskID:              3,
		ExclusiveMaxVisibilityTimestamp: timestamp.Add(common.ScheduledTaskMinPrecision),
		PageSize:                        10,
	}
	rows, err := s.store.RangeSelectFromHistoryScheduledTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].CategoryID = categoryID
	}
	s.Equal(1, len(rows), "expected only taskID=3, prior rows must be excluded")
	s.Equal(tasks[2:], rows)
}

// TestInsertSelect_UpperBoundExclusive verifies that tasks at exactly ExclusiveMaxVisibilityTimestamp are NOT returned.
func (s *historyHistoryScheduledTaskSuite) TestInsertSelect_UpperBoundExclusive() {
	shardID := rand.Int31()
	categoryID := rand.Int31()
	timestamp := s.now()
	boundary := timestamp.Add(time.Millisecond)

	tasks := []sqlplugin.HistoryScheduledTasksRow{
		s.newRandomScheduledTaskRow(shardID, categoryID, timestamp, 1),
		s.newRandomScheduledTaskRow(shardID, categoryID, boundary, 1), // at the upper bound
	}
	result, err := s.store.InsertIntoHistoryScheduledTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	// ExclusiveMax = boundary, so task at boundary must NOT be returned.
	// With <  : returns only (timestamp, 1)  (correct)
	// With <= : returns both                 (wrong)
	filter := sqlplugin.HistoryScheduledTasksRangeFilter{
		ShardID:                         shardID,
		CategoryID:                      categoryID,
		InclusiveMinVisibilityTimestamp: timestamp,
		InclusiveMinTaskID:              1,
		ExclusiveMaxVisibilityTimestamp: boundary,
		PageSize:                        10,
	}
	rows, err := s.store.RangeSelectFromHistoryScheduledTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].CategoryID = categoryID
	}
	s.Equal(1, len(rows), "task at ExclusiveMaxVisibilityTimestamp must not be returned")
	s.Equal(tasks[:1], rows)
}

// TestInsertSelect_CrossTimestampCursor verifies that when the cursor
// points to an existing row, later timestamps return all their tasks
// regardless of taskID — even taskIDs smaller than the cursor's.
func (s *historyHistoryScheduledTaskSuite) TestInsertSelect_CrossTimestampCursor() {
	shardID := rand.Int31()
	categoryID := rand.Int31()
	timestamp := s.now()
	laterTimestamp := timestamp.Add(time.Millisecond)

	tasks := []sqlplugin.HistoryScheduledTasksRow{
		s.newRandomScheduledTaskRow(shardID, categoryID, timestamp, 1),
		s.newRandomScheduledTaskRow(shardID, categoryID, timestamp, 2),
		s.newRandomScheduledTaskRow(shardID, categoryID, timestamp, 3),
		s.newRandomScheduledTaskRow(shardID, categoryID, laterTimestamp, 1), // taskID=1 < cursor taskID=2, but later timestamp
		s.newRandomScheduledTaskRow(shardID, categoryID, laterTimestamp, 2),
	}
	result, err := s.store.InsertIntoHistoryScheduledTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(5, int(rowsAffected))

	// Cursor at (timestamp, taskID=2) — an existing row.
	// At timestamp: taskID=1 excluded (1 < 2), taskID=2 included (>= 2), taskID=3 included.
	// At laterTimestamp: ALL tasks included (laterTs > ts), even taskID=1.
	// Total: 4 rows
	// With >= : (ts,2), (ts,3), (laterTs,1), (laterTs,2) → 4 rows ✓
	// With >  : (ts,3), (laterTs,1), (laterTs,2)         → 3 rows ✗
	filter := sqlplugin.HistoryScheduledTasksRangeFilter{
		ShardID:                         shardID,
		CategoryID:                      categoryID,
		InclusiveMinVisibilityTimestamp: timestamp,
		InclusiveMinTaskID:              2,
		ExclusiveMaxVisibilityTimestamp: laterTimestamp.Add(common.ScheduledTaskMinPrecision),
		PageSize:                        10,
	}
	rows, err := s.store.RangeSelectFromHistoryScheduledTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].CategoryID = categoryID
	}
	expected := []sqlplugin.HistoryScheduledTasksRow{tasks[1], tasks[2], tasks[3], tasks[4]}
	s.Equal(4, len(rows), "cursor row must be included and later timestamps must include all tasks")
	s.Equal(expected, rows)
}

// TestInsertSelect_MultiplePages verifies cursor continuity across multiple pages with no rows lost or duplicated.
func (s *historyHistoryScheduledTaskSuite) TestInsertSelect_MultiplePages() {
	shardID := rand.Int31()
	categoryID := rand.Int31()
	timestamp := s.now()
	pageSize := 3

	// Insert 9 tasks: 3 at each of 3 timestamps
	var tasks []sqlplugin.HistoryScheduledTasksRow
	for ts := 0; ts < 3; ts++ {
		for taskID := int64(1); taskID <= 3; taskID++ {
			tasks = append(tasks, s.newRandomScheduledTaskRow(
				shardID, categoryID, timestamp.Add(time.Duration(ts)*time.Millisecond), taskID,
			))
		}
	}
	result, err := s.store.InsertIntoHistoryScheduledTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(9, int(rowsAffected))

	maxTimestamp := timestamp.Add(3 * time.Millisecond)

	// Collect all rows across pages
	var allRows []sqlplugin.HistoryScheduledTasksRow
	filter := sqlplugin.HistoryScheduledTasksRangeFilter{
		ShardID:                         shardID,
		CategoryID:                      categoryID,
		InclusiveMinVisibilityTimestamp: timestamp,
		InclusiveMinTaskID:              0,
		ExclusiveMaxVisibilityTimestamp: maxTimestamp,
		PageSize:                        pageSize,
	}
	for {
		page, err := s.store.RangeSelectFromHistoryScheduledTasks(newExecutionContext(), filter)
		s.NoError(err)
		if len(page) == 0 {
			break
		}
		for index := range page {
			page[index].ShardID = shardID
			page[index].CategoryID = categoryID
		}
		allRows = append(allRows, page...)
		// Advance cursor past the last returned row
		lastRow := page[len(page)-1]
		filter.InclusiveMinVisibilityTimestamp = lastRow.VisibilityTimestamp
		filter.InclusiveMinTaskID = lastRow.TaskID + 1
	}
	// Must get all 9 rows, no duplicates, no gaps
	s.Equal(9, len(allRows), "pagination must return all rows exactly once")
	s.Equal(tasks, allRows)
}

// TestInsertSelect_PageBoundarySplitsSameTimestamp verifies that when a page
// boundary falls in the middle of tasks sharing the same timestamp, no rows
// are lost or duplicated. This is the critical edge case for the tuple cursor:
// the cursor advances to (sameTimestamp, lastTaskID+1) and the next page must
// pick up the remaining tasks at that timestamp without re-reading prior ones.
func (s *historyHistoryScheduledTaskSuite) TestInsertSelect_PageBoundarySplitsSameTimestamp() {
	shardID := rand.Int31()
	categoryID := rand.Int31()
	timestamp := s.now()
	pageSize := 2

	// Insert 5 tasks at the SAME timestamp.
	// With pageSize=2, pages will split within the same timestamp group:
	// Page 1: taskID=1, taskID=2
	// Page 2: taskID=3, taskID=4  (cursor splits here within same ts)
	// Page 3: taskID=5
	// Page 4: empty
	var tasks []sqlplugin.HistoryScheduledTasksRow
	for taskID := int64(1); taskID <= 5; taskID++ {
		tasks = append(tasks, s.newRandomScheduledTaskRow(shardID, categoryID, timestamp, taskID))
	}
	result, err := s.store.InsertIntoHistoryScheduledTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(5, int(rowsAffected))

	// Paginate through all rows
	var allRows []sqlplugin.HistoryScheduledTasksRow
	filter := sqlplugin.HistoryScheduledTasksRangeFilter{
		ShardID:                         shardID,
		CategoryID:                      categoryID,
		InclusiveMinVisibilityTimestamp: timestamp,
		InclusiveMinTaskID:              0,
		ExclusiveMaxVisibilityTimestamp: timestamp.Add(common.ScheduledTaskMinPrecision),
		PageSize:                        pageSize,
	}
	pageCount := 0
	for {
		page, err := s.store.RangeSelectFromHistoryScheduledTasks(newExecutionContext(), filter)
		s.NoError(err)
		if len(page) == 0 {
			break
		}
		pageCount++
		for index := range page {
			page[index].ShardID = shardID
			page[index].CategoryID = categoryID
		}
		allRows = append(allRows, page...)
		lastRow := page[len(page)-1]
		filter.InclusiveMinVisibilityTimestamp = lastRow.VisibilityTimestamp
		filter.InclusiveMinTaskID = lastRow.TaskID + 1
	}
	s.Equal(3, pageCount, "expected 3 pages for 5 tasks with pageSize=2")
	s.Equal(5, len(allRows), "all rows must be returned exactly once")
	s.Equal(tasks, allRows)
}

func (s *historyHistoryScheduledTaskSuite) now() time.Time {
	return time.Now().UTC().Truncate(time.Millisecond)
}

func (s *historyHistoryScheduledTaskSuite) newRandomScheduledTaskRow(
	shardID int32,
	categoryID int32,
	timestamp time.Time,
	taskID int64,
) sqlplugin.HistoryScheduledTasksRow {
	return sqlplugin.HistoryScheduledTasksRow{
		ShardID:             shardID,
		CategoryID:          categoryID,
		VisibilityTimestamp: timestamp,
		TaskID:              taskID,
		Data:                shuffle.Bytes(testHistoryScheduledTaskData),
		DataEncoding:        testHistoryScheduledTaskEncoding,
	}
}
