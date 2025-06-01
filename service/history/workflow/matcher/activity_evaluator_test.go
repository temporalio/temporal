package matcher

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/sqlquery"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestActivityInfoMatchEvaluator_LogicalOperations(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedMatch bool
		expectedError bool
	}{
		{
			name:          "test AND, true",
			query:         fmt.Sprintf("%s = 'activity_id' AND %s = 'activity_type'", activityIDColName, activityTypeNameColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "test AND, false, left cause",
			query:         fmt.Sprintf("%s = 'activity_id_unknown' AND %s = 'activity_type'", activityIDColName, activityTypeNameColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "test and, false, right cause",
			query:         fmt.Sprintf("%s = 'activity_id' AND %s = 'activity_type_unknown'", activityIDColName, activityTypeNameColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "test OR, true",
			query:         fmt.Sprintf("%s = 'activity_id' OR %s = 'activity_type'", activityIDColName, activityTypeNameColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "test OR, true, left only",
			query:         fmt.Sprintf("%s = 'activity_id' OR %s = 'activity_type_unknown'", activityIDColName, activityTypeNameColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "test OR, true, right only",
			query:         fmt.Sprintf("%s = 'activity_id_unknown' OR %s = 'activity_type'", activityIDColName, activityTypeNameColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "test OR, false",
			query:         fmt.Sprintf("%s = 'workflow_id_unknown' OR %s = 'workflow_type_unknown'", activityIDColName, activityTypeNameColName),
			expectedMatch: false,
			expectedError: false,
		},
	}

	ai := &persistencespb.ActivityInfo{
		ActivityId: "activity_id",
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := MatchActivity(ai, tt.query)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMatch, match)
			}
		})
	}

}

func TestActivityInfoMatchEvaluator_Basic(t *testing.T) {
	startTimeStr := "2023-10-26T14:30:00Z"

	tests := []struct {
		name          string
		query         string
		expectedMatch bool
		expectedError bool
	}{
		{
			name:          "empty query",
			query:         "",
			expectedMatch: false,
			expectedError: true,
		},
		{
			name:          "query start with where",
			query:         fmt.Sprintf("where %s = 'activity_id'", activityIDColName),
			expectedMatch: false,
			expectedError: true,
		},
		{
			name:          "query start with select",
			query:         fmt.Sprintf("select * where %s = 'activity_id'", activityIDColName),
			expectedMatch: false,
			expectedError: true,
		},
		// activityIDColName
		{
			name:          "activity id",
			query:         fmt.Sprintf("%s = 'activity_id'", activityIDColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity id - starts_with",
			query:         fmt.Sprintf("%s starts_with 'activity_'", activityIDColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity id - not starts_with",
			query:         fmt.Sprintf("%s not starts_with 'other_activity_'", activityIDColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity type",
			query:         fmt.Sprintf("%s = 'activity_type'", activityTypeNameColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity type - starts_with",
			query:         fmt.Sprintf("%s starts_with 'activity_'", activityTypeNameColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity type - not starts_with",
			query:         fmt.Sprintf("%s not starts_with 'other_activity_'", activityTypeNameColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity Task Queue",
			query:         fmt.Sprintf("%s = 'task_queue'", activityTaskQueueColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity Task Queue - starts_with",
			query:         fmt.Sprintf("%s starts_with 'task_q'", activityTaskQueueColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity Task Queue - not starts_with",
			query:         fmt.Sprintf("%s not starts_with 'other_task_queue'", activityTaskQueueColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity state",
			query:         fmt.Sprintf("%s = 'Started'", activityStateColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity state - negative",
			query:         fmt.Sprintf("%s = 'Terminated'", activityStateColName),
			expectedMatch: false,
			expectedError: false,
		},
	}
	startTime, err := sqlquery.ConvertToTime(fmt.Sprintf("'%s'", startTimeStr))
	assert.NoError(t, err)

	ai := &persistencespb.ActivityInfo{
		ActivityId: "activity_id",
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
		StartedTime:      timestamppb.New(startTime),
		CancelRequested:  false,
		StartedEventId:   1,
		ScheduledEventId: 1,
		TaskQueue:        "task_queue",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := MatchActivity(ai, tt.query)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMatch, match)
			}
		})
	}
}

func TestActivityInfoMatchEvaluator_StartTime(t *testing.T) {
	startTimeStr := "2023-10-26T14:30:00Z"
	beforeTimeStr := "2023-10-26T13:00:00Z"
	afterTimeStr := "2023-10-26T15:00:00Z"

	tests := []struct {
		name          string
		query         string
		expectedMatch bool
		expectedError bool
	}{
		{
			name:          "activity start time",
			query:         fmt.Sprintf("%s = '%s'", activityStartedTime, startTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity start time, >= clause, equal",
			query:         fmt.Sprintf("%s >= '%s'", activityStartedTime, startTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity start time, >= clause, after",
			query:         fmt.Sprintf("%s >= '%s'", activityStartedTime, afterTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity start time, >= clause, before",
			query:         fmt.Sprintf("%s >= '%s'", activityStartedTime, beforeTimeStr),
			expectedMatch: true,
			expectedError: false,
		},

		{
			name:          "activity start time, > clause, equal",
			query:         fmt.Sprintf("%s > '%s'", activityStartedTime, startTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity start time, > clause, after",
			query:         fmt.Sprintf("%s > '%s'", activityStartedTime, afterTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity start time, > clause, before",
			query:         fmt.Sprintf("%s >= '%s'", activityStartedTime, beforeTimeStr),
			expectedMatch: true,
			expectedError: false,
		},

		{
			name:          "activity start time, <= clause, equal",
			query:         fmt.Sprintf("%s <= '%s'", activityStartedTime, startTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity start time, <= clause, after",
			query:         fmt.Sprintf("%s <= '%s'", activityStartedTime, afterTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity start time, <= clause, before",
			query:         fmt.Sprintf("%s <= '%s'", activityStartedTime, beforeTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity start time < clause, equal",
			query:         fmt.Sprintf("%s < '%s'", activityStartedTime, startTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity start time, < clause, after",
			query:         fmt.Sprintf("%s < '%s'", activityStartedTime, afterTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity start time where < clause, before",
			query:         fmt.Sprintf("%s < '%s'", activityStartedTime, beforeTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity start time between clause, match",
			query:         fmt.Sprintf("%s between '%s' and '%s'", activityStartedTime, beforeTimeStr, afterTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity start time between clause, miss",
			query:         fmt.Sprintf("%s between '%s' and '%s'", activityStartedTime, afterTimeStr, "2023-10-26T16:00:00Z"),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity start time between clause, inclusive left",
			query:         fmt.Sprintf("%s between '%s' and '%s'", activityStartedTime, beforeTimeStr, startTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity start time between clause, inclusive right",
			query:         fmt.Sprintf("%s between '%s' and '%s'", activityStartedTime, startTimeStr, afterTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity start time not between clause, miss",
			query:         fmt.Sprintf("%s not between '%s' and '%s'", activityStartedTime, beforeTimeStr, afterTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity start time not between clause, match",
			query:         fmt.Sprintf("%s not between '%s' and '%s'", activityStartedTime, afterTimeStr, "2023-10-26T16:00:00Z"),
			expectedMatch: true,
			expectedError: false,
		},
	}
	startTime, err := sqlquery.ConvertToTime(fmt.Sprintf("'%s'", startTimeStr))
	assert.NoError(t, err)

	ai := &persistencespb.ActivityInfo{
		ActivityId: "activity_id",
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
		StartedTime:      timestamppb.New(startTime),
		CancelRequested:  false,
		StartedEventId:   1,
		ScheduledEventId: 1,
	}

	controller := gomock.NewController(t)
	defer controller.Finish()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := MatchActivity(ai, tt.query)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMatch, match)
			}
		})
	}
}

func TestActivityInfoMatchEvaluator_Attempts(t *testing.T) {
	startTimeStr := "2023-10-26T14:30:00Z"

	tests := []struct {
		name          string
		query         string
		expectedMatch bool
		expectedError bool
	}{
		{
			name:          "activity attempts, = clause, match",
			query:         fmt.Sprintf("%s = 2", activityAttemptsColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity attempts, = clause, not match",
			query:         fmt.Sprintf("%s = 3", activityAttemptsColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity attempts, = clause, wrong type",
			query:         fmt.Sprintf("%s = 'two'", activityAttemptsColName),
			expectedMatch: false,
			expectedError: true,
		},
		{
			name:          "activity attempts, >= clause, match",
			query:         fmt.Sprintf("%s >= 2", activityAttemptsColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity attempts, >= clause, not match",
			query:         fmt.Sprintf("%s >= 3", activityAttemptsColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity attempts, > clause, match",
			query:         fmt.Sprintf("%s > 1", activityAttemptsColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity attempts, > clause, not match",
			query:         fmt.Sprintf("%s > 3", activityAttemptsColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity attempts, <= clause, match",
			query:         fmt.Sprintf("%s <= 2", activityAttemptsColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity attempts, <= clause, not match",
			query:         fmt.Sprintf("%s <= 1", activityAttemptsColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity attempts, < clause, match",
			query:         fmt.Sprintf("%s < 3", activityAttemptsColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity attempts, < clause, not match",
			query:         fmt.Sprintf("%s < 2", activityAttemptsColName),
			expectedMatch: false,
			expectedError: false,
		},
	}

	startTime, err := sqlquery.ConvertToTime(fmt.Sprintf("'%s'", startTimeStr))
	assert.NoError(t, err)

	ai := &persistencespb.ActivityInfo{
		ActivityId: "activity_id",
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
		StartedTime:      timestamppb.New(startTime),
		CancelRequested:  false,
		StartedEventId:   1,
		ScheduledEventId: 1,
		Attempt:          2,
	}

	controller := gomock.NewController(t)
	defer controller.Finish()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := MatchActivity(ai, tt.query)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMatch, match)
			}
		})
	}
}

func TestActivityInfoMatchEvaluator_BackoffInterval(t *testing.T) {
	startTimeStr := "2023-10-26T14:30:00Z"

	tests := []struct {
		name          string
		query         string
		expectedMatch bool
		expectedError bool
	}{
		{
			name:          "activity backoff, = clause, match",
			query:         fmt.Sprintf("%s = 10", activityBackoffIntervalColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity backoff, = clause, not match",
			query:         fmt.Sprintf("%s = 13", activityBackoffIntervalColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity backoff, = clause, wrong type",
			query:         fmt.Sprintf("%s = 'two'", activityBackoffIntervalColName),
			expectedMatch: false,
			expectedError: true,
		},
		{
			name:          "activity backoff, >= clause, match",
			query:         fmt.Sprintf("%s >= 10", activityBackoffIntervalColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity backoff, >= clause, not match",
			query:         fmt.Sprintf("%s >= 12", activityBackoffIntervalColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity backoff, > clause, match",
			query:         fmt.Sprintf("%s > 1", activityBackoffIntervalColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity backoff, > clause, not match",
			query:         fmt.Sprintf("%s > 10", activityBackoffIntervalColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity backoff, <= clause, match",
			query:         fmt.Sprintf("%s <= 10", activityBackoffIntervalColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity backoff, <= clause, not match",
			query:         fmt.Sprintf("%s <= 1", activityBackoffIntervalColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "activity backoff, < clause, match",
			query:         fmt.Sprintf("%s < 12", activityBackoffIntervalColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "activity backoff, < clause, not match",
			query:         fmt.Sprintf("%s < 10", activityBackoffIntervalColName),
			expectedMatch: false,
			expectedError: false,
		},
	}

	startTime, err := sqlquery.ConvertToTime(fmt.Sprintf("'%s'", startTimeStr))
	assert.NoError(t, err)

	ai := &persistencespb.ActivityInfo{
		ActivityId: "activity_id",
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
		StartedTime:             timestamppb.New(startTime),
		CancelRequested:         false,
		StartedEventId:          1,
		ScheduledEventId:        1,
		Attempt:                 2,
		LastAttemptCompleteTime: timestamppb.New(startTime),
		ScheduledTime:           timestamppb.New(startTime.Add(10 * time.Second)),
	}

	controller := gomock.NewController(t)
	defer controller.Finish()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := MatchActivity(ai, tt.query)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMatch, match)
			}
		})
	}
}
