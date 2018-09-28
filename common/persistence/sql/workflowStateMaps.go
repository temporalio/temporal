// Copyright (c) 2018 Uber Technologies, Inc.
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

package sql

import (
	"fmt"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"

	"strings"

	"github.com/jmoiron/sqlx"
)

/*
CRUD methods for the execution row's set/map/list objects.

You need to lock next_event_id before calling any of these.
*/

func stringMap(a []string, f func(string) string) []string {
	b := make([]string, len(a))
	for i, v := range a {
		b[i] = f(v)
	}
	return b
}

func prependColons(a []string) []string {
	return stringMap(a, func(x string) string { return ":" + x })
}

func makeAssignmentsForUpdate(a []string) []string {
	return stringMap(a, func(x string) string { return x + " = :" + x })
}

const (
	deleteMapSQLQueryTemplate = `DELETE FROM %v
WHERE
shard_id = :shard_id AND
domain_id = :domain_id AND
workflow_id = :workflow_id AND
run_id = :run_id`

	// %[2]v is the columns of the value struct (i.e. no primary key columns), comma separated
	// %[3]v should be %[2]v with colons prepended.
	// i.e. %[3]v = ",".join(":" + s for s in %[2]v)
	// So that this query can be used with BindNamed
	// %[4]v should be the name of the key associated with the map
	// e.g. for ActivityInfo it is "schedule_id"
	setKeyInMapSQLQueryTemplate = `REPLACE INTO %[1]v
(shard_id, domain_id, workflow_id, run_id, %[4]v, %[2]v)
VALUES
(:shard_id, :domain_id, :workflow_id, :run_id, :%[4]v, %[3]v)`

	// %[2]v is the name of the key
	deleteKeyInMapSQLQueryTemplate = `DELETE FROM %[1]v
WHERE
shard_id = :shard_id AND
domain_id = :domain_id AND
workflow_id = :workflow_id AND
run_id = :run_id AND
%[2]v = :%[2]v`

	// %[1]v is the name of the table
	// %[2]v is the name of the key
	// %[3]v is the value columns, separated by commas
	getMapSQLQueryTemplate = `SELECT %[2]v, %[3]v FROM %[1]v
WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ?`
)

func makeDeleteMapSQLQuery(tableName string) string {
	return fmt.Sprintf(deleteMapSQLQueryTemplate, tableName)
}

func makeSetKeyInMapSQLQuery(tableName string, nonPrimaryKeyColumns []string, mapKeyName string) string {
	return fmt.Sprintf(setKeyInMapSQLQueryTemplate,
		tableName,
		strings.Join(nonPrimaryKeyColumns, ","),
		strings.Join(stringMap(nonPrimaryKeyColumns, func(x string) string {
			return ":" + x
		}), ","),
		mapKeyName)
}

func makeDeleteKeyInMapSQLQuery(tableName string, mapKeyName string) string {
	return fmt.Sprintf(deleteKeyInMapSQLQueryTemplate,
		tableName,
		mapKeyName)
}

func makeGetMapSQLQueryTemplate(tableName string, nonPrimaryKeyColumns []string, mapKeyName string) string {
	return fmt.Sprintf(getMapSQLQueryTemplate,
		tableName,
		mapKeyName,
		strings.Join(nonPrimaryKeyColumns, ","))
}

var (
	// Omit shard_id, run_id, domain_id, workflow_id, schedule_id since they're in the primary key
	activityInfoColumns = []string{
		"version",
		"scheduled_event",
		"scheduled_time",
		"started_id",
		"started_event",
		"started_time",
		"activity_id",
		"request_id",
		"details",
		"schedule_to_start_timeout",
		"schedule_to_close_timeout",
		"start_to_close_timeout",
		"heartbeat_timeout",
		"cancel_requested",
		"cancel_request_id",
		"last_heartbeat_updated_time",
		"timer_task_status",
		"attempt",
		"task_list",
		"started_identity",
		"has_retry_policy",
		"init_interval",
		"backoff_coefficient",
		"max_interval",
		"expiration_time",
		"max_attempts",
		"non_retriable_errors",
	}
	activityInfoTableName = "activity_info_maps"
	activityInfoKey       = "schedule_id"

	deleteActivityInfoMapSQLQuery      = makeDeleteMapSQLQuery(activityInfoTableName)
	setKeyInActivityInfoMapSQLQuery    = makeSetKeyInMapSQLQuery(activityInfoTableName, activityInfoColumns, activityInfoKey)
	deleteKeyInActivityInfoMapSQLQuery = makeDeleteKeyInMapSQLQuery(activityInfoTableName, activityInfoKey)
	getActivityInfoMapSQLQuery         = makeGetMapSQLQueryTemplate(activityInfoTableName, activityInfoColumns, activityInfoKey)
)

type (
	activityInfoMapsPrimaryKey struct {
		ShardID    int64
		DomainID   string
		WorkflowID string
		RunID      string
		ScheduleID int64
	}

	activityInfoMapsRow struct {
		activityInfoMapsPrimaryKey
		Version                  int64
		ScheduledEvent           *[]byte
		ScheduledTime            time.Time
		StartedID                int64
		StartedEvent             *[]byte
		StartedTime              time.Time
		ActivityID               string
		RequestID                string
		Details                  *[]byte
		ScheduleToStartTimeout   int64
		ScheduleToCloseTimeout   int64
		StartToCloseTimeout      int64
		HeartbeatTimeout         int64
		CancelRequested          int64
		CancelRequestID          int64
		LastHeartbeatUpdatedTime time.Time
		TimerTaskStatus          int64
		Attempt                  int64
		TaskList                 string
		StartedIdentity          string
		HasRetryPolicy           int64
		InitInterval             int64
		BackoffCoefficient       float64
		MaxInterval              int64
		ExpirationTime           time.Time
		MaxAttempts              int64
		NonRetriableErrors       *[]byte
	}
)

func updateActivityInfos(tx *sqlx.Tx,
	activityInfos []*persistence.ActivityInfo,
	deleteInfos []int64,
	shardID int,
	domainID,
	workflowID,
	runID string) error {

	if len(activityInfos) > 0 {
		activityInfoMapsRows := make([]*activityInfoMapsRow, len(activityInfos))
		for i, v := range activityInfos {
			activityInfoMapsRows[i] = &activityInfoMapsRow{
				activityInfoMapsPrimaryKey: activityInfoMapsPrimaryKey{
					ShardID:    int64(shardID),
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					ScheduleID: v.ScheduleID,
				},
				Version:                  v.Version,
				ScheduledEvent:           nil,
				ScheduledTime:            v.ScheduledTime,
				StartedID:                v.StartedID,
				StartedEvent:             nil,
				StartedTime:              v.StartedTime,
				ActivityID:               v.ActivityID,
				RequestID:                v.RequestID,
				ScheduleToStartTimeout:   int64(v.ScheduleToStartTimeout),
				ScheduleToCloseTimeout:   int64(v.ScheduleToCloseTimeout),
				StartToCloseTimeout:      int64(v.StartToCloseTimeout),
				HeartbeatTimeout:         int64(v.HeartbeatTimeout),
				CancelRequested:          boolToInt64(v.CancelRequested),
				CancelRequestID:          v.CancelRequestID,
				LastHeartbeatUpdatedTime: v.LastHeartBeatUpdatedTime,
				TimerTaskStatus:          int64(v.TimerTaskStatus),
				Attempt:                  int64(v.Attempt),
				TaskList:                 v.TaskList,
				StartedIdentity:          v.StartedIdentity,
				HasRetryPolicy:           boolToInt64(v.HasRetryPolicy),
				InitInterval:             int64(v.InitialInterval),
				BackoffCoefficient:       v.BackoffCoefficient,
				MaxInterval:              int64(v.MaximumInterval),
				ExpirationTime:           v.ExpirationTime,
				MaxAttempts:              int64(v.MaximumAttempts),
			}

			if v.Details != nil {
				activityInfoMapsRows[i].Details = &v.Details
			}

			if v.NonRetriableErrors != nil {
				nonRetriableErrors, err := gobSerialize(&v.NonRetriableErrors)
				if err != nil {
					return &workflow.InternalServiceError{
						Message: fmt.Sprintf("Failed to update activity info. Failed to serialize ActivityInfo.NonRetriableErrors. Error: %v", err),
					}
				}
				activityInfoMapsRows[i].NonRetriableErrors = &nonRetriableErrors
			}
		}

		query, args, err := tx.BindNamed(setKeyInActivityInfoMapSQLQuery, activityInfoMapsRows)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update activity info. Failed to bind query. Error: %v", err),
			}
		}

		if _, err := tx.Exec(query, args...); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update activity info. Failed to execute update query. Error: %v", err),
			}
		}
		// There is no sense in checking rowsAffected == len(activityInfo) for a REPLACE query, because
		// it is 1 for each inserted row and 2 for each replaced row

		//rowsAffected, err := result.RowsAffected()
		//if err != nil {
		//	return &workflow.InternalServiceError{
		//		Message: fmt.Sprintf("Failed to update activity info. Failed to verify number of rows updated. Error: %v", err),
		//	}
		//}
		//if rowsAffected == 0 {
		//	return &workflow.InternalServiceError{
		//		Message: fmt.Sprintf("Failed to update activity info. Touched 0 rows. Error: %v", err),
		//	}
		//}
		//if int(rowsAffected) != len(activityInfos) {
		//	return &workflow.InternalServiceError{
		//		Message: fmt.Sprintf("Failed to update activity info. Touched %v rows instead of %v", rowsAffected, len(activityInfos)),
		//	}
		//}
	}

	if len(deleteInfos) > 0 {
		activityInfoMapsPrimaryKeys := make([]*activityInfoMapsPrimaryKey, len(deleteInfos))
		for i, v := range deleteInfos {
			activityInfoMapsPrimaryKeys[i] = &activityInfoMapsPrimaryKey{
				ShardID:    int64(shardID),
				DomainID:   domainID,
				WorkflowID: workflowID,
				RunID:      runID,
				ScheduleID: v,
			}
		}

		query, args, err := tx.BindNamed(deleteKeyInActivityInfoMapSQLQuery, activityInfoMapsPrimaryKeys)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update activity info. Failed to bind query. Error: %v", err),
			}
		}
		result, err := tx.Exec(query, args...)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update activity info. Failed to execute delete query. Error: %v", err),
			}
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update activity info. Failed to verify number of rows deleted. Error: %v", err),
			}
		}
		if int(rowsAffected) != len(deleteInfos) {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update activity info. Deleted %v rows instead of %v", rowsAffected, len(activityInfos)),
			}
		}
	}

	return nil
}

func getActivityInfoMap(tx *sqlx.Tx,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[int64]*persistence.ActivityInfo, error) {
	var activityInfoMapsRows []activityInfoMapsRow

	if err := tx.Select(&activityInfoMapsRows,
		getActivityInfoMapSQLQuery,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get activity info. Error: %v", err),
		}
	}

	ret := make(map[int64]*persistence.ActivityInfo)
	for _, v := range activityInfoMapsRows {
		ret[v.ScheduleID] = &persistence.ActivityInfo{
			Version:                  v.Version,
			ScheduleID:               v.ScheduleID,
			ScheduledEvent:           nil,
			ScheduledTime:            v.ScheduledTime,
			StartedID:                v.StartedID,
			StartedEvent:             nil,
			StartedTime:              v.StartedTime,
			ActivityID:               v.ActivityID,
			RequestID:                v.RequestID,
			ScheduleToStartTimeout:   int32(v.ScheduleToStartTimeout),
			ScheduleToCloseTimeout:   int32(v.ScheduleToCloseTimeout),
			StartToCloseTimeout:      int32(v.StartToCloseTimeout),
			HeartbeatTimeout:         int32(v.HeartbeatTimeout),
			CancelRequested:          int64ToBool(v.CancelRequested),
			CancelRequestID:          v.CancelRequestID,
			LastHeartBeatUpdatedTime: v.LastHeartbeatUpdatedTime,
			TimerTaskStatus:          int32(v.TimerTaskStatus),
			Attempt:                  int32(v.Attempt),
			DomainID:                 v.DomainID,
			StartedIdentity:          v.StartedIdentity,
			TaskList:                 v.TaskList,
			HasRetryPolicy:           int64ToBool(v.HasRetryPolicy),
			InitialInterval:          int32(v.InitInterval),
			BackoffCoefficient:       v.BackoffCoefficient,
			MaximumInterval:          int32(v.MaxInterval),
			ExpirationTime:           v.ExpirationTime,
			MaximumAttempts:          int32(v.MaxAttempts),
		}

		if v.Details != nil {
			ret[v.ScheduleID].Details = *v.Details
		}

		if v.NonRetriableErrors != nil {
			ret[v.ScheduleID].NonRetriableErrors = []string{}
			if err := gobDeserialize(*v.NonRetriableErrors, &ret[v.ScheduleID].NonRetriableErrors); err != nil {
				return nil, &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to get activity info. Failed to deserialize ActivityInfo.NonRetriableErrors. %v", err),
				}
			}
		}
	}

	return ret, nil
}

func deleteActivityInfoMap(tx *sqlx.Tx, shardID int, domainID, workflowID, runID string) error {
	if _, err := tx.NamedExec(deleteActivityInfoMapSQLQuery, &activityInfoMapsPrimaryKey{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to delete activity info map. Error: %v", err),
		}
	}
	return nil
}

var (
	timerInfoColumns = []string{
		"version",
		"started_id",
		"expiry_time",
		"task_id",
	}
	timerInfoTableName = "timer_info_maps"
	timerInfoKey       = "timer_id"

	deleteTimerInfoMapSQLQuery      = makeDeleteMapSQLQuery(timerInfoTableName)
	setKeyInTimerInfoMapSQLQuery    = makeSetKeyInMapSQLQuery(timerInfoTableName, timerInfoColumns, timerInfoKey)
	deleteKeyInTimerInfoMapSQLQuery = makeDeleteKeyInMapSQLQuery(timerInfoTableName, timerInfoKey)
	getTimerInfoMapSQLQuery         = makeGetMapSQLQueryTemplate(timerInfoTableName, timerInfoColumns, timerInfoKey)
)

type (
	timerInfoMapsPrimaryKey struct {
		ShardID    int64
		DomainID   string
		WorkflowID string
		RunID      string
		TimerID    string
	}

	timerInfoMapsRow struct {
		timerInfoMapsPrimaryKey
		Version    int64
		StartedID  int64
		ExpiryTime time.Time
		TaskID     int64
	}
)

func updateTimerInfos(tx *sqlx.Tx,
	timerInfos []*persistence.TimerInfo,
	deleteInfos []string,
	shardID int,
	domainID,
	workflowID,
	runID string) error {
	if len(timerInfos) > 0 {
		timerInfoMapsRows := make([]*timerInfoMapsRow, len(timerInfos))
		for i, v := range timerInfos {
			timerInfoMapsRows[i] = &timerInfoMapsRow{
				timerInfoMapsPrimaryKey: timerInfoMapsPrimaryKey{
					ShardID:    int64(shardID),
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					TimerID:    v.TimerID,
				},
				Version:    v.Version,
				StartedID:  v.StartedID,
				ExpiryTime: v.ExpiryTime,
				TaskID:     v.TaskID,
			}
		}

		query, args, err := tx.BindNamed(setKeyInTimerInfoMapSQLQuery, timerInfoMapsRows)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update timer info. Failed to bind query. Error: %v", err),
			}
		}

		if _, err := tx.Exec(query, args...); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update timer info. Failed to execute update query. Error: %v", err),
			}
		}

	}
	if len(deleteInfos) > 0 {
		timerInfoMapsPrimaryKeys := make([]*timerInfoMapsPrimaryKey, len(deleteInfos))
		for i, v := range deleteInfos {
			timerInfoMapsPrimaryKeys[i] = &timerInfoMapsPrimaryKey{
				ShardID:    int64(shardID),
				DomainID:   domainID,
				WorkflowID: workflowID,
				RunID:      runID,
				TimerID:    v,
			}
		}

		query, args, err := tx.BindNamed(deleteKeyInTimerInfoMapSQLQuery, timerInfoMapsPrimaryKeys)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update timer info. Failed to bind query. Error: %v", err),
			}
		}
		result, err := tx.Exec(query, args...)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update timer info. Failed to execute delete query. Error: %v", err),
			}
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update timer info. Failed to verify number of rows deleted. Error: %v", err),
			}
		}
		if int(rowsAffected) != len(deleteInfos) {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update timer info. Deleted %v rows instead of %v", rowsAffected, len(timerInfos)),
			}
		}
	}
	return nil
}

func getTimerInfoMap(tx *sqlx.Tx,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[string]*persistence.TimerInfo, error) {
	var timerInfoMapsRows []timerInfoMapsRow

	if err := tx.Select(&timerInfoMapsRows,
		getTimerInfoMapSQLQuery,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get timer info. Error: %v", err),
		}
	}

	ret := make(map[string]*persistence.TimerInfo)
	for _, v := range timerInfoMapsRows {
		ret[v.TimerID] = &persistence.TimerInfo{
			Version:    v.Version,
			TimerID:    v.TimerID,
			StartedID:  v.StartedID,
			ExpiryTime: v.ExpiryTime,
			TaskID:     v.TaskID,
		}
	}

	return ret, nil
}

func deleteTimerInfoMap(tx *sqlx.Tx, shardID int, domainID, workflowID, runID string) error {
	if _, err := tx.NamedExec(deleteTimerInfoMapSQLQuery, &timerInfoMapsPrimaryKey{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to delete timer info map. Error: %v", err),
		}
	}
	return nil
}

var (
	childExecutionInfoColumns = []string{
		"version",
		"initiated_event",
		"started_id",
		"started_event",
		"create_request_id",
	}
	childExecutionInfoTableName = "child_execution_info_maps"
	childExecutionInfoKey       = "initiated_id"

	deleteChildExecutionInfoMapSQLQuery      = makeDeleteMapSQLQuery(childExecutionInfoTableName)
	setKeyInChildExecutionInfoMapSQLQuery    = makeSetKeyInMapSQLQuery(childExecutionInfoTableName, childExecutionInfoColumns, childExecutionInfoKey)
	deleteKeyInChildExecutionInfoMapSQLQuery = makeDeleteKeyInMapSQLQuery(childExecutionInfoTableName, childExecutionInfoKey)
	getChildExecutionInfoMapSQLQuery         = makeGetMapSQLQueryTemplate(childExecutionInfoTableName, childExecutionInfoColumns, childExecutionInfoKey)
)

type (
	childExecutionInfoMapsPrimaryKey struct {
		ShardID     int64
		DomainID    string
		WorkflowID  string
		RunID       string
		InitiatedID int64
	}

	childExecutionInfoMapsRow struct {
		childExecutionInfoMapsPrimaryKey
		Version         int64
		InitiatedEvent  *[]byte
		StartedID       int64
		StartedEvent    *[]byte
		CreateRequestID string
	}
)

func updateChildExecutionInfos(tx *sqlx.Tx,
	childExecutionInfos []*persistence.ChildExecutionInfo,
	deleteInfos *int64,
	shardID int,
	domainID,
	workflowID,
	runID string) error {
	if len(childExecutionInfos) > 0 {
		timerInfoMapsRows := make([]*childExecutionInfoMapsRow, len(childExecutionInfos))
		for i, v := range childExecutionInfos {
			timerInfoMapsRows[i] = &childExecutionInfoMapsRow{
				childExecutionInfoMapsPrimaryKey: childExecutionInfoMapsPrimaryKey{
					ShardID:     int64(shardID),
					DomainID:    domainID,
					WorkflowID:  workflowID,
					RunID:       runID,
					InitiatedID: v.InitiatedID,
				},
				Version:         v.Version,
				InitiatedEvent:  nil,
				StartedID:       v.StartedID,
				StartedEvent:    nil,
				CreateRequestID: v.CreateRequestID,
			}
		}

		query, args, err := tx.BindNamed(setKeyInChildExecutionInfoMapSQLQuery, timerInfoMapsRows)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update child execution info. Failed to bind query. Error: %v", err),
			}
		}

		if _, err := tx.Exec(query, args...); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update child execution info. Failed to execute update query. Error: %v", err),
			}
		}

	}
	if deleteInfos != nil {
		if _, err := tx.NamedExec(deleteKeyInChildExecutionInfoMapSQLQuery, &childExecutionInfoMapsPrimaryKey{
			ShardID:     int64(shardID),
			DomainID:    domainID,
			WorkflowID:  workflowID,
			RunID:       runID,
			InitiatedID: *deleteInfos,
		}); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update child execution info. Failed to execute delete query. Error: %v", err),
			}
		}
	}

	return nil
}

func getChildExecutionInfoMap(tx *sqlx.Tx,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[int64]*persistence.ChildExecutionInfo, error) {
	var childExecutionInfoMapsRows []childExecutionInfoMapsRow

	if err := tx.Select(&childExecutionInfoMapsRows,
		getChildExecutionInfoMapSQLQuery,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get timer info. Error: %v", err),
		}
	}

	ret := make(map[int64]*persistence.ChildExecutionInfo)
	for _, v := range childExecutionInfoMapsRows {
		ret[v.InitiatedID] = &persistence.ChildExecutionInfo{
			InitiatedID:     v.InitiatedID,
			Version:         v.Version,
			InitiatedEvent:  nil,
			StartedID:       v.StartedID,
			StartedEvent:    nil,
			CreateRequestID: v.CreateRequestID,
		}

	}

	return ret, nil
}

func deleteChildExecutionInfoMap(tx *sqlx.Tx, shardID int, domainID, workflowID, runID string) error {
	if _, err := tx.NamedExec(deleteChildExecutionInfoMapSQLQuery, &childExecutionInfoMapsPrimaryKey{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to delete timer info map. Error: %v", err),
		}
	}
	return nil
}

var (
	requestCancelInfoColumns = []string{
		"version",
		"cancel_request_id",
	}
	requestCancelInfoTableName = "request_cancel_info_maps"
	requestCancelInfoKey       = "initiated_id"

	deleteRequestCancelInfoMapSQLQuery      = makeDeleteMapSQLQuery(requestCancelInfoTableName)
	setKeyInRequestCancelInfoMapSQLQuery    = makeSetKeyInMapSQLQuery(requestCancelInfoTableName, requestCancelInfoColumns, requestCancelInfoKey)
	deleteKeyInRequestCancelInfoMapSQLQuery = makeDeleteKeyInMapSQLQuery(requestCancelInfoTableName, requestCancelInfoKey)
	getRequestCancelInfoMapSQLQuery         = makeGetMapSQLQueryTemplate(requestCancelInfoTableName, requestCancelInfoColumns, requestCancelInfoKey)
)

type (
	requestCancelInfoMapsPrimaryKey struct {
		ShardID     int64
		DomainID    string
		WorkflowID  string
		RunID       string
		InitiatedID int64
	}

	requestCancelInfoMapsRow struct {
		requestCancelInfoMapsPrimaryKey
		Version         int64
		CancelRequestID string
	}
)

func updateRequestCancelInfos(tx *sqlx.Tx,
	requestCancelInfos []*persistence.RequestCancelInfo,
	deleteInfo *int64,
	shardID int,
	domainID,
	workflowID,
	runID string) error {
	if len(requestCancelInfos) > 0 {
		requestCancelInfoMapsRows := make([]*requestCancelInfoMapsRow, len(requestCancelInfos))
		for i, v := range requestCancelInfos {
			requestCancelInfoMapsRows[i] = &requestCancelInfoMapsRow{
				requestCancelInfoMapsPrimaryKey: requestCancelInfoMapsPrimaryKey{
					ShardID:     int64(shardID),
					DomainID:    domainID,
					WorkflowID:  workflowID,
					RunID:       runID,
					InitiatedID: v.InitiatedID,
				},
				Version:         v.Version,
				CancelRequestID: v.CancelRequestID,
			}
		}

		query, args, err := tx.BindNamed(setKeyInRequestCancelInfoMapSQLQuery, requestCancelInfoMapsRows)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update request cancel info. Failed to bind query. Error: %v", err),
			}
		}

		if _, err := tx.Exec(query, args...); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update request cancel info. Failed to execute update query. Error: %v", err),
			}
		}

	}
	if deleteInfo == nil {
		return nil
	}
	result, err := tx.NamedExec(deleteKeyInRequestCancelInfoMapSQLQuery, &requestCancelInfoMapsPrimaryKey{
		ShardID:     int64(shardID),
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		InitiatedID: *deleteInfo,
	})
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update request cancel info. Failed to execute delete query. Error: %v", err),
		}
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update request cancel info. Failed to verify number of rows deleted. Error: %v", err),
		}
	}
	if int(rowsAffected) != 1 {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update request cancel info. Deleted %v rows instead of 1", rowsAffected),
		}
	}
	return nil
}

func getRequestCancelInfoMap(tx *sqlx.Tx,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[int64]*persistence.RequestCancelInfo, error) {
	var requestCancelInfoMapsRows []requestCancelInfoMapsRow

	if err := tx.Select(&requestCancelInfoMapsRows,
		getRequestCancelInfoMapSQLQuery,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get request cancel info. Error: %v", err),
		}
	}

	ret := make(map[int64]*persistence.RequestCancelInfo)
	for _, v := range requestCancelInfoMapsRows {
		ret[v.InitiatedID] = &persistence.RequestCancelInfo{
			Version:         v.Version,
			CancelRequestID: v.CancelRequestID,
			InitiatedID:     v.InitiatedID,
		}
	}

	return ret, nil
}

func deleteRequestCancelInfoMap(tx *sqlx.Tx, shardID int, domainID, workflowID, runID string) error {
	if _, err := tx.NamedExec(deleteRequestCancelInfoMapSQLQuery, &requestCancelInfoMapsPrimaryKey{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to delete request cancel info map. Error: %v", err),
		}
	}
	return nil
}

var (
	signalInfoColumns = []string{
		"version",
		"signal_request_id",
		"signal_name",
		"input",
		"control",
	}
	signalInfoTableName = "signal_info_maps"
	signalInfoKey       = "initiated_id"

	deleteSignalInfoMapSQLQuery      = makeDeleteMapSQLQuery(signalInfoTableName)
	setKeyInSignalInfoMapSQLQuery    = makeSetKeyInMapSQLQuery(signalInfoTableName, signalInfoColumns, signalInfoKey)
	deleteKeyInSignalInfoMapSQLQuery = makeDeleteKeyInMapSQLQuery(signalInfoTableName, signalInfoKey)
	getSignalInfoMapSQLQuery         = makeGetMapSQLQueryTemplate(signalInfoTableName, signalInfoColumns, signalInfoKey)
)

type (
	signalInfoMapsPrimaryKey struct {
		ShardID     int64
		DomainID    string
		WorkflowID  string
		RunID       string
		InitiatedID int64
	}

	signalInfoMapsRow struct {
		signalInfoMapsPrimaryKey
		Version         int64
		SignalRequestID string
		SignalName      string
		Input           *[]byte
		Control         *[]byte
	}
)

func updateSignalInfos(tx *sqlx.Tx,
	signalInfos []*persistence.SignalInfo,
	deleteInfo *int64,
	shardID int,
	domainID,
	workflowID,
	runID string) error {
	if len(signalInfos) > 0 {
		signalInfoMapsRows := make([]*signalInfoMapsRow, len(signalInfos))
		for i, v := range signalInfos {
			signalInfoMapsRows[i] = &signalInfoMapsRow{
				signalInfoMapsPrimaryKey: signalInfoMapsPrimaryKey{
					ShardID:     int64(shardID),
					DomainID:    domainID,
					WorkflowID:  workflowID,
					RunID:       runID,
					InitiatedID: v.InitiatedID,
				},
				Version:         v.Version,
				SignalRequestID: v.SignalRequestID,
				SignalName:      v.SignalName,
				Input:           takeAddressIfNotNil(v.Input),
				Control:         takeAddressIfNotNil(v.Control),
			}
		}

		query, args, err := tx.BindNamed(setKeyInSignalInfoMapSQLQuery, signalInfoMapsRows)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update signal info. Failed to bind query. Error: %v", err),
			}
		}

		if _, err := tx.Exec(query, args...); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update signal info. Failed to execute update query. Error: %v", err),
			}
		}
	}
	if deleteInfo == nil {
		return nil
	}
	result, err := tx.NamedExec(deleteKeyInSignalInfoMapSQLQuery, &signalInfoMapsPrimaryKey{
		ShardID:     int64(shardID),
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		InitiatedID: *deleteInfo,
	})
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update signal info. Failed to execute delete query. Error: %v", err),
		}
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update signal info. Failed to verify number of rows deleted. Error: %v", err),
		}
	}
	if int(rowsAffected) != 1 {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update signal info. Deleted %v rows instead of 1", rowsAffected),
		}
	}
	return nil
}

func getSignalInfoMap(tx *sqlx.Tx,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[int64]*persistence.SignalInfo, error) {
	var signalInfoMapsRows []signalInfoMapsRow

	if err := tx.Select(&signalInfoMapsRows,
		getSignalInfoMapSQLQuery,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get signal info. Error: %v", err),
		}
	}

	ret := make(map[int64]*persistence.SignalInfo)
	for _, v := range signalInfoMapsRows {
		ret[v.InitiatedID] = &persistence.SignalInfo{
			Version:         v.Version,
			InitiatedID:     v.InitiatedID,
			SignalRequestID: v.SignalRequestID,
			SignalName:      v.SignalName,
			Input:           dereferenceIfNotNil(v.Input),
			Control:         dereferenceIfNotNil(v.Control),
		}
	}

	return ret, nil
}

func deleteSignalInfoMap(tx *sqlx.Tx, shardID int, domainID, workflowID, runID string) error {
	if _, err := tx.NamedExec(deleteSignalInfoMapSQLQuery, &requestCancelInfoMapsPrimaryKey{
		ShardID:    int64(shardID),
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to delete signal info map. Error: %v", err),
		}
	}
	return nil
}

var (
	bufferedReplicationTasksMapColumns = []string{
		"version",
		"next_event_id",
		"history",
		"new_run_history",
	}
	bufferedReplicationTasksTableName = "buffered_replication_task_maps"
	bufferedReplicationTasksKey       = "first_event_id"

	deleteBufferedReplicationTasksMapSQLQuery      = makeDeleteMapSQLQuery(bufferedReplicationTasksTableName)
	setKeyInBufferedReplicationTasksMapSQLQuery    = makeSetKeyInMapSQLQuery(bufferedReplicationTasksTableName, bufferedReplicationTasksMapColumns, bufferedReplicationTasksKey)
	deleteKeyInBufferedReplicationTasksMapSQLQuery = makeDeleteKeyInMapSQLQuery(bufferedReplicationTasksTableName, bufferedReplicationTasksKey)
	getBufferedReplicationTasksMapSQLQuery         = makeGetMapSQLQueryTemplate(bufferedReplicationTasksTableName, bufferedReplicationTasksMapColumns, bufferedReplicationTasksKey)
)

type (
	bufferedReplicationTaskMapsPrimaryKey struct {
		ShardID      int64
		DomainID     string
		WorkflowID   string
		RunID        string
		FirstEventID int64
	}

	bufferedReplicationTaskMapsRow struct {
		bufferedReplicationTaskMapsPrimaryKey
		NextEventID   int64
		Version       int64
		History       *[]byte
		NewRunHistory *[]byte
	}
)

func updateBufferedReplicationTasks(tx *sqlx.Tx,
	newBufferedReplicationTask *persistence.BufferedReplicationTask,
	deleteInfo *int64,
	shardID int,
	domainID,
	workflowID,
	runID string) error {
	if newBufferedReplicationTask != nil {
		arg := &bufferedReplicationTaskMapsRow{
			bufferedReplicationTaskMapsPrimaryKey: bufferedReplicationTaskMapsPrimaryKey{
				ShardID:      int64(shardID),
				DomainID:     domainID,
				WorkflowID:   workflowID,
				RunID:        runID,
				FirstEventID: newBufferedReplicationTask.FirstEventID,
			},
			Version:     newBufferedReplicationTask.Version,
			NextEventID: newBufferedReplicationTask.NextEventID,
		}

		if newBufferedReplicationTask.History != nil {
			b, err := gobSerialize(&newBufferedReplicationTask.History)
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update buffered replication tasks. Failed to serialize a BufferedReplicationTask.History. Error: %v", err),
				}
			}
			arg.History = &b
		}

		if newBufferedReplicationTask.NewRunHistory != nil {
			b, err := gobSerialize(&newBufferedReplicationTask.NewRunHistory)
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update buffered replication tasks. Failed to serialize a BufferedReplicationTask.NewRunHistory. Error: %v", err),
				}
			}
			arg.NewRunHistory = &b
		}

		if _, err := tx.NamedExec(setKeyInBufferedReplicationTasksMapSQLQuery, arg); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update buffered replication tasks. Failed to execute update query. Error: %v", err),
			}
		}

	}
	if deleteInfo == nil {
		return nil
	}
	result, err := tx.NamedExec(deleteKeyInBufferedReplicationTasksMapSQLQuery, &bufferedReplicationTaskMapsPrimaryKey{
		ShardID:      int64(shardID),
		DomainID:     domainID,
		WorkflowID:   workflowID,
		RunID:        runID,
		FirstEventID: *deleteInfo,
	})
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update buffered replication tasks. Failed to execute delete query. Error: %v", err),
		}
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update buffered replication tasks. Failed to verify number of rows deleted. Error: %v", err),
		}
	}
	if int(rowsAffected) != 1 {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update buffered replication tasks. Deleted %v rows instead of 1", rowsAffected),
		}
	}
	return nil
}

func getBufferedReplicationTasks(tx *sqlx.Tx,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[int64]*persistence.BufferedReplicationTask, error) {
	var bufferedReplicationTaskMapsRows []bufferedReplicationTaskMapsRow

	if err := tx.Select(&bufferedReplicationTaskMapsRows,
		getBufferedReplicationTasksMapSQLQuery,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get buffered replication tasks. Error: %v", err),
		}
	}

	ret := make(map[int64]*persistence.BufferedReplicationTask)
	for _, v := range bufferedReplicationTaskMapsRows {
		ret[v.FirstEventID] = &persistence.BufferedReplicationTask{
			Version:      v.Version,
			FirstEventID: v.FirstEventID,
			NextEventID:  v.NextEventID,
		}

		if v.History != nil {
			ret[v.FirstEventID].History = make([]*workflow.HistoryEvent, 0)
			if err := gobDeserialize(*v.History, &ret[v.FirstEventID].History); err != nil {
				return nil, &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to get buffered replication tasks. Failed to deserialize a BufferedReplicationTask.History. Error: %v", err),
				}
			}
		}

		if v.NewRunHistory != nil {
			ret[v.FirstEventID].NewRunHistory = make([]*workflow.HistoryEvent, 0)
			if err := gobDeserialize(*v.NewRunHistory, &ret[v.FirstEventID].NewRunHistory); err != nil {
				return nil, &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to get buffered replication tasks. Failed to deserialize a BufferedReplicationTask.NewRunHistory. Error: %v", err),
				}
			}
		}
	}

	return ret, nil
}

//
//func deleteBufferedReplicationTaskMap(tx *sqlx.Tx, shardID int, domainID, workflowID, runID string) error {
//	if _, err := tx.Exec(deleteBufferedReplicationTasksMapSQLQuery, &bufferedReplicationTaskMapsPrimaryKey{
//		ShardID: int64(shardID),
//		DomainID: domainID,
//		WorkflowID: workflowID,
//		RunID: runID,
//	}); err != nil {
//		return &workflow.InternalServiceError{
//			Message: fmt.Sprintf("Failed to delete buffered replication task map. Error: %v", err),
//		}
//	}
//	return nil
//}
