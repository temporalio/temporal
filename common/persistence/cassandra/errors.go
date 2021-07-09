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

package cassandra

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

var (
	errorPriority = map[reflect.Type]int{
		reflect.TypeOf(&p.ShardOwnershipLostError{}):             0,
		reflect.TypeOf(&p.CurrentWorkflowConditionFailedError{}): 1,
		reflect.TypeOf(&p.ConditionFailedError{}):                2,
	}

	errorDefaultPriority = math.MaxInt64
)

type (
	executionCASCondition struct {
		runID       string
		dbVersion   int64
		nextEventID int64 // TODO deprecate this variable once DB version comparison is the default
	}
)

func convertErrors(
	record map[string]interface{},
	iter gocql.Iter,
	requestShardID int32,
	requestRangeID int64,
	requestCurrentRunID string,
	requestExecutionCASConditions []executionCASCondition,
) error {

	records := []map[string]interface{}{record}
	errors := extractErrors(
		record,
		requestShardID,
		requestRangeID,
		requestCurrentRunID,
		requestExecutionCASConditions,
	)

	record = make(map[string]interface{})
	for iter.MapScan(record) {
		records = append(records, record)
		errors = append(errors, extractErrors(
			record,
			requestShardID,
			requestRangeID,
			requestCurrentRunID,
			requestExecutionCASConditions,
		)...)

		record = make(map[string]interface{})
	}

	errors = sortErrors(errors)
	if len(errors) == 0 {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("Encounter unknown error: shard ID: %v, range ID: %v, error: %v",
				requestShardID,
				requestRangeID,
				printRecords(records),
			),
		}
	}
	return errors[0]
}

func extractErrors(
	record map[string]interface{},
	requestShardID int32,
	requestRangeID int64,
	requestCurrentRunID string,
	requestExecutionCASConditions []executionCASCondition,
) []error {

	var errors []error

	if err := extractShardOwnershipLostError(
		record,
		requestShardID,
		requestRangeID,
	); err != nil {
		errors = append(errors, err)
	}

	if err := extractCurrentWorkflowConflictError(
		record,
		requestCurrentRunID,
	); err != nil {
		errors = append(errors, err)
	}

	for _, condition := range requestExecutionCASConditions {
		if err := extractWorkflowVersionConflictError(
			record,
			condition.runID,
			condition.dbVersion,
			condition.nextEventID,
		); err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

func sortErrors(
	errors []error,
) []error {
	sort.Slice(errors, func(i int, j int) bool {
		leftPriority, ok := errorPriority[reflect.TypeOf(errors[i])]
		if !ok {
			leftPriority = errorDefaultPriority
		}
		rightPriority, ok := errorPriority[reflect.TypeOf(errors[j])]
		if !ok {
			rightPriority = errorDefaultPriority
		}
		return leftPriority < rightPriority
	})
	return errors
}

func extractShardOwnershipLostError(
	record map[string]interface{},
	requestShardID int32,
	requestRangeID int64,
) error {
	rowType, ok := record["type"].(int)
	if !ok {
		// this case should not happen, maybe panic?
		return nil
	}
	if rowType != rowTypeShard {
		return nil
	}

	actualRangeID := record["range_id"].(int64)
	if actualRangeID != requestRangeID {
		return &p.ShardOwnershipLostError{
			ShardID: requestShardID,
			Msg: fmt.Sprintf("Encounter shard ownership lost, request range ID: %v, actual range ID: %v",
				requestRangeID,
				actualRangeID,
			),
		}
	}
	return nil
}

func extractCurrentWorkflowConflictError(
	record map[string]interface{},
	requestCurrentRunID string,
) error {
	rowType, ok := record["type"].(int)
	if !ok {
		// this case should not happen, maybe panic?
		return nil
	}
	if rowType != rowTypeExecution {
		return nil
	}
	if runID := gocql.UUIDToString(record["run_id"]); runID != permanentRunID {
		return nil
	}

	actualCurrentRunID := gocql.UUIDToString(record["current_run_id"])
	if actualCurrentRunID != requestCurrentRunID {
		return &p.CurrentWorkflowConditionFailedError{
			Msg: fmt.Sprintf("Encounter concurrent workflow error, request run ID: %v, actual run ID: %v",
				requestCurrentRunID,
				actualCurrentRunID,
			),
		}
	}
	return nil
}

func extractWorkflowVersionConflictError(
	record map[string]interface{},
	requestRunID string,
	requestDBVersion int64,
	requestNextEventID int64, // TODO deprecate this variable once DB version comparison is the default
) error {
	rowType, ok := record["type"].(int)
	if !ok {
		// this case should not happen, maybe panic?
		return nil
	}
	if rowType != rowTypeExecution {
		return nil
	}
	if runID := gocql.UUIDToString(record["run_id"]); runID != requestRunID {
		return nil
	}

	actualNextEventID, _ := record["next_event_id"].(int64)
	actualDBVersion, _ := record["db_version"].(int64)

	// TODO remove this block once DB version comparison is the default
	if requestDBVersion == 0 {
		if actualNextEventID != requestNextEventID {
			return &p.ConditionFailedError{
				Msg: fmt.Sprintf("Encounter workflow next event ID mismatch, request next event ID: %v, actual next event ID: %v",
					requestNextEventID,
					actualNextEventID,
				),
			}
		}
		return nil
	}

	if actualDBVersion != requestDBVersion {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("Encounter workflow db version mismatch, request db version ID: %v, actual db version ID: %v",
				requestDBVersion,
				actualDBVersion,
			),
		}
	}
	return nil
}

func printRecords(
	records []map[string]interface{},
) string {
	binary, _ := json.MarshalIndent(records, "", "  ")
	return string(binary)
}
