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

func convertErrors(
	iter gocql.Iter,
	requestShardID int32,
	requestRangeID int64,
	requestCurrentRunID string,
	requestRunID string,
	requestDBVersion int64,
	requestNextEventID int64, // TODO deprecate this variable once DB version comparison is the default
) error {
	errs := extractErrors(
		iter,
		requestShardID,
		requestRangeID,
		requestCurrentRunID,
		requestRunID,
		requestDBVersion,
		requestNextEventID,
	)
	errs = sortErrors(errs)

	if len(errs) == 0 {
		return nil
	}
	return errs[0]
}

func extractErrors(
	iter gocql.Iter,
	requestShardID int32,
	requestRangeID int64,
	requestCurrentRunID string,
	requestRunID string,
	requestDBVersion int64,
	requestNextEventID int64, // TODO deprecate this variable once DB version comparison is the default
) []error {

	var errors []error

	record := make(map[string]interface{})
	for iter.MapScan(record) {
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

		if err := extractWorkflowVersionConflictError(
			record,
			requestRunID,
			requestDBVersion,
			requestNextEventID,
		); err != nil {
			errors = append(errors, err)
		}

		record = make(map[string]interface{})
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

	// TODO remove this block once DB version comparison is the default
	if requestDBVersion == 0 {
		actualNextEventID := record["next_event_id"].(int64)
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

	actualDBVersion := record["db_version"].(int64)
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
