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

package sqlplugin

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/iancoleman/strcase"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/searchattribute"
)

var (
	ErrInvalidKeywordListDataType = errors.New("Unexpected data type in keyword list")
)

type (
	// VisibilitySearchAttributes represents the search attributes json
	// in executions_visibility table
	VisibilitySearchAttributes map[string]interface{}

	// VisibilityRow represents a row in executions_visibility table
	VisibilityRow struct {
		NamespaceID          string
		RunID                string
		WorkflowTypeName     string
		WorkflowID           string
		StartTime            time.Time
		ExecutionTime        time.Time
		Status               int32
		CloseTime            *time.Time
		HistoryLength        *int64
		HistorySizeBytes     *int64
		ExecutionDuration    *time.Duration
		StateTransitionCount *int64
		Memo                 []byte
		Encoding             string
		TaskQueue            string
		SearchAttributes     *VisibilitySearchAttributes
		ParentWorkflowID     *string
		ParentRunID          *string
		RootWorkflowID       string
		RootRunID            string
	}

	// VisibilitySelectFilter contains the column names within executions_visibility table that
	// can be used to filter results through a WHERE clause
	VisibilitySelectFilter struct {
		NamespaceID      string
		RunID            *string
		WorkflowID       *string
		WorkflowTypeName *string
		Status           int32
		MinTime          *time.Time
		MaxTime          *time.Time
		PageSize         *int

		Query     string
		QueryArgs []interface{}
		GroupBy   []string
	}

	VisibilityGetFilter struct {
		NamespaceID string
		RunID       string
	}

	VisibilityDeleteFilter struct {
		NamespaceID string
		RunID       string
	}

	VisibilityCountRow struct {
		GroupValues []any
		Count       int64
	}

	Visibility interface {
		// InsertIntoVisibility inserts a row into visibility table. If a row already exist,
		// no changes will be made by this API
		InsertIntoVisibility(ctx context.Context, row *VisibilityRow) (sql.Result, error)
		// ReplaceIntoVisibility deletes old row (if it exist) and inserts new row into visibility table
		ReplaceIntoVisibility(ctx context.Context, row *VisibilityRow) (sql.Result, error)
		// SelectFromVisibility returns one or more rows from visibility table
		// Required filter params:
		// - getClosedWorkflowExecution - retrieves single row - {namespaceID, runID, closed=true}
		// - All other queries retrieve multiple rows (range):
		//   - MUST specify following required params:
		//     - namespaceID, minStartTime, maxStartTime, runID and pageSize where some or all of these may come from previous page token
		//   - OPTIONALLY specify one of following params
		//     - workflowID, workflowTypeName, status (along with closed=true)
		SelectFromVisibility(ctx context.Context, filter VisibilitySelectFilter) ([]VisibilityRow, error)
		GetFromVisibility(ctx context.Context, filter VisibilityGetFilter) (*VisibilityRow, error)
		DeleteFromVisibility(ctx context.Context, filter VisibilityDeleteFilter) (sql.Result, error)
		CountFromVisibility(ctx context.Context, filter VisibilitySelectFilter) (int64, error)
		CountGroupByFromVisibility(ctx context.Context, filter VisibilitySelectFilter) ([]VisibilityCountRow, error)
	}
)

var _ sql.Scanner = (*VisibilitySearchAttributes)(nil)
var _ driver.Valuer = (*VisibilitySearchAttributes)(nil)

var DbFields = getDbFields()

func (vsa *VisibilitySearchAttributes) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	switch v := src.(type) {
	case []byte:
		return json.Unmarshal(v, &vsa)
	case string:
		return json.Unmarshal([]byte(v), &vsa)
	default:
		return fmt.Errorf("unsupported type for VisibilitySearchAttributes: %T", v)
	}
}

func (vsa VisibilitySearchAttributes) Value() (driver.Value, error) {
	if vsa == nil {
		return nil, nil
	}
	bs, err := json.Marshal(vsa)
	if err != nil {
		return nil, err
	}
	return string(bs), nil
}

func ParseCountGroupByRows(rows *sql.Rows, groupBy []string) ([]VisibilityCountRow, error) {
	// Number of columns is number of group by fields plus the count column.
	rowValues := make([]any, len(groupBy)+1)
	for i := range rowValues {
		rowValues[i] = new(any)
	}

	var res []VisibilityCountRow
	for rows.Next() {
		err := rows.Scan(rowValues...)
		if err != nil {
			return nil, err
		}
		groupValues := make([]any, len(groupBy))
		for i := range groupBy {
			groupValues[i], err = parseCountGroupByGroupValue(groupBy[i], *(rowValues[i].(*any)))
			if err != nil {
				return nil, err
			}
		}
		count := *(rowValues[len(rowValues)-1].(*any))
		res = append(res, VisibilityCountRow{
			GroupValues: groupValues,
			Count:       count.(int64),
		})
	}
	return res, nil
}

func parseCountGroupByGroupValue(fieldName string, value any) (any, error) {
	switch fieldName {
	case searchattribute.ExecutionStatus:
		switch typedValue := value.(type) {
		case int:
			return enumspb.WorkflowExecutionStatus(typedValue).String(), nil
		case int32:
			return enumspb.WorkflowExecutionStatus(typedValue).String(), nil
		case int64:
			return enumspb.WorkflowExecutionStatus(typedValue).String(), nil
		default:
			// This should never happen.
			return nil, serviceerror.NewInternal(
				fmt.Sprintf(
					"Unable to parse %s value from DB (got: %v of type: %T, expected type: integer)",
					searchattribute.ExecutionStatus,
					value,
					value,
				),
			)
		}
	default:
		return value, nil
	}
}

func getDbFields() []string {
	t := reflect.TypeOf(VisibilityRow{})
	dbFields := make([]string, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		dbFields[i] = f.Tag.Get("db")
		if dbFields[i] == "" {
			dbFields[i] = strcase.ToSnake(f.Name)
		}
	}
	return dbFields
}

// TODO (rodrigozhou): deprecate with standard visibility code.
// GenerateSelectQuery generates the SELECT query based on the fields of VisibilitySelectFilter
// for backward compatibility of any use case using old format (eg: unit test).
// It will be removed after all use cases change to use query converter.
func GenerateSelectQuery(
	filter *VisibilitySelectFilter,
	convertToDbDateTime func(time.Time) time.Time,
) error {
	whereClauses := make([]string, 0, 10)
	queryArgs := make([]interface{}, 0, 10)

	whereClauses = append(
		whereClauses,
		fmt.Sprintf("%s = ?", searchattribute.GetSqlDbColName(searchattribute.NamespaceID)),
	)
	queryArgs = append(queryArgs, filter.NamespaceID)

	if filter.WorkflowID != nil {
		whereClauses = append(
			whereClauses,
			fmt.Sprintf("%s = ?", searchattribute.GetSqlDbColName(searchattribute.WorkflowID)),
		)
		queryArgs = append(queryArgs, *filter.WorkflowID)
	}

	if filter.WorkflowTypeName != nil {
		whereClauses = append(
			whereClauses,
			fmt.Sprintf("%s = ?", searchattribute.GetSqlDbColName(searchattribute.WorkflowType)),
		)
		queryArgs = append(queryArgs, *filter.WorkflowTypeName)
	}

	timeAttr := searchattribute.StartTime
	if filter.Status != int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING) {
		timeAttr = searchattribute.CloseTime
	}
	if filter.Status == int32(enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED) {
		whereClauses = append(
			whereClauses,
			fmt.Sprintf("%s != ?", searchattribute.GetSqlDbColName(searchattribute.ExecutionStatus)),
		)
		queryArgs = append(queryArgs, int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))
	} else {
		whereClauses = append(
			whereClauses,
			fmt.Sprintf("%s = ?", searchattribute.GetSqlDbColName(searchattribute.ExecutionStatus)),
		)
		queryArgs = append(queryArgs, filter.Status)
	}

	switch {
	case filter.RunID != nil && filter.MinTime == nil && filter.Status != 1:
		whereClauses = append(
			whereClauses,
			fmt.Sprintf("%s = ?", searchattribute.GetSqlDbColName(searchattribute.RunID)),
		)
		queryArgs = append(
			queryArgs,
			*filter.RunID,
			1, // page size arg
		)
	case filter.RunID != nil && filter.MinTime != nil && filter.MaxTime != nil && filter.PageSize != nil:
		// pagination filters
		*filter.MinTime = convertToDbDateTime(*filter.MinTime)
		*filter.MaxTime = convertToDbDateTime(*filter.MaxTime)
		whereClauses = append(
			whereClauses,
			fmt.Sprintf("%s >= ?", searchattribute.GetSqlDbColName(timeAttr)),
			fmt.Sprintf("%s <= ?", searchattribute.GetSqlDbColName(timeAttr)),
			fmt.Sprintf(
				"((%s = ? AND %s > ?) OR %s < ?)",
				searchattribute.GetSqlDbColName(timeAttr),
				searchattribute.GetSqlDbColName(searchattribute.RunID),
				searchattribute.GetSqlDbColName(timeAttr),
			),
		)
		queryArgs = append(
			queryArgs,
			*filter.MinTime,
			*filter.MaxTime,
			*filter.MaxTime,
			*filter.RunID,
			*filter.MaxTime,
			*filter.PageSize,
		)
	default:
		return fmt.Errorf("invalid query filter")
	}

	filter.Query = fmt.Sprintf(
		`SELECT %s FROM executions_visibility
		WHERE %s
		ORDER BY %s DESC, %s
		LIMIT ?`,
		strings.Join(DbFields, ", "),
		strings.Join(whereClauses, " AND "),
		searchattribute.GetSqlDbColName(timeAttr),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
	)
	filter.QueryArgs = queryArgs
	return nil
}
