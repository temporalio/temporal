package mongodb

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

type mongoQueryConverter struct{}

func newMongoQueryConverter() *mongoQueryConverter {
	return &mongoQueryConverter{}
}

func (c *mongoQueryConverter) GetDatetimeFormat() string {
	return time.RFC3339Nano
}

func (c *mongoQueryConverter) BuildParenExpr(expr bson.M) (bson.M, error) {
	return expr, nil
}

func (c *mongoQueryConverter) BuildNotExpr(expr bson.M) (bson.M, error) {
	if len(expr) == 0 {
		return bson.M{}, nil
	}
	return bson.M{"$nor": []bson.M{expr}}, nil
}

func (c *mongoQueryConverter) BuildAndExpr(exprs ...bson.M) (bson.M, error) {
	return mergeExpressions("$and", exprs...), nil
}

func (c *mongoQueryConverter) BuildOrExpr(exprs ...bson.M) (bson.M, error) {
	return mergeExpressions("$or", exprs...), nil
}

func (c *mongoQueryConverter) ConvertComparisonExpr(operator string, col *query.SAColumn, value any) (bson.M, error) {
	return buildComparisonFilter(operator, col, value)
}

func (c *mongoQueryConverter) ConvertKeywordComparisonExpr(operator string, col *query.SAColumn, value any) (bson.M, error) {
	return buildComparisonFilter(operator, col, value)
}

func (c *mongoQueryConverter) ConvertKeywordListComparisonExpr(operator string, col *query.SAColumn, value any) (bson.M, error) {
	return buildComparisonFilter(operator, col, value)
}

func (c *mongoQueryConverter) ConvertTextComparisonExpr(operator string, col *query.SAColumn, value any) (bson.M, error) {
	return buildTextComparisonFilter(operator, col, value)
}

func (c *mongoQueryConverter) ConvertRangeExpr(operator string, col *query.SAColumn, from, to any) (bson.M, error) {
	field, err := mongoFieldName(col.FieldName)
	if err != nil {
		return nil, err
	}
	fromVal, err := normalizeMongoValue(field, col.ValueType, from)
	if err != nil {
		return nil, err
	}
	toVal, err := normalizeMongoValue(field, col.ValueType, to)
	if err != nil {
		return nil, err
	}

	switch operator {
	case sqlparser.BetweenStr:
		return bson.M{field: bson.M{"$gte": fromVal, "$lte": toVal}}, nil
	case sqlparser.NotBetweenStr:
		return bson.M{"$or": []bson.M{
			{field: bson.M{"$lt": fromVal}},
			{field: bson.M{"$gt": toVal}},
		}}, nil
	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported range operator %q", operator))
	}
}

func (c *mongoQueryConverter) ConvertIsExpr(operator string, col *query.SAColumn) (bson.M, error) {
	field, err := mongoFieldName(col.FieldName)
	if err != nil {
		return nil, err
	}

	switch operator {
	case sqlparser.IsNullStr:
		return bson.M{field: bson.M{"$exists": false}}, nil
	case sqlparser.IsNotNullStr:
		return bson.M{field: bson.M{"$exists": true}}, nil
	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported IS operator %q", operator))
	}
}

func mergeExpressions(operator string, exprs ...bson.M) bson.M {
	flattened := make([]bson.M, 0, len(exprs))
	for _, expr := range exprs {
		if len(expr) == 0 {
			continue
		}
		flattened = append(flattened, expr)
	}
	if len(flattened) == 0 {
		return bson.M{}
	}
	if len(flattened) == 1 {
		return flattened[0]
	}
	return bson.M{operator: flattened}
}

func buildComparisonFilter(operator string, col *query.SAColumn, value any) (bson.M, error) {
	field, err := mongoFieldName(col.FieldName)
	if err != nil {
		return nil, err
	}

	normalize := func(v any) (any, error) {
		return normalizeMongoValue(field, col.ValueType, v)
	}

	return buildComparisonFilterForField(operator, field, value, normalize)
}

func buildComparisonFilterForField(operator, field string, value any, normalize func(any) (any, error)) (bson.M, error) {
	switch operator {
	case sqlparser.EqualStr:
		return buildSimpleFilter(field, "", value, normalize)
	case sqlparser.NotEqualStr:
		return buildSimpleFilter(field, "$ne", value, normalize)
	case sqlparser.LessThanStr:
		return buildSimpleFilter(field, "$lt", value, normalize)
	case sqlparser.GreaterThanStr:
		return buildSimpleFilter(field, "$gt", value, normalize)
	case sqlparser.LessEqualStr:
		return buildSimpleFilter(field, "$lte", value, normalize)
	case sqlparser.GreaterEqualStr:
		return buildSimpleFilter(field, "$gte", value, normalize)
	case sqlparser.InStr, sqlparser.NotInStr:
		return buildInFilter(operator, field, value, normalize)
	case sqlparser.StartsWithStr, sqlparser.NotStartsWithStr:
		return buildStartsWithFilter(operator, field, value, normalize)
	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported operator %q", operator))
	}
}

func buildSimpleFilter(field, op string, value any, normalize func(any) (any, error)) (bson.M, error) {
	v, err := normalize(value)
	if err != nil {
		return nil, err
	}
	if op == "" {
		return bson.M{field: v}, nil
	}
	return bson.M{field: bson.M{op: v}}, nil
}

func buildInFilter(operator, field string, value any, normalize func(any) (any, error)) (bson.M, error) {
	values, ok := value.([]any)
	if !ok {
		return nil, serviceerror.NewInvalidArgument("IN operator expects tuple value")
	}
	converted := make([]any, 0, len(values))
	for _, item := range values {
		v, err := normalize(item)
		if err != nil {
			return nil, err
		}
		converted = append(converted, v)
	}
	op := "$in"
	if operator == sqlparser.NotInStr {
		op = "$nin"
	}
	return bson.M{field: bson.M{op: converted}}, nil
}

func buildStartsWithFilter(operator, field string, value any, normalize func(any) (any, error)) (bson.M, error) {
	v, err := normalize(value)
	if err != nil {
		return nil, err
	}
	str, ok := v.(string)
	if !ok {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("expected string for field %s", field))
	}
	pattern := "^" + regexp.QuoteMeta(str)
	regex := primitive.Regex{Pattern: pattern}
	if operator == sqlparser.NotStartsWithStr {
		return bson.M{field: bson.M{"$not": regex}}, nil
	}
	return bson.M{field: regex}, nil
}

// buildTextComparisonFilter builds a filter for Text search attributes.
// Text fields support tokenized matching - each word in the search value is matched
// against words in the document. This mimics Elasticsearch's MatchQuery behavior.
func buildTextComparisonFilter(operator string, col *query.SAColumn, value any) (bson.M, error) {
	field, err := mongoFieldName(col.FieldName)
	if err != nil {
		return nil, err
	}
	str, ok := value.(string)
	if !ok {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("expected string for text field %s", field))
	}

	// Tokenize the search string into words (split by whitespace)
	words := strings.Fields(str)
	if len(words) == 0 {
		return bson.M{field: ""}, nil
	}

	// Build regex patterns that match any word (case-insensitive, word boundary)
	// This mimics Elasticsearch's match query behavior where any word can match
	regexPatterns := make([]bson.M, len(words))
	for i, word := range words {
		// Match word anywhere in the text (case-insensitive)
		pattern := regexp.QuoteMeta(word)
		regexPatterns[i] = bson.M{field: primitive.Regex{Pattern: pattern, Options: "i"}}
	}

	var filter bson.M
	if len(regexPatterns) == 1 {
		filter = regexPatterns[0]
	} else {
		// All words must match (similar to Elasticsearch "AND" operator)
		filter = bson.M{"$and": regexPatterns}
	}

	switch operator {
	case sqlparser.EqualStr:
		return filter, nil
	case sqlparser.NotEqualStr:
		return bson.M{"$nor": []bson.M{filter}}, nil
	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported text operator %q", operator))
	}
}

func mongoFieldName(saField string) (string, error) {
	switch saField {
	case sadefs.WorkflowID:
		return "workflow_id", nil
	case sadefs.RunID:
		return "run_id", nil
	case sadefs.WorkflowType:
		return "workflow_type_name", nil
	case sadefs.TaskQueue:
		return "task_queue", nil
	case sadefs.ExecutionStatus:
		return "status", nil
	case sadefs.StartTime:
		return "start_time", nil
	case sadefs.ExecutionTime:
		return "execution_time", nil
	case sadefs.CloseTime:
		return "close_time", nil
	case sadefs.StateTransitionCount:
		return "state_transition_count", nil
	case sadefs.HistoryLength:
		return "history_length", nil
	case sadefs.HistorySizeBytes:
		return "history_size_bytes", nil
	case sadefs.ExecutionDuration:
		return "execution_duration", nil
	case sadefs.ParentWorkflowID:
		return "parent_workflow_id", nil
	case sadefs.ParentRunID:
		return "parent_run_id", nil
	case sadefs.RootWorkflowID:
		return "root_workflow_id", nil
	default:
		if strings.HasPrefix(saField, sadefs.ReservedPrefix) {
			return "search_attributes." + saField, nil
		}
		return "search_attributes." + saField, nil
	}
}

func normalizeMongoValue(field string, valueType enumspb.IndexedValueType, value any) (any, error) {
	switch valueType {
	case enumspb.INDEXED_VALUE_TYPE_INT:
		if v, ok := value.(int64); ok {
			return v, nil
		}
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("expected int64 for field %s", field))
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		if v, ok := value.(float64); ok {
			return v, nil
		}
		if v, ok := value.(int64); ok {
			return float64(v), nil
		}
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("expected float for field %s", field))
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		if v, ok := value.(bool); ok {
			return v, nil
		}
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("expected bool for field %s", field))
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		str, ok := value.(string)
		if !ok {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("expected datetime string for field %s", field))
		}
		tm, err := time.Parse(time.RFC3339Nano, str)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid datetime for field %s", field))
		}
		return tm, nil
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		enumspb.INDEXED_VALUE_TYPE_TEXT,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
		str, ok := value.(string)
		if !ok {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("expected string for field %s", field))
		}
		if field == "status" {
			status, err := enumspb.WorkflowExecutionStatusFromString(str)
			if err != nil {
				return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid workflow status %q", str))
			}
			return int32(status), nil
		}
		return str, nil
	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported value type %v for field %s", valueType, field))
	}
}

// buildMongoSort converts a SQL-like ORDER BY clause to MongoDB sort.
// Returns default sort (visibility_time DESC, run_id ASC) if orderBy is empty.
func buildMongoSort(orderBy sqlparser.OrderBy) (bson.D, error) {
	if len(orderBy) == 0 {
		return bson.D{
			{Key: "visibility_time", Value: -1},
			{Key: "run_id", Value: 1},
		}, nil
	}

	sort := make(bson.D, 0, len(orderBy))
	for _, orderExpr := range orderBy {
		colName, ok := orderExpr.Expr.(*query.SAColumn)
		if !ok {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid order by expression: %s", sqlparser.String(orderExpr)))
		}

		field, err := mongoFieldName(colName.FieldName)
		if err != nil {
			return nil, err
		}

		direction := 1 // ascending
		if orderExpr.Direction == sqlparser.DescScr {
			direction = -1
		}
		sort = append(sort, bson.E{Key: field, Value: direction})
	}

	return sort, nil
}
