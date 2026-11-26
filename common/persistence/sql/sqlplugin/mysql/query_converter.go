package mysql

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

var maxDatetime = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

type (
	castExpr struct {
		sqlparser.Expr
		Value sqlparser.Expr
		Type  *sqlparser.ConvertType
	}

	memberOfExpr struct {
		sqlparser.Expr
		Value   sqlparser.Expr
		JSONArr sqlparser.Expr
	}

	jsonOverlapsExpr struct {
		sqlparser.Expr
		JSONDoc1 sqlparser.Expr
		JSONDoc2 sqlparser.Expr
	}
)

var (
	convertTypeDatetime = &sqlparser.ConvertType{Type: "datetime"}
	convertTypeJSON     = &sqlparser.ConvertType{Type: "json"}
)

var _ sqlparser.Expr = (*castExpr)(nil)
var _ sqlparser.Expr = (*memberOfExpr)(nil)
var _ sqlparser.Expr = (*jsonOverlapsExpr)(nil)

func (node *castExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("cast(%v as %v)", node.Value, node.Type)
}

func (node *memberOfExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("%v member of (%v)", node.Value, node.JSONArr)
}

func (node *jsonOverlapsExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("json_overlaps(%v, %v)", node.JSONDoc1, node.JSONDoc2)
}

type queryConverter struct{}

var _ sqlplugin.VisibilityQueryConverter = (*queryConverter)(nil)

func (c *queryConverter) GetDatetimeFormat() string {
	return "2006-01-02 15:04:05.999999"
}

func (c *queryConverter) GetCoalesceCloseTimeExpr() sqlparser.Expr {
	return query.NewFuncExpr(
		"coalesce",
		query.CloseTimeSAColumn,
		&castExpr{
			Value: query.NewUnsafeSQLString(maxDatetime.Format(c.GetDatetimeFormat())),
			Type:  convertTypeDatetime,
		},
	)
}

func (c *queryConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *query.SAColumn,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	var negate bool
	var newExpr sqlparser.Expr
	switch operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		newExpr = &memberOfExpr{
			Value:   value,
			JSONArr: col,
		}
		negate = operator == sqlparser.NotEqualStr
	case sqlparser.InStr, sqlparser.NotInStr:
		var err error
		newExpr, err = c.buildJSONOverlapsExpr(col, value)
		if err != nil {
			return nil, err
		}
		negate = operator == sqlparser.NotInStr
	default:
		// this should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type",
			query.InvalidExpressionErrMessage,
			operator,
		)
	}

	if negate {
		newExpr = &sqlparser.NotExpr{Expr: newExpr}
	}
	return newExpr, nil
}

func (c *queryConverter) ConvertTextComparisonExpr(
	operator string,
	col *query.SAColumn,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	// build the following expression:
	// `match ({col}) against ({value} in natural language mode)`
	var newExpr sqlparser.Expr = &sqlparser.MatchExpr{
		Columns: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: col}},
		Expr:    value,
		Option:  sqlparser.NaturalLanguageModeStr,
	}
	if operator == sqlparser.NotEqualStr {
		newExpr = &sqlparser.NotExpr{Expr: newExpr}
	}
	return newExpr, nil
}

func (c *queryConverter) BuildSelectStmt(
	queryParams *query.QueryParams[sqlparser.Expr],
	pageSize int,
	token *sqlplugin.VisibilityPageToken,
) (string, []any) {
	var whereClauses []string
	var queryArgs []any

	if queryParams.QueryExpr != nil {
		if queryString := sqlparser.String(queryParams.QueryExpr); queryString != "" {
			whereClauses = append(whereClauses, "("+queryString+")")
		}
	}

	if token != nil {
		whereClauses = append(
			whereClauses,
			fmt.Sprintf(
				"((%s = ? AND %s = ? AND %s > ?) OR (%s = ? AND %s < ?) OR %s < ?)",
				sqlparser.String(c.GetCoalesceCloseTimeExpr()),
				sadefs.GetSqlDbColName(sadefs.StartTime),
				sadefs.GetSqlDbColName(sadefs.RunID),
				sqlparser.String(c.GetCoalesceCloseTimeExpr()),
				sadefs.GetSqlDbColName(sadefs.StartTime),
				sqlparser.String(c.GetCoalesceCloseTimeExpr()),
			),
		)
		queryArgs = append(
			queryArgs,
			token.CloseTime,
			token.StartTime,
			token.RunID,
			token.CloseTime,
			token.StartTime,
			token.CloseTime,
		)
	}

	dbFields := make([]string, len(sqlplugin.DbFields))
	for i, field := range sqlplugin.DbFields {
		dbFields[i] = "ev." + field
	}

	whereString := ""
	if len(whereClauses) > 0 {
		whereString = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	stmt := fmt.Sprintf(
		"SELECT %s FROM executions_visibility ev "+
			"LEFT JOIN custom_search_attributes USING (%s, %s) "+
			"LEFT JOIN chasm_search_attributes USING (%s, %s)"+
			"%s "+
			"ORDER BY %s DESC, %s DESC, %s LIMIT ?",
		strings.Join(dbFields, ", "),
		sadefs.GetSqlDbColName(sadefs.NamespaceID),
		sadefs.GetSqlDbColName(sadefs.RunID),
		sadefs.GetSqlDbColName(sadefs.NamespaceID),
		sadefs.GetSqlDbColName(sadefs.RunID),
		whereString,
		sqlparser.String(c.GetCoalesceCloseTimeExpr()),
		sadefs.GetSqlDbColName(sadefs.StartTime),
		sadefs.GetSqlDbColName(sadefs.RunID),
	)
	queryArgs = append(queryArgs, pageSize)

	return stmt, queryArgs
}

func (c *queryConverter) BuildCountStmt(
	queryParams *query.QueryParams[sqlparser.Expr],
) (string, []any) {
	whereString := ""
	if queryParams.QueryExpr != nil {
		whereString = sqlparser.String(queryParams.QueryExpr)
		if whereString != "" {
			whereString = " WHERE " + whereString
		}
	}

	groupBy := make([]string, 0, len(queryParams.GroupBy)+1)
	for _, field := range queryParams.GroupBy {
		groupBy = append(groupBy, sadefs.GetSqlDbColName(field.FieldName))
	}

	groupByClause := ""
	if len(queryParams.GroupBy) > 0 {
		groupByClause = fmt.Sprintf(" GROUP BY %s", strings.Join(groupBy, ", "))
	}

	// The joins make MySQL count far more slowly (#10991), so skip them when every
	// column the query touches lives on executions_visibility itself.
	joinClause := ""
	if countNeedsJoins(queryParams) {
		joinClause = fmt.Sprintf(
			" LEFT JOIN custom_search_attributes USING (%s, %s)"+
				" LEFT JOIN chasm_search_attributes USING (%s, %s)",
			sadefs.GetSqlDbColName(sadefs.NamespaceID),
			sadefs.GetSqlDbColName(sadefs.RunID),
			sadefs.GetSqlDbColName(sadefs.NamespaceID),
			sadefs.GetSqlDbColName(sadefs.RunID),
		)
	}

	return fmt.Sprintf(
		"SELECT %s FROM executions_visibility ev%s%s%s",
		strings.Join(append(groupBy, "COUNT(*)"), ", "),
		joinClause,
		whereString,
		groupByClause,
	), nil
}

func countNeedsJoins(queryParams *query.QueryParams[sqlparser.Expr]) bool {
	for _, col := range queryParams.GroupBy {
		if !isExecutionsVisibilityColumn(col) {
			return true
		}
	}
	return exprNeedsJoins(queryParams.QueryExpr)
}

// isExecutionsVisibilityColumn reports whether col is stored on executions_visibility.
// Preallocated custom search attributes (Keyword01, ...) live in custom_search_attributes
// and CHASM ones (TemporalKeyword01, ...) in chasm_search_attributes.
func isExecutionsVisibilityColumn(col *query.SAColumn) bool {
	return sadefs.IsReserved(col.FieldName) &&
		!sadefs.IsChasmSearchAttribute(col.FieldName) &&
		!sadefs.IsPreallocatedCSAFieldName(col.FieldName, col.ValueType)
}

// exprNeedsJoins answers true for any node it doesn't recognize, so a new expression
// type can only cost performance, never produce a query that references a missing column.
func exprNeedsJoins(expr sqlparser.Expr) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case *query.SAColumn:
		return !isExecutionsVisibilityColumn(e)
	case *query.UnsafeSQLString, *sqlparser.SQLVal, sqlparser.BoolVal, *sqlparser.NullVal:
		return false
	case sqlparser.ValTuple:
		for _, v := range e {
			if exprNeedsJoins(v) {
				return true
			}
		}
		return false
	case *sqlparser.AndExpr:
		return exprNeedsJoins(e.Left) || exprNeedsJoins(e.Right)
	case *sqlparser.OrExpr:
		return exprNeedsJoins(e.Left) || exprNeedsJoins(e.Right)
	case *sqlparser.NotExpr:
		return exprNeedsJoins(e.Expr)
	case *sqlparser.ParenExpr:
		return exprNeedsJoins(e.Expr)
	case *sqlparser.ComparisonExpr:
		return exprNeedsJoins(e.Left) || exprNeedsJoins(e.Right) || exprNeedsJoins(e.Escape)
	case *sqlparser.RangeCond:
		return exprNeedsJoins(e.Left) || exprNeedsJoins(e.From) || exprNeedsJoins(e.To)
	case *sqlparser.IsExpr:
		return exprNeedsJoins(e.Expr)
	case *sqlparser.FuncExpr:
		return selectExprsNeedJoins(e.Exprs)
	case *sqlparser.MatchExpr:
		return selectExprsNeedJoins(e.Columns) || exprNeedsJoins(e.Expr)
	case *castExpr:
		return exprNeedsJoins(e.Value)
	case *memberOfExpr:
		return exprNeedsJoins(e.Value) || exprNeedsJoins(e.JSONArr)
	case *jsonOverlapsExpr:
		return exprNeedsJoins(e.JSONDoc1) || exprNeedsJoins(e.JSONDoc2)
	default:
		return true
	}
}

func selectExprsNeedJoins(exprs sqlparser.SelectExprs) bool {
	for _, se := range exprs {
		aliased, ok := se.(*sqlparser.AliasedExpr)
		if !ok || exprNeedsJoins(aliased.Expr) {
			return true
		}
	}
	return false
}

func (c *queryConverter) buildJSONOverlapsExpr(
	col *query.SAColumn,
	value sqlparser.Expr,
) (*jsonOverlapsExpr, error) {
	valTuple, isValTuple := value.(sqlparser.ValTuple)
	if !isValTuple {
		return nil, query.NewConverterError(
			"%s: unexpected value type (expected tuple of strings, got %s)",
			query.InvalidExpressionErrMessage,
			sqlparser.String(value),
		)
	}
	values, err := query.GetUnsafeStringTupleValues(valTuple)
	if err != nil {
		return nil, err
	}
	jsonValue, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	return &jsonOverlapsExpr{
		JSONDoc1: col,
		JSONDoc2: &castExpr{
			Value: query.NewUnsafeSQLString(string(jsonValue)),
			Type:  convertTypeJSON,
		},
	}, nil
}
