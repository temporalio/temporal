package sql

import (
	"strconv"
	"strings"

	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

const (
	// Default escape char is set explicitly to '!' for two reasons:
	// 1. SQLite doesn't have a default escape char;
	// 2. MySQL requires to escape the backslack char unlike SQLite and PostgreSQL.
	// Thus, in order to avoid having specific code for each DB, it's better to
	// set the escape char to a simpler char that doesn't require escaping.
	defaultLikeEscapeChar = '!'
)

var (
	// strings.Replacer takes a sequence of old to new replacements
	escapeCharMap = []string{
		"'", "''",
		"\"", "\\\"",
		"\b", "\\b",
		"\n", "\\n",
		"\r", "\\r",
		"\t", "\\t",
		"\\", "\\\\",
	}

	defaultLikeEscapeExpr = query.NewUnsafeSQLString(string(defaultLikeEscapeChar))
)

type (
	convertComparisonExprIf interface {
		ConvertComparisonExpr(
			operator string,
			col *query.SAColumn,
			value sqlparser.Expr,
		) (sqlparser.Expr, error)
	}

	convertKeywordComparisonExprIf interface {
		ConvertKeywordComparisonExpr(
			operator string,
			col *query.SAColumn,
			value sqlparser.Expr,
		) (sqlparser.Expr, error)
	}

	convertRangeExprIf interface {
		ConvertRangeExpr(
			operator string,
			col *query.SAColumn,
			from, to sqlparser.Expr,
		) (sqlparser.Expr, error)
	}

	convertIsExprIf interface {
		ConvertIsExpr(
			operator string,
			col *query.SAColumn,
		) (sqlparser.Expr, error)
	}
)

type SQLQueryConverter struct {
	sqlplugin.VisibilityQueryConverter
}

var _ query.StoreQueryConverter[sqlparser.Expr] = (*SQLQueryConverter)(nil)

func NewSQLQueryConverter(pluginName string) (*SQLQueryConverter, error) {
	visQueryConverter, err := sql.GetPluginVisibilityQueryConverter(pluginName)
	if err != nil {
		return nil, err
	}
	return &SQLQueryConverter{VisibilityQueryConverter: visQueryConverter}, nil
}

func (c *SQLQueryConverter) BuildParenExpr(expr sqlparser.Expr) (sqlparser.Expr, error) {
	switch expr.(type) {
	case *sqlparser.NotExpr, *sqlparser.AndExpr, *sqlparser.OrExpr:
		return &sqlparser.ParenExpr{Expr: expr}, nil
	default:
		return expr, nil
	}
}

func (c *SQLQueryConverter) BuildNotExpr(expr sqlparser.Expr) (sqlparser.Expr, error) {
	expr, _ = c.BuildParenExpr(expr)
	return &sqlparser.NotExpr{Expr: expr}, nil
}

func (c *SQLQueryConverter) BuildAndExpr(exprs ...sqlparser.Expr) (sqlparser.Expr, error) {
	if len(exprs) == 1 {
		return exprs[0], nil
	}
	wrappedExprs := make([]sqlparser.Expr, len(exprs))
	for i, expr := range exprs {
		if _, ok := expr.(*sqlparser.OrExpr); ok {
			wrappedExprs[i], _ = c.BuildParenExpr(expr)
		} else {
			wrappedExprs[i] = expr
		}
	}
	return query.ReduceExprs(
		func(left, right sqlparser.Expr) sqlparser.Expr {
			if left == nil {
				return right
			}
			if right == nil {
				return left
			}
			return &sqlparser.AndExpr{
				Left:  left,
				Right: right,
			}
		},
		wrappedExprs...,
	), nil
}

func (c *SQLQueryConverter) BuildOrExpr(exprs ...sqlparser.Expr) (sqlparser.Expr, error) {
	return query.ReduceExprs(
		func(left, right sqlparser.Expr) sqlparser.Expr {
			if left == nil {
				return right
			}
			if right == nil {
				return left
			}
			return &sqlparser.OrExpr{
				Left:  left,
				Right: right,
			}
		},
		exprs...,
	), nil
}

func (c *SQLQueryConverter) ConvertComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (sqlparser.Expr, error) {
	valueExpr, err := c.buildValueExpr(col.FieldName, value)
	if err != nil {
		return nil, err
	}
	if customConverter, ok := c.VisibilityQueryConverter.(convertComparisonExprIf); ok {
		return customConverter.ConvertComparisonExpr(operator, col, valueExpr)
	}
	return &sqlparser.ComparisonExpr{
		Operator: operator,
		Left:     col,
		Right:    valueExpr,
	}, nil
}

func (c *SQLQueryConverter) ConvertKeywordComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (sqlparser.Expr, error) {
	valueExpr, err := c.buildValueExpr(col.FieldName, value)
	if err != nil {
		return nil, err
	}

	if customConverter, ok := c.VisibilityQueryConverter.(convertKeywordComparisonExprIf); ok {
		return customConverter.ConvertKeywordComparisonExpr(operator, col, valueExpr)
	}

	switch operator {
	case sqlparser.StartsWithStr, sqlparser.NotStartsWithStr:
		v, ok := valueExpr.(*query.UnsafeSQLString)
		if !ok {
			return nil, query.NewConverterError(
				"%s: right-hand side of %q operator must be a literal string (got: %v)",
				query.InvalidExpressionErrMessage,
				operator,
				value,
			)
		}
		v.Val = escapeLikeValueForPrefixSearch(v.Val, defaultLikeEscapeChar)
		if operator == sqlparser.StartsWithStr {
			operator = sqlparser.LikeStr
		} else {
			operator = sqlparser.NotLikeStr
		}
		return &sqlparser.ComparisonExpr{
			Operator: operator,
			Left:     col,
			Right:    v,
			Escape:   defaultLikeEscapeExpr,
		}, nil
	default:
		return c.ConvertComparisonExpr(operator, col, value)
	}
}

func (c *SQLQueryConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (sqlparser.Expr, error) {
	valueExpr, err := c.buildValueExpr(col.FieldName, value)
	if err != nil {
		return nil, err
	}
	return c.VisibilityQueryConverter.ConvertKeywordListComparisonExpr(operator, col, valueExpr)
}

func (c *SQLQueryConverter) ConvertTextComparisonExpr(
	operator string,
	col *query.SAColumn,
	value any,
) (sqlparser.Expr, error) {
	valueExpr, err := c.buildValueExpr(col.FieldName, value)
	if err != nil {
		return nil, err
	}
	return c.VisibilityQueryConverter.ConvertTextComparisonExpr(operator, col, valueExpr)
}

func (c *SQLQueryConverter) ConvertRangeExpr(
	operator string,
	col *query.SAColumn,
	from, to any,
) (sqlparser.Expr, error) {
	fromExpr, err := c.buildValueExpr(col.FieldName, from)
	if err != nil {
		return nil, err
	}
	toExpr, err := c.buildValueExpr(col.FieldName, to)
	if err != nil {
		return nil, err
	}
	if customConverter, ok := c.VisibilityQueryConverter.(convertRangeExprIf); ok {
		return customConverter.ConvertRangeExpr(operator, col, fromExpr, toExpr)
	}
	return &sqlparser.RangeCond{
		Operator: operator,
		Left:     col,
		From:     fromExpr,
		To:       toExpr,
	}, nil
}

func (c *SQLQueryConverter) ConvertIsExpr(
	operator string,
	col *query.SAColumn,
) (sqlparser.Expr, error) {
	if customConverter, ok := c.VisibilityQueryConverter.(convertIsExprIf); ok {
		return customConverter.ConvertIsExpr(operator, col)
	}
	return &sqlparser.IsExpr{
		Operator: operator,
		Expr:     col,
	}, nil
}

func (c *SQLQueryConverter) buildValueExpr(name string, value any) (sqlparser.Expr, error) {
	// ExecutionStatus is stored as an integer in SQL database.
	if name == searchattribute.ExecutionStatus {
		// Query converter already validates the value, so there should not be any errors here.
		status, _ := enumspb.WorkflowExecutionStatusFromString(value.(string))
		return sqlparser.NewIntVal([]byte(strconv.FormatInt(int64(status), 10))), nil
	}

	switch v := value.(type) {
	case string:
		// escape strings for safety
		replacer := strings.NewReplacer(escapeCharMap...)
		return query.NewUnsafeSQLString(replacer.Replace(v)), nil
	case int64:
		return sqlparser.NewIntVal([]byte(strconv.FormatInt(v, 10))), nil
	case float64:
		return sqlparser.NewFloatVal([]byte(strconv.FormatFloat(v, 'f', -1, 64))), nil
	case bool:
		return sqlparser.BoolVal(v), nil
	case []any:
		res := make(sqlparser.ValTuple, len(v))
		for i, item := range v {
			var err error
			res[i], err = c.buildValueExpr(name, item)
			if err != nil {
				return nil, err
			}
		}
		return res, nil
	default:
		// this should never happen: query.ParseSqlValue returns one of the types above
		return nil, query.NewConverterError(
			"%s: unexpected value type %T",
			query.InvalidExpressionErrMessage,
			value,
		)
	}
}

func escapeLikeValueForPrefixSearch(in string, escape byte) string {
	sb := strings.Builder{}
	for _, c := range in {
		if c == '%' || c == '_' || c == rune(escape) {
			sb.WriteByte(escape)
		}
		sb.WriteRune(c)
	}
	sb.WriteByte('%')
	return sb.String()
}
