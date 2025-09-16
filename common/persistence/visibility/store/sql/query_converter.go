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

type sqlQueryConverter struct {
	pqc sqlplugin.VisibilityQueryConverter
}

var _ query.StoreQueryConverter[sqlparser.Expr] = (*sqlQueryConverter)(nil)

func newSQLQueryConverter(pluginName string) (*sqlQueryConverter, error) {
	pqc, err := sql.GetPluginVisibilityQueryConverter(pluginName)
	if err != nil {
		return nil, err
	}
	return &sqlQueryConverter{pqc: pqc}, nil
}

func (c *sqlQueryConverter) GetDatetimeFormat() string {
	return c.pqc.GetDatetimeFormat()
}

func (c *sqlQueryConverter) BuildParenExpr(expr sqlparser.Expr) (sqlparser.Expr, error) {
	if _, ok := expr.(*sqlparser.ParenExpr); ok {
		return expr, nil
	}
	return &sqlparser.ParenExpr{Expr: expr}, nil
}

func (c *sqlQueryConverter) BuildNotExpr(expr sqlparser.Expr) (sqlparser.Expr, error) {
	expr, _ = c.BuildParenExpr(expr)
	return &sqlparser.NotExpr{Expr: expr}, nil
}

func (c *sqlQueryConverter) BuildAndExpr(exprs ...sqlparser.Expr) (sqlparser.Expr, error) {
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

func (c *sqlQueryConverter) BuildOrExpr(exprs ...sqlparser.Expr) (sqlparser.Expr, error) {
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

func (c *sqlQueryConverter) ConvertComparisonExpr(
	operator string,
	col *query.SAColName,
	value any,
) (sqlparser.Expr, error) {
	valueExpr, err := c.buildValueExpr(col.FieldName, value)
	if err != nil {
		return nil, err
	}
	return &sqlparser.ComparisonExpr{
		Operator: operator,
		Left:     col,
		Right:    valueExpr,
	}, nil
}

func (c *sqlQueryConverter) ConvertKeywordComparisonExpr(
	operator string,
	col *query.SAColName,
	value any,
) (sqlparser.Expr, error) {
	valueExpr, err := c.buildValueExpr(col.FieldName, value)
	if err != nil {
		return nil, err
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

func (c *sqlQueryConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *query.SAColName,
	value any,
) (sqlparser.Expr, error) {
	valueExpr, err := c.buildValueExpr(col.FieldName, value)
	if err != nil {
		return nil, err
	}
	return c.pqc.ConvertKeywordListComparisonExpr(operator, col, valueExpr)
}

func (c *sqlQueryConverter) ConvertTextComparisonExpr(
	operator string,
	col *query.SAColName,
	value any,
) (sqlparser.Expr, error) {
	valueExpr, err := c.buildValueExpr(col.FieldName, value)
	if err != nil {
		return nil, err
	}
	return c.pqc.ConvertTextComparisonExpr(operator, col, valueExpr)
}

func (c *sqlQueryConverter) ConvertRangeExpr(
	operator string,
	col *query.SAColName,
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
	return &sqlparser.RangeCond{
		Operator: operator,
		Left:     col,
		From:     fromExpr,
		To:       toExpr,
	}, nil
}

func (c *sqlQueryConverter) ConvertIsExpr(
	operator string,
	col *query.SAColName,
) (sqlparser.Expr, error) {
	return &sqlparser.IsExpr{
		Operator: operator,
		Expr:     col,
	}, nil
}

func (c *sqlQueryConverter) BuildSelectStmt(
	queryParams *query.QueryParams[sqlparser.Expr],
	pageSize int,
	pageToken *sqlplugin.VisibilityPageToken,
) (string, []any) {
	return c.pqc.BuildSelectStmt(queryParams, pageSize, pageToken)
}

func (c *sqlQueryConverter) BuildCountStmt(
	queryParams *query.QueryParams[sqlparser.Expr],
) (string, []any) {
	return c.pqc.BuildCountStmt(queryParams)
}

func (c *sqlQueryConverter) buildValueExpr(name string, value any) (sqlparser.Expr, error) {
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
