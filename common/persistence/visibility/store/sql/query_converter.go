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

package sql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

type (
	pluginQueryConverter interface {
		convertKeywordListComparisonExpr(expr *sqlparser.ComparisonExpr) (sqlparser.Expr, error)

		convertTextComparisonExpr(expr *sqlparser.ComparisonExpr) (sqlparser.Expr, error)

		buildSelectStmt(
			namespaceID namespace.ID,
			queryString string,
			pageSize int,
			token *pageToken,
		) (string, []any)

		buildCountStmt(namespaceID namespace.ID, queryString string, groupBy []string) (string, []any)

		getDatetimeFormat() string

		getCoalesceCloseTimeExpr() sqlparser.Expr
	}

	QueryConverter struct {
		pluginQueryConverter
		namespaceName namespace.Name
		namespaceID   namespace.ID
		saTypeMap     searchattribute.NameTypeMap
		saMapper      searchattribute.Mapper
		queryString   string

		seenNamespaceDivision bool
	}

	queryParams struct {
		queryString string
		// List of search attributes to group by (field name, not db name).
		groupBy []string
	}
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

	supportedComparisonOperators = []string{
		sqlparser.EqualStr,
		sqlparser.NotEqualStr,
		sqlparser.LessThanStr,
		sqlparser.GreaterThanStr,
		sqlparser.LessEqualStr,
		sqlparser.GreaterEqualStr,
		sqlparser.InStr,
		sqlparser.NotInStr,
		sqlparser.StartsWithStr,
		sqlparser.NotStartsWithStr,
	}

	supportedKeyworkListOperators = []string{
		sqlparser.EqualStr,
		sqlparser.NotEqualStr,
		sqlparser.InStr,
		sqlparser.NotInStr,
	}

	supportedTextOperators = []string{
		sqlparser.EqualStr,
		sqlparser.NotEqualStr,
	}

	supportedTypesRangeCond = []enumspb.IndexedValueType{
		enumspb.INDEXED_VALUE_TYPE_DATETIME,
		enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		enumspb.INDEXED_VALUE_TYPE_INT,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}

	defaultLikeEscapeExpr = newUnsafeSQLString(string(defaultLikeEscapeChar))
)

func newQueryConverterInternal(
	pqc pluginQueryConverter,
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	queryString string,
) *QueryConverter {
	return &QueryConverter{
		pluginQueryConverter: pqc,
		namespaceName:        namespaceName,
		namespaceID:          namespaceID,
		saTypeMap:            saTypeMap,
		saMapper:             saMapper,
		queryString:          queryString,

		seenNamespaceDivision: false,
	}
}

func (c *QueryConverter) BuildSelectStmt(
	pageSize int,
	nextPageToken []byte,
) (*sqlplugin.VisibilitySelectFilter, error) {
	token, err := deserializePageToken(nextPageToken)
	if err != nil {
		return nil, err
	}
	qp, err := c.convertWhereString(c.queryString)
	if err != nil {
		return nil, err
	}
	if len(qp.groupBy) > 0 {
		return nil, query.NewConverterError("%s: 'group by' clause", query.NotSupportedErrMessage)
	}
	queryString, queryArgs := c.buildSelectStmt(
		c.namespaceID,
		qp.queryString,
		pageSize,
		token,
	)
	return &sqlplugin.VisibilitySelectFilter{Query: queryString, QueryArgs: queryArgs}, nil
}

func (c *QueryConverter) BuildCountStmt() (*sqlplugin.VisibilitySelectFilter, error) {
	qp, err := c.convertWhereString(c.queryString)
	if err != nil {
		return nil, err
	}
	groupByDbNames := make([]string, len(qp.groupBy))
	for i, fieldName := range qp.groupBy {
		groupByDbNames[i] = searchattribute.GetSqlDbColName(fieldName)
	}
	queryString, queryArgs := c.buildCountStmt(c.namespaceID, qp.queryString, groupByDbNames)
	return &sqlplugin.VisibilitySelectFilter{
		Query:     queryString,
		QueryArgs: queryArgs,
		GroupBy:   qp.groupBy,
	}, nil
}

func (c *QueryConverter) convertWhereString(queryString string) (*queryParams, error) {
	where := strings.TrimSpace(queryString)
	if where != "" &&
		!strings.HasPrefix(strings.ToLower(where), "order by") &&
		!strings.HasPrefix(strings.ToLower(where), "group by") {
		where = "where " + where
	}
	// sqlparser can't parse just WHERE clause but instead accepts only valid SQL statement.
	sql := "select * from table1 " + where
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, query.NewConverterError("%s: %v", query.MalformedSqlQueryErrMessage, err)
	}

	selectStmt, _ := stmt.(*sqlparser.Select)
	err = c.convertSelectStmt(selectStmt)
	if err != nil {
		return nil, err
	}

	res := &queryParams{}
	if selectStmt.Where != nil {
		res.queryString = sqlparser.String(selectStmt.Where.Expr)
	}
	for _, groupByExpr := range selectStmt.GroupBy {
		// The parser already ensures the type is saColName.
		colName := groupByExpr.(*saColName)
		res.groupBy = append(res.groupBy, colName.fieldName)
	}
	return res, nil
}

func (c *QueryConverter) convertSelectStmt(sel *sqlparser.Select) error {
	if sel.OrderBy != nil {
		return query.NewConverterError("%s: 'order by' clause", query.NotSupportedErrMessage)
	}

	if sel.Limit != nil {
		return query.NewConverterError("%s: 'limit' clause", query.NotSupportedErrMessage)
	}

	if sel.Where == nil {
		sel.Where = &sqlparser.Where{
			Type: sqlparser.WhereStr,
			Expr: nil,
		}
	}

	if sel.Where.Expr != nil {
		err := c.convertWhereExpr(&sel.Where.Expr)
		if err != nil {
			return err
		}

		// Wrap user's query in parenthesis. This is to ensure that further changes
		// to the query won't affect the user's query.
		switch sel.Where.Expr.(type) {
		case *sqlparser.ParenExpr:
			// no-op: top-level expression is already a parenthesis
		default:
			sel.Where.Expr = &sqlparser.ParenExpr{
				Expr: sel.Where.Expr,
			}
		}
	}

	// This logic comes from elasticsearch/visibility_store.go#convertQuery function.
	// If the query did not explicitly filter on TemporalNamespaceDivision,
	// then add "is null" query to it.
	if !c.seenNamespaceDivision {
		namespaceDivisionExpr := &sqlparser.IsExpr{
			Operator: sqlparser.IsNullStr,
			Expr: newColName(
				searchattribute.GetSqlDbColName(searchattribute.TemporalNamespaceDivision),
			),
		}
		if sel.Where.Expr == nil {
			sel.Where.Expr = namespaceDivisionExpr
		} else {
			sel.Where.Expr = &sqlparser.AndExpr{
				Left:  sel.Where.Expr,
				Right: namespaceDivisionExpr,
			}
		}
	}

	if len(sel.GroupBy) > 1 {
		return query.NewConverterError(
			"%s: 'group by' clause supports only a single field",
			query.NotSupportedErrMessage,
		)
	}
	for k := range sel.GroupBy {
		colName, err := c.convertColName(&sel.GroupBy[k])
		if err != nil {
			return err
		}
		if colName.fieldName != searchattribute.ExecutionStatus {
			return query.NewConverterError(
				"%s: 'group by' clause is only supported for %s search attribute",
				query.NotSupportedErrMessage,
				searchattribute.ExecutionStatus,
			)
		}
	}

	return nil
}

func (c *QueryConverter) convertWhereExpr(expr *sqlparser.Expr) error {
	if expr == nil || *expr == nil {
		return errors.New("cannot be nil")
	}

	switch e := (*expr).(type) {
	case *sqlparser.ParenExpr:
		return c.convertWhereExpr(&e.Expr)
	case *sqlparser.NotExpr:
		return c.convertWhereExpr(&e.Expr)
	case *sqlparser.AndExpr:
		return c.convertAndExpr(expr)
	case *sqlparser.OrExpr:
		return c.convertOrExpr(expr)
	case *sqlparser.ComparisonExpr:
		return c.convertComparisonExpr(expr)
	case *sqlparser.RangeCond:
		return c.convertRangeCond(expr)
	case *sqlparser.IsExpr:
		return c.convertIsExpr(expr)
	case *sqlparser.FuncExpr:
		return query.NewConverterError("%s: function expression", query.NotSupportedErrMessage)
	case *sqlparser.ColName:
		return query.NewConverterError("%s: incomplete expression", query.InvalidExpressionErrMessage)
	default:
		return query.NewConverterError("%s: expression of type %T", query.NotSupportedErrMessage, e)
	}
}

func (c *QueryConverter) convertAndExpr(exprRef *sqlparser.Expr) error {
	expr, ok := (*exprRef).(*sqlparser.AndExpr)
	if !ok {
		return query.NewConverterError("`%s` is not an 'AND' expression", sqlparser.String(*exprRef))
	}
	err := c.convertWhereExpr(&expr.Left)
	if err != nil {
		return err
	}
	return c.convertWhereExpr(&expr.Right)
}

func (c *QueryConverter) convertOrExpr(exprRef *sqlparser.Expr) error {
	expr, ok := (*exprRef).(*sqlparser.OrExpr)
	if !ok {
		return query.NewConverterError("`%s` is not an 'OR' expression", sqlparser.String(*exprRef))
	}
	err := c.convertWhereExpr(&expr.Left)
	if err != nil {
		return err
	}
	return c.convertWhereExpr(&expr.Right)
}

func (c *QueryConverter) convertComparisonExpr(exprRef *sqlparser.Expr) error {
	expr, ok := (*exprRef).(*sqlparser.ComparisonExpr)
	if !ok {
		return query.NewConverterError(
			"`%s` is not a comparison expression",
			sqlparser.String(*exprRef),
		)
	}

	if !isSupportedComparisonOperator(expr.Operator) {
		return query.NewConverterError(
			"%s: invalid operator '%s' in `%s`",
			query.InvalidExpressionErrMessage,
			expr.Operator,
			sqlparser.String(expr),
		)
	}

	saColNameExpr, err := c.convertColName(&expr.Left)
	if err != nil {
		return err
	}

	err = c.convertValueExpr(&expr.Right, saColNameExpr.alias, saColNameExpr.valueType)
	if err != nil {
		return err
	}

	switch saColNameExpr.valueType {
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
		newExpr, err := c.convertKeywordListComparisonExpr(expr)
		if err != nil {
			return err
		}
		*exprRef = newExpr
	case enumspb.INDEXED_VALUE_TYPE_TEXT:
		newExpr, err := c.convertTextComparisonExpr(expr)
		if err != nil {
			return err
		}
		*exprRef = newExpr
	}

	switch expr.Operator {
	case sqlparser.StartsWithStr, sqlparser.NotStartsWithStr:
		valueExpr, ok := expr.Right.(*unsafeSQLString)
		if !ok {
			return query.NewConverterError(
				"%s: right-hand side of '%s' must be a literal string (got: %v)",
				query.InvalidExpressionErrMessage,
				expr.Operator,
				sqlparser.String(expr.Right),
			)
		}
		if expr.Operator == sqlparser.StartsWithStr {
			expr.Operator = sqlparser.LikeStr
		} else {
			expr.Operator = sqlparser.NotLikeStr
		}
		expr.Escape = defaultLikeEscapeExpr
		valueExpr.Val = escapeLikeValueForPrefixSearch(valueExpr.Val, defaultLikeEscapeChar)
	}

	return nil
}

func (c *QueryConverter) convertRangeCond(exprRef *sqlparser.Expr) error {
	expr, ok := (*exprRef).(*sqlparser.RangeCond)
	if !ok {
		return query.NewConverterError(
			"`%s` is not a range condition expression",
			sqlparser.String(*exprRef),
		)
	}
	saColNameExpr, err := c.convertColName(&expr.Left)
	if err != nil {
		return err
	}
	if !isSupportedTypeRangeCond(saColNameExpr.valueType) {
		return query.NewConverterError(
			"%s: cannot do range condition on search attribute '%s' of type %s",
			query.InvalidExpressionErrMessage,
			saColNameExpr.alias,
			saColNameExpr.valueType.String(),
		)
	}
	err = c.convertValueExpr(&expr.From, saColNameExpr.alias, saColNameExpr.valueType)
	if err != nil {
		return err
	}
	err = c.convertValueExpr(&expr.To, saColNameExpr.alias, saColNameExpr.valueType)
	if err != nil {
		return err
	}
	return nil
}

func (c *QueryConverter) convertColName(exprRef *sqlparser.Expr) (*saColName, error) {
	expr, ok := (*exprRef).(*sqlparser.ColName)
	if !ok {
		return nil, query.NewConverterError(
			"%s: must be a column name but was %T",
			query.InvalidExpressionErrMessage,
			*exprRef,
		)
	}
	saAlias := strings.ReplaceAll(sqlparser.String(expr), "`", "")
	saFieldName := saAlias
	if searchattribute.IsMappable(saAlias) {
		var err error
		saFieldName, err = c.saMapper.GetFieldName(saAlias, c.namespaceName.String())
		if err != nil {
			return nil, query.NewConverterError(
				"%s: column name '%s' is not a valid search attribute",
				query.InvalidExpressionErrMessage,
				saAlias,
			)
		}
	}
	saType, err := c.saTypeMap.GetType(saFieldName)
	if err != nil {
		// This should never happen since it came from mapping.
		return nil, query.NewConverterError(
			"%s: column name '%s' is not a valid search attribute",
			query.InvalidExpressionErrMessage,
			saAlias,
		)
	}
	if saFieldName == searchattribute.TemporalNamespaceDivision {
		c.seenNamespaceDivision = true
	}
	if saAlias == searchattribute.CloseTime {
		*exprRef = c.getCoalesceCloseTimeExpr()
		return closeTimeSaColName, nil
	}
	newExpr := newSAColName(
		searchattribute.GetSqlDbColName(saFieldName),
		saAlias,
		saFieldName,
		saType,
	)
	*exprRef = newExpr
	return newExpr, nil
}

func (c *QueryConverter) convertValueExpr(
	exprRef *sqlparser.Expr,
	saName string,
	saType enumspb.IndexedValueType,
) error {
	expr := *exprRef
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		value, err := c.parseSQLVal(e, saName, saType)
		if err != nil {
			return err
		}
		switch v := value.(type) {
		case string:
			// escape strings for safety
			replacer := strings.NewReplacer(escapeCharMap...)
			*exprRef = newUnsafeSQLString(replacer.Replace(v))
		case int64:
			*exprRef = sqlparser.NewIntVal([]byte(strconv.FormatInt(v, 10)))
		case float64:
			*exprRef = sqlparser.NewFloatVal([]byte(strconv.FormatFloat(v, 'f', -1, 64)))
		default:
			// this should never happen: query.ParseSqlValue returns one of the types above
			return query.NewConverterError(
				"%s: unexpected value type %T for search attribute %s",
				query.InvalidExpressionErrMessage,
				v,
				saName,
			)
		}
		return nil
	case sqlparser.BoolVal:
		// no-op: no validation needed
		return nil
	case sqlparser.ValTuple:
		// This is "in (1,2,3)" case.
		for i := range e {
			err := c.convertValueExpr(&e[i], saName, saType)
			if err != nil {
				return err
			}
		}
		return nil
	case *sqlparser.GroupConcatExpr:
		return query.NewConverterError("%s: 'group_concat'", query.NotSupportedErrMessage)
	case *sqlparser.FuncExpr:
		return query.NewConverterError("%s: nested func", query.NotSupportedErrMessage)
	case *sqlparser.ColName:
		return query.NewConverterError(
			"%s: column name on the right side of comparison expression (did you forget to quote '%s'?)",
			query.NotSupportedErrMessage,
			sqlparser.String(expr),
		)
	default:
		return query.NewConverterError(
			"%s: unexpected value type %T",
			query.InvalidExpressionErrMessage,
			expr,
		)
	}
}

// parseSQLVal handles values for specific search attributes.
// Returns a string, an int64 or a float64 if there are no errors.
// For datetime, converts to UTC.
// For execution status, converts string to enum value.
// For execution duration, converts to nanoseconds.
func (c *QueryConverter) parseSQLVal(
	expr *sqlparser.SQLVal,
	saName string,
	saType enumspb.IndexedValueType,
) (any, error) {
	// Using expr.Val instead of sqlparser.String(expr) because the latter escapes chars using MySQL
	// conventions which is incompatible with SQLite.
	var sqlValue string
	switch expr.Type {
	case sqlparser.StrVal:
		sqlValue = fmt.Sprintf(`'%s'`, expr.Val)
	default:
		sqlValue = string(expr.Val)
	}
	value, err := query.ParseSqlValue(sqlValue)
	if err != nil {
		return nil, err
	}

	if saType == enumspb.INDEXED_VALUE_TYPE_DATETIME {
		var tm time.Time
		switch v := value.(type) {
		case int64:
			tm = time.Unix(0, v)
		case string:
			var err error
			tm, err = time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return nil, query.NewConverterError(
					"%s: unable to parse datetime '%s'",
					query.InvalidExpressionErrMessage,
					v,
				)
			}
		default:
			return nil, query.NewConverterError(
				"%s: unexpected value type %T for search attribute %s",
				query.InvalidExpressionErrMessage,
				v,
				saName,
			)
		}
		return tm.UTC().Format(c.getDatetimeFormat()), nil
	}

	if saName == searchattribute.ExecutionStatus {
		var status int64
		switch v := value.(type) {
		case int64:
			status = v
		case string:
			code, err := enumspb.WorkflowExecutionStatusFromString(v)
			if err != nil {
				return nil, query.NewConverterError(
					"%s: invalid ExecutionStatus value '%s'",
					query.InvalidExpressionErrMessage,
					v,
				)
			}
			status = int64(code)
		default:
			return nil, query.NewConverterError(
				"%s: unexpected value type %T for search attribute %s",
				query.InvalidExpressionErrMessage,
				v,
				saName,
			)
		}
		return status, nil
	}

	if saName == searchattribute.ExecutionDuration {
		if durationStr, isString := value.(string); isString {
			duration, err := query.ParseExecutionDurationStr(durationStr)
			if err != nil {
				return nil, query.NewConverterError(
					"invalid value for search attribute %s: %v (%v)", saName, value, err)
			}
			value = duration.Nanoseconds()
		}
	}

	return value, nil
}

func (c *QueryConverter) convertIsExpr(exprRef *sqlparser.Expr) error {
	expr, ok := (*exprRef).(*sqlparser.IsExpr)
	if !ok {
		return query.NewConverterError("`%s` is not an 'IS' expression", sqlparser.String(*exprRef))
	}
	_, err := c.convertColName(&expr.Expr)
	if err != nil {
		return err
	}
	switch expr.Operator {
	case sqlparser.IsNullStr, sqlparser.IsNotNullStr:
		// no-op
	default:
		return query.NewConverterError(
			"%s: 'IS' operator can only be used with 'NULL' or 'NOT NULL'",
			query.InvalidExpressionErrMessage,
		)
	}
	return nil
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

func isSupportedOperator(supportedOperators []string, operator string) bool {
	for _, op := range supportedOperators {
		if operator == op {
			return true
		}
	}
	return false
}

func isSupportedComparisonOperator(operator string) bool {
	return isSupportedOperator(supportedComparisonOperators, operator)
}

func isSupportedKeywordListOperator(operator string) bool {
	return isSupportedOperator(supportedKeyworkListOperators, operator)
}

func isSupportedTextOperator(operator string) bool {
	return isSupportedOperator(supportedTextOperators, operator)
}

func isSupportedTypeRangeCond(saType enumspb.IndexedValueType) bool {
	for _, tp := range supportedTypesRangeCond {
		if saType == tp {
			return true
		}
	}
	return false
}
