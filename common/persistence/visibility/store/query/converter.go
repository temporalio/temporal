//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination converter_mock.go

package query

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/sqlquery"
)

type (
	// StoreQueryConverter interface abstracts the Visibility store expression builder.
	// The Convert* functions are the base comparison expressions builders, while the Build* functions
	// build the logical operators expressions.
	//
	// For example, for SQL databases, ExprT can be sqlparser.Expr which can be converted to string as
	// a standard SQL query. Check the following implementations for reference:
	// - SQL database: common/persistence/visibility/store/sql/query_converter.go
	// - Elasticsearch: common/persistence/visibility/store/elasticsearch/query_converter.go
	StoreQueryConverter[ExprT any] interface {
		GetDatetimeFormat() string

		BuildParenExpr(expr ExprT) (ExprT, error)

		BuildNotExpr(expr ExprT) (ExprT, error)

		BuildAndExpr(exprs ...ExprT) (ExprT, error)

		BuildOrExpr(exprs ...ExprT) (ExprT, error)

		ConvertComparisonExpr(operator string, col *SAColumn, value any) (ExprT, error)

		ConvertKeywordComparisonExpr(operator string, col *SAColumn, value any) (ExprT, error)

		ConvertKeywordListComparisonExpr(operator string, col *SAColumn, value any) (ExprT, error)

		ConvertTextComparisonExpr(operator string, col *SAColumn, value any) (ExprT, error)

		ConvertRangeExpr(operator string, col *SAColumn, from, to any) (ExprT, error)

		ConvertIsExpr(operator string, col *SAColumn) (ExprT, error)
	}

	QueryConverter[ExprT any] struct {
		storeQC       StoreQueryConverter[ExprT]
		saInterceptor SearchAttributeInterceptor

		namespaceName namespace.Name
		saTypeMap     searchattribute.NameTypeMap
		saMapper      searchattribute.Mapper
		chasmMapper   *chasm.VisibilitySearchAttributesMapper
		archetypeID   chasm.ArchetypeID

		seenNamespaceDivision bool
	}

	QueryConverterOptionFunc[ExprT any] func(*QueryConverter[ExprT])

	QueryParams[ExprT any] struct {
		QueryExpr ExprT
		OrderBy   sqlparser.OrderBy
		// List of search attributes to group by (field name).
		GroupBy []*SAColumn
	}
)

var (
	groupByFieldAllowlist = []string{
		sadefs.ExecutionStatus,
	}

	groupByFieldPrefixAllowlist = []string{
		"TemporalLowCardinalityKeyword",
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
	}

	supportedKeywordOperators = []string{
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

	supportedKeywordListOperators = []string{
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
)

func NewQueryConverter[ExprT any](
	storeQC StoreQueryConverter[ExprT],
	namespaceName namespace.Name,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
) *QueryConverter[ExprT] {
	c := &QueryConverter[ExprT]{
		storeQC:       storeQC,
		saInterceptor: nopSearchAttributeInterceptor,

		namespaceName: namespaceName,
		saTypeMap:     saTypeMap,
		saMapper:      saMapper,

		seenNamespaceDivision: false,
	}
	return c
}

func (c *QueryConverter[ExprT]) WithSearchAttributeInterceptor(
	saInterceptor SearchAttributeInterceptor,
) *QueryConverter[ExprT] {
	if saInterceptor == nil {
		saInterceptor = nopSearchAttributeInterceptor
	}
	c.saInterceptor = saInterceptor
	return c
}

func (c *QueryConverter[ExprT]) WithChasmMapper(
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
) *QueryConverter[ExprT] {
	c.chasmMapper = chasmMapper
	return c
}

func (c *QueryConverter[ExprT]) WithArchetypeID(
	archetypeID chasm.ArchetypeID,
) *QueryConverter[ExprT] {
	c.archetypeID = archetypeID
	return c
}

func (c *QueryConverter[ExprT]) SeenNamespaceDivision() bool {
	return c.seenNamespaceDivision
}

func (c *QueryConverter[ExprT]) Convert(
	queryString string,
) (*QueryParams[ExprT], error) {
	queryParams, err := c.convertWhereString(queryString)
	if err != nil {
		return nil, err
	}

	// If the query did not explicitly filter on TemporalNamespaceDivision,
	// try setting the namespace division filter based on the archetype ID,
	// else filter by null (no division).
	var namespaceDivisionExpr ExprT
	if !c.seenNamespaceDivision {
		nsDivisionCol := NamespaceDivisionSAColumn()
		if err := c.saInterceptor.Intercept(nsDivisionCol); err != nil {
			return nil, err
		}

		if c.archetypeID != chasm.UnspecifiedArchetypeID {
			// For CHASM queries, filter by archetype ID
			namespaceDivisionExpr, err = c.storeQC.ConvertComparisonExpr(
				sqlparser.EqualStr,
				nsDivisionCol,
				strconv.Itoa(int(c.archetypeID)),
			)
		} else {
			// For regular workflow queries, filter by null (no division)
			namespaceDivisionExpr, err = c.storeQC.ConvertIsExpr(
				sqlparser.IsNullStr,
				nsDivisionCol,
			)
		}
		if err != nil {
			return nil, err
		}
	}

	queryParams.QueryExpr, err = c.storeQC.BuildAndExpr(namespaceDivisionExpr, queryParams.QueryExpr)
	if err != nil {
		return nil, err
	}

	return queryParams, nil
}

func (c *QueryConverter[ExprT]) convertWhereString(queryString string) (*QueryParams[ExprT], error) {
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
		return nil, NewConverterError("%s: %v", MalformedSqlQueryErrMessage, err)
	}

	//nolint:revive // type cast is guaranteed to be a *sqlparser.Select
	selectStmt, _ := stmt.(*sqlparser.Select)
	return c.convertSelectStmt(selectStmt)
}

func (c *QueryConverter[ExprT]) convertSelectStmt(
	sel *sqlparser.Select,
) (*QueryParams[ExprT], error) {
	// TODO: Forbid ORDER BY clause. It's currently allowed only with Elasticsearch for backwards
	// compatibility. Support for ORDER BY will be completely removed in a future release.
	// if sel.OrderBy != nil {
	// 	return nil, NewConverterError("%s: 'ORDER BY' clause", NotSupportedErrMessage)
	// }

	if sel.Limit != nil {
		return nil, NewConverterError("%s: 'LIMIT' clause", NotSupportedErrMessage)
	}

	if sel.Where == nil {
		sel.Where = &sqlparser.Where{
			Type: sqlparser.WhereStr,
			Expr: nil,
		}
	}

	res := &QueryParams[ExprT]{}
	if sel.Where.Expr != nil {
		queryExpr, err := c.convertWhereExpr(sel.Where.Expr)
		if err != nil {
			return nil, err
		}
		res.QueryExpr = queryExpr
	}

	if len(sel.GroupBy) > 1 {
		return nil, NewConverterError(
			"%s: 'GROUP BY' clause supports only a single field",
			NotSupportedErrMessage,
		)
	}
	for k := range sel.GroupBy {
		colName, err := c.convertColName(sel.GroupBy[k])
		if err != nil {
			return nil, err
		}
		if !IsGroupByFieldAllowed(colName.FieldName) {
			return nil, NewConverterError(
				"%s: 'GROUP BY' clause is only supported for ExecutionStatus",
				NotSupportedErrMessage,
			)
		}
		res.GroupBy = append(res.GroupBy, colName)
	}

	for k := range sel.OrderBy {
		colName, err := c.convertColName(sel.OrderBy[k].Expr)
		if err != nil {
			return nil, err
		}
		if colName.ValueType == enumspb.INDEXED_VALUE_TYPE_TEXT {
			return nil, NewConverterError(
				"%s: unable to sort by search attribute type %s",
				NotSupportedErrMessage,
				colName.ValueType,
			)
		}
		sel.OrderBy[k].Expr = colName
	}
	res.OrderBy = sel.OrderBy

	return res, nil
}

func (c *QueryConverter[ExprT]) convertWhereExpr(expr sqlparser.Expr) (ExprT, error) {
	var out ExprT
	if expr == nil {
		// this should never happen
		return out, serviceerror.NewInternal("where expression is nil")
	}

	switch e := expr.(type) {
	case *sqlparser.ParenExpr:
		return c.convertParenExpr(e)
	case *sqlparser.NotExpr:
		return c.convertNotExpr(e)
	case *sqlparser.AndExpr:
		return c.convertAndExpr(e)
	case *sqlparser.OrExpr:
		return c.convertOrExpr(e)
	case *sqlparser.ComparisonExpr:
		return c.convertComparisonExpr(e)
	case *sqlparser.RangeCond:
		return c.convertRangeCond(e)
	case *sqlparser.IsExpr:
		return c.convertIsExpr(e)
	case *sqlparser.FuncExpr:
		return out, NewConverterError("%s: function expression", NotSupportedErrMessage)
	case *sqlparser.ColName:
		return out, NewConverterError("%s: incomplete expression", InvalidExpressionErrMessage)
	default:
		return out, NewConverterError("%s: expression of type %T", NotSupportedErrMessage, e)
	}
}

func (c *QueryConverter[ExprT]) convertParenExpr(expr *sqlparser.ParenExpr) (ExprT, error) {
	var out ExprT
	newExpr, err := c.convertWhereExpr(expr.Expr)
	if err != nil {
		return out, err
	}
	return c.storeQC.BuildParenExpr(newExpr)
}

func (c *QueryConverter[ExprT]) convertNotExpr(expr *sqlparser.NotExpr) (ExprT, error) {
	var out ExprT
	newExpr, err := c.convertWhereExpr(expr.Expr)
	if err != nil {
		return out, err
	}
	return c.storeQC.BuildNotExpr(newExpr)
}

func (c *QueryConverter[ExprT]) convertAndExpr(expr *sqlparser.AndExpr) (ExprT, error) {
	var out ExprT
	left, err := c.convertWhereExpr(expr.Left)
	if err != nil {
		return out, err
	}
	right, err := c.convertWhereExpr(expr.Right)
	if err != nil {
		return out, err
	}
	return c.storeQC.BuildAndExpr(left, right)
}

func (c *QueryConverter[ExprT]) convertOrExpr(expr *sqlparser.OrExpr) (ExprT, error) {
	var out ExprT
	left, err := c.convertWhereExpr(expr.Left)
	if err != nil {
		return out, err
	}
	right, err := c.convertWhereExpr(expr.Right)
	if err != nil {
		return out, err
	}
	return c.storeQC.BuildOrExpr(left, right)
}

func (c *QueryConverter[ExprT]) convertComparisonExpr(
	expr *sqlparser.ComparisonExpr,
) (out ExprT, err error) {
	colName, err := c.convertColName(expr.Left)
	if err != nil {
		return out, err
	}

	values, err := c.parseValueExpr(expr.Right, colName.Alias, colName.FieldName, colName.ValueType)
	if err != nil {
		return out, err
	}

	switch colName.ValueType {
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD:
		if !isSupportedKeywordOperator(expr.Operator) {
			return out, NewOperatorNotSupportedError(colName.Alias, colName.ValueType, expr.Operator)
		}
		return c.storeQC.ConvertKeywordComparisonExpr(expr.Operator, colName, values)
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
		if !isSupportedKeywordListOperator(expr.Operator) {
			return out, NewOperatorNotSupportedError(colName.Alias, colName.ValueType, expr.Operator)
		}
		return c.storeQC.ConvertKeywordListComparisonExpr(expr.Operator, colName, values)
	case enumspb.INDEXED_VALUE_TYPE_TEXT:
		if !isSupportedTextOperator(expr.Operator) {
			return out, NewOperatorNotSupportedError(colName.Alias, colName.ValueType, expr.Operator)
		}
		return c.storeQC.ConvertTextComparisonExpr(expr.Operator, colName, values)
	default:
		if !isSupportedComparisonOperator(expr.Operator) {
			return out, NewOperatorNotSupportedError(colName.Alias, colName.ValueType, expr.Operator)
		}
		return c.storeQC.ConvertComparisonExpr(expr.Operator, colName, values)
	}
}

func (c *QueryConverter[ExprT]) convertRangeCond(expr *sqlparser.RangeCond) (ExprT, error) {
	var out ExprT
	colName, err := c.convertColName(expr.Left)
	if err != nil {
		return out, err
	}

	if !isSupportedTypeRangeCond(colName.ValueType) {
		return out, NewConverterError(
			"%s: cannot do range condition on search attribute '%s' of type %s",
			InvalidExpressionErrMessage,
			colName.Alias,
			colName.ValueType.String(),
		)
	}

	from, err := c.parseValueExpr(expr.From, colName.Alias, colName.FieldName, colName.ValueType)
	if err != nil {
		return out, err
	}

	to, err := c.parseValueExpr(expr.To, colName.Alias, colName.FieldName, colName.ValueType)
	if err != nil {
		return out, err
	}

	return c.storeQC.ConvertRangeExpr(expr.Operator, colName, from, to)
}

func (c *QueryConverter[ExprT]) convertIsExpr(expr *sqlparser.IsExpr) (ExprT, error) {
	var out ExprT
	colName, err := c.convertColName(expr.Expr)
	if err != nil {
		return out, err
	}

	switch expr.Operator {
	case sqlparser.IsNullStr, sqlparser.IsNotNullStr:
		return c.storeQC.ConvertIsExpr(expr.Operator, colName)
	default:
		return out, NewConverterError(
			"%s: 'IS' operator can only be used as 'IS NULL' or 'IS NOT NULL'",
			InvalidExpressionErrMessage,
		)
	}
}

func (c *QueryConverter[ExprT]) convertColName(in sqlparser.Expr) (*SAColumn, error) {
	expr, ok := in.(*sqlparser.ColName)
	if !ok {
		return nil, NewConverterError(
			"%s: must be a column name but was %T",
			InvalidExpressionErrMessage,
			in,
		)
	}
	saAlias := strings.ReplaceAll(sqlparser.String(expr), "`", "")
	saFieldName, saType, err := c.resolveSearchAttributeAlias(saAlias)
	if err != nil {
		return nil, err
	}

	if saFieldName == sadefs.TemporalNamespaceDivision {
		c.seenNamespaceDivision = true
	}

	colName := NewSAColumn(saAlias, saFieldName, saType)
	if err := c.saInterceptor.Intercept(colName); err != nil {
		return nil, err
	}
	return colName, nil
}

func (c *QueryConverter[ExprT]) resolveSearchAttributeAlias(
	alias string,
) (fieldName string, fieldType enumspb.IndexedValueType, retErr error) {
	// resolveCSA only returns true if `alias` is a custom search attribute.
	resolveCSA := func(alias string) bool {
		fn, err := c.saMapper.GetFieldName(alias, c.namespaceName.String())
		if err != nil {
			return false
		}
		ft, err := c.saTypeMap.GetType(fn)
		if err != nil {
			return false
		}
		fieldName, fieldType = fn, ft
		return true
	}
	// resolveChasmSA only returns true if `alias` is a CHASM search attribute.
	resolveChasmSA := func(alias string) bool {
		if c.chasmMapper == nil {
			return false
		}
		fn, err := c.chasmMapper.Field(alias)
		if err != nil {
			return false
		}
		ft, err := c.chasmMapper.ValueType(fn)
		if err != nil {
			return false
		}
		fieldName, fieldType = fn, ft
		return true
	}

	var err error
	fieldName = alias
	// First, check if it's a custom search attribute.
	if sadefs.IsMappable(alias) && resolveCSA(alias) {
		return
	}
	// Second, check if it's a CHASM search attribute.
	if resolveChasmSA(alias) {
		return
	}
	// Third, check if it's a system/reserved search attribute.
	fieldType, err = c.saTypeMap.GetType(fieldName)
	if err == nil {
		return
	}
	// Fourth, check for special aliases or adding/removing the `Temporal` prefix.
	if strings.TrimPrefix(alias, sadefs.ReservedPrefix) == sadefs.ScheduleID {
		fieldName = sadefs.WorkflowID
	} else if strings.HasPrefix(fieldName, sadefs.ReservedPrefix) {
		fieldName = fieldName[len(sadefs.ReservedPrefix):]
	} else {
		fieldName = sadefs.ReservedPrefix + fieldName
	}
	fieldType, err = c.saTypeMap.GetType(fieldName)
	if err == nil {
		return
	}

	retErr = NewConverterError(
		"%s: column name '%s' is not a valid search attribute",
		InvalidExpressionErrMessage,
		alias,
	)
	return
}

func (c *QueryConverter[ExprT]) parseValueExpr(
	expr sqlparser.Expr,
	saName string,
	saFieldName string,
	saType enumspb.IndexedValueType,
) (any, error) {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		value, err := c.parseSQLVal(e, saName, saType)
		if err != nil {
			return nil, err
		}
		if saName == sadefs.ScheduleID && saFieldName == sadefs.WorkflowID {
			value = primitives.ScheduleWorkflowIDPrefix + fmt.Sprintf("%v", value)
		}
		return value, nil
	case sqlparser.BoolVal:
		// no-op: no validation needed
		return bool(e), nil
	case sqlparser.ValTuple:
		// This is "in (1,2,3)" case.
		values := make([]any, 0, len(e))
		for i := range e {
			item, err := c.parseValueExpr(e[i], saName, saFieldName, saType)
			if err != nil {
				return nil, err
			}
			values = append(values, item)
		}
		return values, nil
	case *sqlparser.GroupConcatExpr:
		return nil, NewConverterError("%s: 'group_concat'", NotSupportedErrMessage)
	case *sqlparser.FuncExpr:
		return nil, NewConverterError("%s: nested func", NotSupportedErrMessage)
	case *sqlparser.ColName:
		return nil, NewConverterError(
			"%s: column name on the right side of comparison expression (did you forget to quote %q?)",
			NotSupportedErrMessage,
			sqlparser.String(expr),
		)
	default:
		return nil, NewConverterError(
			"%s: unexpected value type %T",
			InvalidExpressionErrMessage,
			expr,
		)
	}
}

// parseSQLVal handles values for specific search attributes.
// Returns a string, an int64 or a float64 if there are no errors.
// For datetime, converts to UTC.
// For execution status, converts string to enum value.
// For execution duration, converts to nanoseconds.
func (c *QueryConverter[ExprT]) parseSQLVal(
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
	value, err := sqlquery.ParseValue(sqlValue)
	if err != nil {
		return nil, NewConverterError(
			"%s: unable to parse value %q",
			InvalidExpressionErrMessage,
			sqlparser.String(expr),
		)
	}

	switch saName {
	case sadefs.ExecutionStatus:
		return parseExecutionStatusValue(value)
	case sadefs.ExecutionDuration:
		return parseExecutionDurationValue(value)
	default:
		return c.validateValueType(saName, saType, value)
	}
}

func (c *QueryConverter[ExprT]) validateValueType(
	saName string,
	saType enumspb.IndexedValueType,
	value any,
) (any, error) {
	valueTypeErr := NewConverterError(
		"%s: invalid value type for search attribute %s of type %s: %#v (type: %T)",
		InvalidExpressionErrMessage,
		saName,
		saType.String(),
		value,
		value,
	)
	switch saType {
	case enumspb.INDEXED_VALUE_TYPE_INT, enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		switch value.(type) {
		case int64, float64:
			// nothing to do
			return value, nil
		default:
			return nil, valueTypeErr
		}
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		if _, ok := value.(bool); !ok {
			return nil, valueTypeErr
		}
		return value, nil
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		var tm time.Time
		switch v := value.(type) {
		case int64:
			tm = time.Unix(0, v)
		case string:
			var err error
			tm, err = time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return nil, NewConverterError(
					"%s: unable to parse datetime '%s'",
					InvalidExpressionErrMessage,
					v,
				)
			}
		default:
			return nil, valueTypeErr
		}
		return tm.UTC().Format(c.storeQC.GetDatetimeFormat()), nil
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		enumspb.INDEXED_VALUE_TYPE_TEXT:
		if _, ok := value.(string); !ok {
			return nil, valueTypeErr
		}
		return value, nil
	default:
		return nil, NewConverterError(
			"%s: unknown search attribute type %s for %s",
			InvalidExpressionErrMessage,
			saType.String(),
			saName,
		)
	}
}

func IsGroupByFieldAllowed(fieldName string) bool {
	for _, allowedField := range groupByFieldAllowlist {
		if fieldName == allowedField {
			return true
		}
	}
	for _, allowedPrefix := range groupByFieldPrefixAllowlist {
		if strings.HasPrefix(fieldName, allowedPrefix) {
			return true
		}
	}
	return false
}

func parseExecutionStatusValue(value any) (string, error) {
	switch v := value.(type) {
	case int64:
		if _, ok := enumspb.WorkflowExecutionStatus_name[int32(v)]; ok {
			return enumspb.WorkflowExecutionStatus(v).String(), nil
		}
		return "", NewConverterError(
			"%s: invalid %s value %v",
			InvalidExpressionErrMessage,
			sadefs.ExecutionStatus,
			v,
		)
	case string:
		if _, err := enumspb.WorkflowExecutionStatusFromString(v); err == nil {
			return v, nil
		}
		return "", NewConverterError(
			"%s: invalid %s value '%s'",
			InvalidExpressionErrMessage,
			sadefs.ExecutionStatus,
			v,
		)
	default:
		return "", NewConverterError(
			"%s: unexpected value type %T for search attribute %s",
			InvalidExpressionErrMessage,
			v,
			sadefs.ExecutionStatus,
		)
	}
}

func parseExecutionDurationValue(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case string:
		duration, err := ParseExecutionDurationStr(v)
		if err != nil {
			return 0, NewConverterError(
				"%s: invalid duration value for search attribute %s: %v",
				InvalidExpressionErrMessage,
				sadefs.ExecutionDuration,
				value,
			)
		}
		return duration.Nanoseconds(), nil
	default:
		return 0, NewConverterError(
			"%s: unexpected value type %T for search attribute %s",
			InvalidExpressionErrMessage,
			v,
			sadefs.ExecutionDuration,
		)
	}
}

func isSupportedComparisonOperator(operator string) bool {
	return slices.Contains(supportedComparisonOperators, operator)
}

func isSupportedKeywordOperator(operator string) bool {
	return slices.Contains(supportedKeywordOperators, operator)
}

func isSupportedKeywordListOperator(operator string) bool {
	return slices.Contains(supportedKeywordListOperators, operator)
}

func isSupportedTextOperator(operator string) bool {
	return slices.Contains(supportedTextOperators, operator)
}

func isSupportedTypeRangeCond(saType enumspb.IndexedValueType) bool {
	return slices.Contains(supportedTypesRangeCond, saType)
}
