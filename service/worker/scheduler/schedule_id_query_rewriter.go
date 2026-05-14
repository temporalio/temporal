package scheduler

import (
	"strings"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

// RewriteScheduleIDQuery rewrites ScheduleId comparisons in the query string to WorkflowId
// comparisons before the query reaches the visibility store converters.
//
// V1 schedules store WorkflowId with a "temporal-sys-scheduler:" prefix; V2/CHASM schedules
// store it without any prefix. When chasmEnabled (migration period), each ScheduleId comparison
// becomes an OR of the prefixed (V1) and unprefixed (V2) WorkflowId conditions for positive
// operators, and AND for negative operators (!=, NOT IN, NOT STARTS_WITH), so both stores are
// correctly included or excluded.
//
// If the user has defined a custom search attribute named ScheduleId that maps to a field other
// than WorkflowId, this function leaves the expression unchanged.
//
// TODO: once V1 schedules are fully migrated to CHASM, drop the OR/AND and emit only the
// unprefixed V2 WorkflowId condition.
func RewriteScheduleIDQuery(
	queryStr string,
	chasmEnabled bool,
	saMapper searchattribute.Mapper,
	ns namespace.Name,
) (string, error) {
	if strings.TrimSpace(queryStr) == "" {
		return queryStr, nil
	}

	stmt, err := sqlparser.Parse("select * from table1 where " + queryStr)
	if err != nil {
		// Malformed SQL is passed through; the normal validation path will return a proper error.
		return queryStr, nil
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return queryStr, nil
	}
	if sel.Where == nil {
		return queryStr, nil
	}

	changed := rewriteExpr(&sel.Where.Expr, chasmEnabled, saMapper, ns.String())
	if !changed {
		return queryStr, nil
	}
	return sqlparser.String(sel.Where.Expr), nil
}

// rewriteExpr recursively walks expr and rewrites ScheduleId comparison nodes in-place.
// Returns true if any rewriting occurred.
func rewriteExpr(exprRef *sqlparser.Expr, chasmEnabled bool, saMapper searchattribute.Mapper, ns string) bool {
	switch e := (*exprRef).(type) {
	case *sqlparser.AndExpr:
		l := rewriteExpr(&e.Left, chasmEnabled, saMapper, ns)
		r := rewriteExpr(&e.Right, chasmEnabled, saMapper, ns)
		return l || r
	case *sqlparser.OrExpr:
		l := rewriteExpr(&e.Left, chasmEnabled, saMapper, ns)
		r := rewriteExpr(&e.Right, chasmEnabled, saMapper, ns)
		return l || r
	case *sqlparser.ParenExpr:
		return rewriteExpr(&e.Expr, chasmEnabled, saMapper, ns)
	case *sqlparser.NotExpr:
		return rewriteExpr(&e.Expr, chasmEnabled, saMapper, ns)
	case *sqlparser.ComparisonExpr:
		return rewriteComparison(exprRef, e, chasmEnabled, saMapper, ns)
	case *sqlparser.IsExpr:
		return rewriteIsExpr(e, saMapper, ns)
	}
	return false
}

// rewriteComparison rewrites a single ComparisonExpr if its LHS is the synthetic ScheduleId SA.
func rewriteComparison(exprRef *sqlparser.Expr, expr *sqlparser.ComparisonExpr, chasmEnabled bool, saMapper searchattribute.Mapper, ns string) bool {
	col, ok := expr.Left.(*sqlparser.ColName)
	if !ok || !isSyntheticScheduleIDColumn(col, saMapper, ns) {
		return false
	}

	workflowIDCol := &sqlparser.ColName{Name: sqlparser.NewColIdent(sadefs.WorkflowID)}

	if !chasmEnabled {
		// V1-only: prefix the value and use WorkflowId as column.
		expr.Left = workflowIDCol
		expr.Right = prefixScheduleIDSQLValues(expr.Right)
		return true
	}

	// CHASM migration path: OR of prefixed (V1) and unprefixed (V2) for positive operators;
	// AND for negative operators so both forms are excluded.
	v1Expr := &sqlparser.ComparisonExpr{
		Operator: expr.Operator,
		Left:     workflowIDCol,
		Right:    prefixScheduleIDSQLValues(expr.Right),
	}
	v2Expr := &sqlparser.ComparisonExpr{
		Operator: expr.Operator,
		Left:     workflowIDCol,
		Right:    expr.Right,
	}

	if IsNegativeScheduleIDOperator(expr.Operator) {
		*exprRef = &sqlparser.ParenExpr{Expr: &sqlparser.AndExpr{Left: v1Expr, Right: v2Expr}}
	} else {
		*exprRef = &sqlparser.ParenExpr{Expr: &sqlparser.OrExpr{Left: v1Expr, Right: v2Expr}}
	}
	return true
}

// rewriteIsExpr rewrites a ScheduleId IS [NOT] NULL expression to use WorkflowId.
// No prefix rewriting is needed for IS NULL / IS NOT NULL.
func rewriteIsExpr(expr *sqlparser.IsExpr, saMapper searchattribute.Mapper, ns string) bool {
	col, ok := expr.Expr.(*sqlparser.ColName)
	if !ok || !isSyntheticScheduleIDColumn(col, saMapper, ns) {
		return false
	}
	expr.Expr = &sqlparser.ColName{Name: sqlparser.NewColIdent(sadefs.WorkflowID)}
	return true
}

// IsNegativeScheduleIDOperator returns true for operators that express exclusion.
// Negative operators require AND when combining V1 and V2 WorkflowId conditions so that
// both prefixed and unprefixed forms are excluded; positive operators use OR.
func IsNegativeScheduleIDOperator(operator string) bool {
	return operator == sqlparser.NotEqualStr ||
		operator == sqlparser.NotInStr ||
		operator == sqlparser.NotStartsWithStr
}

// isSyntheticScheduleIDColumn returns true if col refers to the synthetic ScheduleId search
// attribute (which maps to WorkflowId), as opposed to a user-defined custom SA named ScheduleId.
func isSyntheticScheduleIDColumn(col *sqlparser.ColName, saMapper searchattribute.Mapper, ns string) bool {
	alias := col.Name.String()
	// Apply the custom SA mapper. If the alias resolves to a different field name, it is a
	// user-defined custom SA and the synthetic rewrite does not apply.
	if mapped, err := saMapper.GetFieldName(alias, ns); err == nil && mapped != alias {
		return false
	}
	// The synthetic ScheduleId is identified by the alias matching "ScheduleId" after stripping
	// the optional "Temporal" reserved prefix (so both ScheduleId and TemporalScheduleId match).
	return strings.TrimPrefix(alias, sadefs.ReservedPrefix) == sadefs.ScheduleID
}

// prefixScheduleIDSQLValues returns a copy of the SQL value expression with the V1 schedule
// WorkflowId prefix prepended to each string literal. Handles single SQLVal and ValTuple (IN).
func prefixScheduleIDSQLValues(expr sqlparser.Expr) sqlparser.Expr {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		if e.Type == sqlparser.StrVal {
			return sqlparser.NewStrVal([]byte(primitives.ScheduleWorkflowIDPrefix + string(e.Val)))
		}
		return e
	case sqlparser.ValTuple:
		result := make(sqlparser.ValTuple, len(e))
		for i, item := range e {
			result[i] = prefixScheduleIDSQLValues(item)
		}
		return result
	default:
		return expr
	}
}
