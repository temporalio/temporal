package query

import (
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

// convertTimeSkippingFlagComparison maps public booleans to the shared T/F keyword index.
// Missing state retains the usual search-attribute NULL semantics.
func (c *QueryConverter[ExprT]) convertTimeSkippingFlagComparison(
	operator string,
	col *SAColumn,
	value any,
) (out ExprT, err error) {
	var matchesFalse, matchesTrue bool
	switch operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		used, ok := value.(bool)
		if !ok {
			return out, NewConverterError("%s: %s requires a boolean value", InvalidExpressionErrMessage, col.Alias)
		}
		matchesFalse, matchesTrue = !used, used
	case sqlparser.InStr, sqlparser.NotInStr:
		values, ok := value.([]any)
		if !ok {
			return out, NewConverterError("%s: %s requires a list of boolean values", InvalidExpressionErrMessage, col.Alias)
		}
		for _, value := range values {
			used, ok := value.(bool)
			if !ok {
				return out, NewConverterError("%s: %s requires boolean values", InvalidExpressionErrMessage, col.Alias)
			}
			matchesFalse = matchesFalse || !used
			matchesTrue = matchesTrue || used
		}
	default:
		return out, NewOperatorNotSupportedError(col.Alias, col.ValueType, operator)
	}
	stateCol := NewSAColumn(col.Alias, sadefs.TemporalTimeSkippingState, enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	states := []string{
		sadefs.TimeSkippingStateDisabledUnused,
		sadefs.TimeSkippingStateDisabledUsed,
		sadefs.TimeSkippingStateEnabledUnused,
		sadefs.TimeSkippingStateEnabledUsed,
	}
	flagIndex := 0
	if col.FieldName == sadefs.TimeSkippingHasSkippedDuration {
		flagIndex = 1
	}
	values := make([]any, 0, len(states))
	for _, state := range states {
		include := matchesFalse
		if state[flagIndex] == 'T' {
			include = matchesTrue
		}
		if include {
			values = append(values, state)
		}
	}
	positive, err := c.storeQC.ConvertKeywordComparisonExpr(sqlparser.InStr, stateCol, values)
	if err != nil {
		return out, err
	}
	if operator == sqlparser.NotEqualStr || operator == sqlparser.NotInStr {
		return c.storeQC.BuildNotExpr(positive)
	}
	return positive, nil
}
