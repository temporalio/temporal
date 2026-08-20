package configurator

import "go.temporal.io/server/common/dynamicconfig/configurator/types"

func (c *Expression) Matches(constraints types.Lookup) (bool, error) {
	switch c.Operator {
	case OpEqual:
		constraintVal, ok := constraints.Get(string(c.Key))
		if !ok {
			break
		}
		return c.Value.CompareAny(constraintVal) == 0, nil
	case OpNotEqual:
		constraintVal, ok := constraints.Get(string(c.Key))
		if !ok {
			break
		}
		return c.Value.CompareAny(constraintVal) != 0, nil
	case OpGreater:
		constraintVal, ok := constraints.Get(string(c.Key))
		if !ok {
			break
		}
		return c.Value.CompareAny(constraintVal) > 0, nil
	case OpLess:
		constraintVal, ok := constraints.Get(string(c.Key))
		if !ok {
			break
		}
		return c.Value.CompareAny(constraintVal) < 0, nil
	case OpAnd:
		return c.matchesAllSubexpressionsWithAnd(constraints)
	case OpOr:
		return c.matchesAllSubexpressionsWithOr(constraints)
	}
	return false, nil
}

func (e *Expression) matchesAllSubexpressionsWithAnd(constraints types.Lookup) (bool, error) {
	for _, subexpr := range e.Subexpressions {
		matches, err := subexpr.Matches(constraints)
		if err != nil {
			return false, err
		}
		if !matches {
			return false, err
		}
	}
	return true, nil
}

func (e *Expression) matchesAllSubexpressionsWithOr(constraints types.Lookup) (bool, error) {
	for _, subexpr := range e.Subexpressions {
		matches, err := subexpr.Matches(constraints)
		if err != nil {
			return false, err
		}
		if matches {
			return true, err
		}
	}
	return false, nil
}
