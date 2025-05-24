package stamp

import (
	"reflect"
)

var (
	_        prop = Rule{}
	ruleType      = reflect.TypeFor[Rule]()
)

type (
	Rule struct {
		Prop[bool]
	}
)

func NewRule(
	owner modelWrapper,
	evalFn func(ctx *PropContext) bool,
	opts ...propOption[bool],
) Rule {
	return Rule{Prop: NewProp[bool](owner, evalFn, opts...)}
}
