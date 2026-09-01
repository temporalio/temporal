package mutationtest

import (
	"fmt"

	"github.com/avito-tech/go-mutesting/mutator"
	_ "github.com/avito-tech/go-mutesting/mutator/arithmetic"  // Register arithmetic operators.
	_ "github.com/avito-tech/go-mutesting/mutator/branch"      // Register branch operators.
	_ "github.com/avito-tech/go-mutesting/mutator/conditional" // Register conditional operators.
	_ "github.com/avito-tech/go-mutesting/mutator/expression"  // Register expression operators.
	_ "github.com/avito-tech/go-mutesting/mutator/loop"        // Register loop operators.
	_ "github.com/avito-tech/go-mutesting/mutator/numbers"     // Register number operators.
)

type mutationOperator struct {
	name   string
	mutate mutator.Mutator
}

var selectedOperatorNames = []string{
	"arithmetic/assign_invert",
	"arithmetic/assignment",
	"arithmetic/base",
	"arithmetic/bitwise",
	"branch/case",
	"branch/else",
	"branch/if",
	"conditional/negated",
	"expression/comparison",
	"loop/break",
	"loop/condition",
	"loop/range_break",
	"numbers/decrementer",
	"numbers/incrementer",
}

func selectedOperators() ([]mutationOperator, error) {
	operators := make([]mutationOperator, 0, len(selectedOperatorNames))
	for _, name := range selectedOperatorNames {
		operator, err := mutator.New(name)
		if err != nil {
			return nil, fmt.Errorf("load mutation operator %s: %w", name, err)
		}
		operators = append(operators, mutationOperator{name: name, mutate: operator})
	}
	return operators, nil
}
