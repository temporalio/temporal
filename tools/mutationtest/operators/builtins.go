package operators

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

var builtinOperatorNames = []string{
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

func builtinDefinitions() ([]definition, error) {
	definitions := make([]definition, 0, len(builtinOperatorNames))
	for _, name := range builtinOperatorNames {
		operator, err := mutator.New(name)
		if err != nil {
			return nil, fmt.Errorf("load mutation operator %s: %w", name, err)
		}
		definitions = append(definitions, definition{
			name:           name,
			defaultEnabled: true,
			implementation: implementationUpstream,
			mutate:         operator,
		})
	}
	return definitions, nil
}
