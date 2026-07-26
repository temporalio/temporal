package configurator

//go:generate pigeon -o parser_gen.go expr.pigeon

// ParseExpression parses a DSL expression string into an Expression tree.
// Example: "(env = prod and region = us-west-1) or env = staging"
func ParseExpression(input string) (*Expression, error) {
	result, err := Parse("", []byte(input))
	if err != nil {
		return nil, err
	}
	return result.(*Expression), nil
}
