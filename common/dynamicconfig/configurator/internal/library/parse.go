package configurator

// parser_gen.go was produced by pigeon from expr.pigeon, which is not vendored here. The
// go:generate directive is deliberately omitted so that `go generate ./...` does not fail for
// anyone without pigeon installed; regenerate the parser in the upstream checkout if the
// grammar ever needs to change.

// ParseExpression parses a DSL expression string into an Expression tree.
// Example: "(env = prod and region = us-west-1) or env = staging"
func ParseExpression(input string) (*Expression, error) {
	result, err := Parse("", []byte(input))
	if err != nil {
		return nil, err
	}
	return result.(*Expression), nil
}
