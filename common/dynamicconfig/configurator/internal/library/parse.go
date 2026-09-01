package configurator

// parser_gen.go is generated from expr.pigeon, which is vendored alongside it. Regenerate
// with:
//
//	go run github.com/mna/pigeon@latest -o parser_gen.go expr.pigeon
//
// The go:generate directive is deliberately omitted so that `go generate ./...` does not fail
// for anyone without pigeon installed.

// ParseExpression parses a DSL expression string into an Expression tree.
// Example: "(env = prod and region = us-west-1) or env = staging"
func ParseExpression(input string) (*Expression, error) {
	result, err := Parse("", []byte(input))
	if err != nil {
		return nil, err
	}
	return result.(*Expression), nil
}
