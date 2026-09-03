# Custom mutation operators

Temporal-owned mutation operators live in this package. Each operator is ordinary Go code with a focused unit test and an explicit entry in `operators/catalog.go`; do not register it through `init`.

Use a canonical `category/name` identifier. A mutator receives the loaded package, type information, and the current AST node, and returns zero or more `mutator.Mutation` values. Guard mutations with type information when syntax alone is ambiguous, and make every `Change`/`Reset` pair restore the exact original AST state.

`boolean_literal.go` is the reference implementation. It checks `types.Info.ObjectOf` against `types.Universe.Lookup`, changes only the universe `true` and `false` identifiers, and restores the original identifier. Its tests cover both directions, restoration, unrelated identifiers, and shadowed names.

To add an operator:

1. Add its implementation and focused tests here.
2. Add one explicit definition to the catalog with a canonical name and default status.
3. Run `go test -tags test_dep ./tools/mutationtest/operators/... -count=1`.
4. Run `.bin/mutationtest -list-mutations` and verify its category and markers.
