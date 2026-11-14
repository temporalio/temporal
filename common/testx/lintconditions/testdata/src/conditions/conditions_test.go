package conditions

func testPredicate(a, b, c, d bool) bool {
	return a && b && c && d // want "extract a named predicate"
}
