package conditions

func contexts(a, b, c, d, e bool) bool {
	if a && b {
	}
	if a && b && c {
	}
	if a || b || c || d { // want "extract a named predicate, preferably a method on the owning type"
	}
	for a && b && c && d { // want "extract a named predicate"
		break
	}
	switch a || b || c || d { // want "extract a named predicate"
	case a && b && c && d: // want "extract a named predicate"
	}
	switch {
	case a || b || c || d: // want "extract a named predicate"
	}
	x := a && b && c && d        // want "extract a named predicate"
	x = a || b && c || d         // want "extract a named predicate"
	var y = a && b || c && d     // want "extract a named predicate"
	consume(a && b && c && d)    // want "extract a named predicate"
	_ = []bool{a || b || c || d} // want "extract a named predicate"
	_, _ = x, y
	return a && b && c && d && e // want "extract a named predicate"
}

func boundaries(a, b, c, d bool) {
	consume((a && b) || (c && d))
	consume((a || b || c) && d)
	consume((a && b && c && d))                // want "extract a named predicate"
	consume((a && b && c && d) || a || b || c) // want "extract a named predicate" "extract a named predicate"
	consume(!a && b && c)
	consume(!(a && b && c && d))  // want "extract a named predicate"
	consume(!a && !b && !c && !d) // want "extract a named predicate"
	consume(a == b && b != c && c == d)
	consume(a == (a && b && c && d)) // want "extract a named predicate"
	consume(a || b && c)
	consume(a && b || c && d) // want "extract a named predicate"
	consume(a && func() bool {
		return a || b || c || d // want "extract a named predicate"
	}())
	_ = func() {
		consume(func() bool {
			return a && b && c && d // want "extract a named predicate"
		}())
	}
}

func consume(bool) {
}
