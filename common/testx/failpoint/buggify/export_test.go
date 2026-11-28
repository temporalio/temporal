package buggify

// DrawScoped is Draw for the executions of a name in a scope, which BUGGIFY numbers separately for
// each scope, so the executions in one scope don't change which executions fire in another.
// Without a scope, it is Draw.
func (c Config) DrawScoped(name, scope string, n uint64) float64 {
	return draw(c.Seed, nameMix(name)^scopeMix(scope), n)
}
