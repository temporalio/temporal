// accessorlib is the dependency package for the cross-package scenario. It
// exposes a borrowed accessor (returns a live field by reference) and a safe one
// (returns a fresh copy). The analyzer must infer each result's form and EXPORT
// it as an ObjectFact so importing packages resolve them without re-analysis.
package accessorlib

type Payload struct{}

type Vis struct {
	sa map[string]*Payload
}

// CustomSearchAttributes returns the live map by reference (inferred borrowed).
func (v *Vis) CustomSearchAttributes() map[string]*Payload { // want CustomSearchAttributes:`borrowed result`
	return v.sa
}

// SafeSearchAttributes returns a fresh copy (inferred owned); embedding its
// result must NOT be flagged.
func (v *Vis) SafeSearchAttributes() map[string]*Payload {
	out := make(map[string]*Payload, len(v.sa))
	for k, val := range v.sa {
		out[k] = val
	}
	return out
}
