//go:build gomad && wasip1 && wasm

package softassert

// Sometimes requires build-time source binding to a catalog ID. Running an
// unbound call cannot provide semantic coverage.
func Sometimes(staticMessage string) {
	panic("softassert.Sometimes requires semantic catalog source binding")
}
