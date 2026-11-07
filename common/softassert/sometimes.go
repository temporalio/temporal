//go:build !(gomad && wasip1 && wasm)

package softassert

// Sometimes declares a path that should be reached in some execution. The
// message must be a string literal. Ordinary builds evaluate the argument and
// perform no other work.
func Sometimes(staticMessage string) {}
