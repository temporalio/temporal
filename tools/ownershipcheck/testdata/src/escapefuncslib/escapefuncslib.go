// escapefuncslib stands in for an opaque / cross-module callee. Retain pretends to
// stash its argument somewhere the analyzer cannot trace (a cache, a queue, an async
// marshal), so inference sees no leak and exports no fact — forcing reliance on the
// -escape-funcs config to know that arg 0 escapes.
package escapefuncslib

func Retain(m map[string]string) {
	_ = m // retained out of view; nothing the analyzer can follow
}
