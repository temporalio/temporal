// ESCAPE-FUNCS fixture. Run with -escape-funcs=escapefuncslib.Retain#0, declaring
// that arg 0 of the opaque callee Retain escapes (is retained/marshaled out of
// view). Passing a borrowed map there is then flagged at the call site, even though
// inference sees no leak inside Retain. An owned value passed to the same callee is
// silent.
package escapefuncs

import "escapefuncslib"

type comp struct {
	shared map[string]string
}

func (c *comp) Leak() {
	escapefuncslib.Retain(c.shared) // borrowed receiver field // want `borrowed map passed to Retain`
}

func (c *comp) Safe() {
	fresh := map[string]string{}
	escapefuncslib.Retain(fresh) // owned local: silent
}
