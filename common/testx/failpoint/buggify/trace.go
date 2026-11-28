package buggify

import (
	"fmt"
	"io"
)

// TraceTo returns an observer for Buggify.OnDecision that writes one line per decision to w, in a
// stable format that a harness can parse: the prefix "BUGGIFY ", then space-separated key=value
// fields in this order, with the string values quoted like Go strings (strconv.Quote) and the
// others bare:
//
//	BUGGIFY event=activation key="<failpoint name>" active=<true|false>
//	BUGGIFY event=fire key="<failpoint name>" scope="<scope>" execution=<n> action=<action>
//
// An activation line is the decision whether the name is activated for the run. A fire line is an
// execution of an activated name that fires, with the number of the execution in its scope
// (executions are numbered per scope, which is why the line names it, and is empty for the unscoped
// failpoint), and the action that it takes, such as Return. Fields are only added at the end of a
// line, and the name of an event or field never changes meaning. Other events have no line. Write
// errors are ignored.
func TraceTo(w io.Writer) func(Event) {
	return func(e Event) {
		switch e.Kind {
		case EventActivation:
			_, _ = fmt.Fprintf(w, "BUGGIFY event=activation key=%q active=%t\n", e.Name, e.Activated)
		case EventFire:
			_, _ = fmt.Fprintf(w, "BUGGIFY event=fire key=%q scope=%q execution=%d action=%s\n", e.Name, e.Scope, e.ExecutionNumber, e.Action)
		default:
		}
	}
}
