package buggify_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
)

// The trace format is stable: a harness parses it.
func TestTraceTo(t *testing.T) {
	t.Parallel()
	var out strings.Builder
	trace := buggify.TraceTo(&out)

	trace(buggify.Event{Kind: buggify.EventActivation, Name: "a.b", Activated: true})
	trace(buggify.Event{Kind: buggify.EventActivation, Name: "c", Activated: false})
	trace(buggify.Event{Kind: buggify.EventFire, Name: "a.b", Scope: "ns", ExecutionNumber: 3, Action: failpoint.ActionReturn})
	trace(buggify.Event{Kind: buggify.EventFire, Name: `q"`, ExecutionNumber: 1, Action: failpoint.ActionOn})
	trace(buggify.Event{Kind: buggify.EventScopeLimit})

	require.Equal(t, `BUGGIFY event=activation key="a.b" active=true
BUGGIFY event=activation key="c" active=false
BUGGIFY event=fire key="a.b" scope="ns" execution=3 action=Return
BUGGIFY event=fire key="q\"" scope="" execution=1 action=On
`, out.String())
}
