//go:build !gomad

package temporal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/membership/ringpop"
)

// Invariant: every build except the gomad tag, including the gomad_wasm_slim
// WASM guest, selects ringpop as the default membership module.
func TestDefaultMembershipLinksRingpop(t *testing.T) {
	require.Equal(t, ringpop.MembershipModule, defaultMembershipModule())
}
