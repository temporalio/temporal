//go:build gomad

package temporal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestDefaultMembershipRefusesUnbuiltRingpop(t *testing.T) {
	err := fx.New(defaultMembershipModule(), fx.NopLogger).Err()
	require.ErrorContains(t, err, "ringpop membership is not built under the gomad build tag; supply StaticServiceHosts")
}
