package update

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAbortReasonUpdateStateMatrix(t *testing.T) {
	for r := AbortReasonRegistryCleared; r < lastAbortReason; r++ {
		for st := stateCreated; st < lastState; st <<= 1 {
			fe, ok := reasonStateMatrix[reasonState{r: r, st: st}]
			// If new abort reason or state is added, this test will fail.
			// Do not modify the test but make sure to update the reasonStateMatrix.
			require.True(t, ok, "Missing combination: %v, %v. If new abort reason or state is added make sure to update the reasonStateMatrix", r, st)
			if fe.f != nil {
				require.Nil(t, fe.err)
			}
			if fe.err != nil {
				require.Nil(t, fe.f)
			}
		}
	}
}
