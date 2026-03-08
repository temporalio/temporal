//go:build gomad

package tests

import (
	"os"
	"testing"

	sim_ctrl "go.temporal.io/server/tools/gomad/ctrl"
)

func TestMain(m *testing.M) {
	os.Exit(sim_ctrl.RunInSim(m,
		sim_ctrl.ResetFiles,
	))
}
