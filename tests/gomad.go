//go:build ignore

package main

import (
	"os"
	"path/filepath"
	"runtime"

	sim_ctrl "go.temporal.io/server/tools/gomad/ctrl"
)

func main() {
	_, thisFile, _, _ := runtime.Caller(0)
	srcDir, _ := filepath.Abs(filepath.Dir(thisFile))

	os.Exit(sim_ctrl.NewController(
		sim_ctrl.WithSourceDir(srcDir),
		sim_ctrl.WithTest("TestWorkflowUpdateSuite"),
		sim_ctrl.ResetFiles,
	).RunTests(filepath.Join(srcDir, "..", "tools", "gomad", "tests", "gomad-run")))
}
