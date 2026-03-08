package sim_ctrl

import (
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// gomadSeed is registered at init time so -gomad.seed appears in -help output.
var gomadSeed = flag.Int64("gomad.seed", 0, "fixed simulation seed for replay (0 = random)")

// RunInSim replaces m.Run() for packages that want to run under GoMaD simulation.
// It transforms the calling test package, compiles it into a simulated binary,
// runs that binary, and returns its exit code. The caller should pass the result
// directly to os.Exit.
//
// Source directory is inferred from the caller's file path (runtime.Caller(1)),
// which works correctly when called directly from TestMain.
//
// Output workspace defaults to $GOMAD_WORKDIR or <srcDir>/.gomad-run.
// Pass ResetFiles to force a full retransform.
func RunInSim(m *testing.M, opts ...Option) int {
	flag.Parse() // safe to call if testing already parsed; reads -gomad.seed

	_, callerFile, _, _ := runtime.Caller(1)
	srcDir := filepath.Dir(callerFile)

	outDir := os.Getenv("GOMAD_WORKDIR")
	if outDir == "" {
		outDir = filepath.Join(srcDir, ".gomad-run")
	}

	ctrlOpts := []Option{WithSourceDir(srcDir)}

	if *gomadSeed != 0 {
		ctrlOpts = append(ctrlOpts, FixedSeed(*gomadSeed))
	}

	// Pass -test.run through to the inner binary if set.
	if f := flag.Lookup("test.run"); f != nil && f.Value.String() != "" {
		ctrlOpts = append(ctrlOpts, WithTest(f.Value.String()))
	}

	// Pass -test.timeout through via extraTestFlags.
	if f := flag.Lookup("test.timeout"); f != nil && f.Value.String() != "10m0s" {
		ctrlOpts = append(ctrlOpts, withExtraTestFlags("-test.timeout="+f.Value.String()))
	}

	ctrlOpts = append(ctrlOpts, opts...)

	return NewController(ctrlOpts...).RunTests(outDir)
}

// withExtraTestFlags is an internal option that appends raw -test.* flags.
func withExtraTestFlags(flags ...string) Option {
	return func(c *Controller) {
		c.conf.extraTestFlags = append(c.conf.extraTestFlags, flags...)
	}
}
