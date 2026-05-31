// Package flakeshake reproduces CI-only timing flakes locally. It runs a caller-supplied
// `go test` invocation under gRPC + persistence Latency fault injection (the injected latency
// mimics a slow, contended CI machine), optionally over several rounds, and documents which
// tests failed.
//
// flakeshake owns only the fault flags and the multipliers that scale them; everything about
// the test invocation (package, -run, persistence, timeout, …) is forwarded verbatim from the
// args after `--`. Failure extraction reuses tools/common/gotestparse.
package flakeshake

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/urfave/cli/v2"
)

const (
	flagOutput                = "output"
	flagRounds                = "rounds"
	flagRPCMultiplier         = "rpc-multiplier"
	flagPersistenceMultiplier = "persistence-multiplier"
)

// Base ("extreme") latency in milliseconds that reliably reproduces the versioning flakes;
// the per-layer multipliers scale these.
const (
	baseRPCMeanMs    = 300
	baseRPCStddevMs  = 150
	baseRPCMaxMs     = 1500
	basePersMeanMs   = 150
	basePersStddevMs = 75
	basePersMaxMs    = 1500
)

// Main is the tool entrypoint.
func Main() {
	if err := NewCliApp().Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "flakeshake:", err)
		os.Exit(1)
	}
}

// NewCliApp builds the flakeshake CLI.
func NewCliApp() *cli.App {
	app := cli.NewApp()
	app.Name = "flakeshake"
	app.Usage = "run `go test` under RPC + persistence latency fault injection to reproduce flakes"
	app.UsageText = "flakeshake --output DIR [--rounds N] [--rpc-multiplier X] [--persistence-multiplier Y] -- <go test args>"
	app.Flags = []cli.Flag{
		&cli.StringFlag{Name: flagOutput, Usage: "directory for the report and per-round logs", Required: true},
		&cli.IntFlag{Name: flagRounds, Usage: "number of rounds to run", Value: 1},
		&cli.Float64Flag{Name: flagRPCMultiplier, Usage: "scale gRPC latency (1.0 = aggressive base)", Value: 1.0},
		&cli.Float64Flag{Name: flagPersistenceMultiplier, Usage: "scale persistence latency (1.0 = aggressive base)", Value: 1.0},
	}
	app.Action = run
	return app
}

func run(c *cli.Context) error {
	testArgs := c.Args().Slice()
	if len(testArgs) == 0 {
		return fmt.Errorf("provide go test args after `--`, e.g. -- ./tests -run TestX")
	}
	rpcMul, persMul := c.Float64(flagRPCMultiplier), c.Float64(flagPersistenceMultiplier)
	if rpcMul < 0 || persMul < 0 {
		return fmt.Errorf("multipliers must be >= 0")
	}
	rounds := max(c.Int(flagRounds), 1)
	outDir := c.String(flagOutput)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	faults := faultFlags(rpcMul, persMul)
	goArgs := append(append([]string{"test"}, testArgs...), faults...)

	agg := newAggregator()
	start := time.Now()
	for round := 1; round <= rounds; round++ {
		cmd := exec.Command("go", goArgs...)
		out, runErr := cmd.CombinedOutput()
		logPath := filepath.Join(outDir, fmt.Sprintf("flakeshake-round-%d.log", round))
		_ = os.WriteFile(logPath, out, 0o644)
		agg.addRound(string(out), runErr != nil)
		fmt.Printf("round %d/%d: %s\n", round, rounds, agg.lastStatus)
	}

	report := agg.report(reportMeta{
		rounds:   rounds,
		elapsed:  time.Since(start),
		goArgs:   goArgs,
		rpcMul:   rpcMul,
		persMul:  persMul,
		baseNote: fmt.Sprintf("rpc %g/%g/%g, persistence %g/%g/%g (mean/stddev/max ms)", baseRPCMeanMs*rpcMul, baseRPCStddevMs*rpcMul, baseRPCMaxMs*rpcMul, basePersMeanMs*persMul, basePersStddevMs*persMul, basePersMaxMs*persMul),
	})
	reportPath := filepath.Join(outDir, "flakeshake.md")
	if err := os.WriteFile(reportPath, []byte(report), 0o644); err != nil {
		return err
	}
	fmt.Printf("flakeshake: %d/%d rounds had failures; %d distinct flake(s)\nreport: %s\n",
		agg.roundsFailed, rounds, len(agg.failedTests), reportPath)
	return nil
}

// faultFlags renders the go test fault-injection flags for the given multipliers.
func faultFlags(rpcMul, persMul float64) []string {
	return []string{
		"-faultRPC=true",
		fmt.Sprintf("-faultRPCLatencyMeanMs=%g", baseRPCMeanMs*rpcMul),
		fmt.Sprintf("-faultRPCLatencyStddevMs=%g", baseRPCStddevMs*rpcMul),
		fmt.Sprintf("-faultRPCLatencyMaxMs=%g", baseRPCMaxMs*rpcMul),
		"-faultPersistenceLatency=true",
		fmt.Sprintf("-faultPersistenceLatencyMeanMs=%g", basePersMeanMs*persMul),
		fmt.Sprintf("-faultPersistenceLatencyStddevMs=%g", basePersStddevMs*persMul),
		fmt.Sprintf("-faultPersistenceLatencyMaxMs=%g", basePersMaxMs*persMul),
	}
}
