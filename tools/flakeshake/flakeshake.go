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
	"slices"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
)

const (
	flagOutput                = "output"
	flagRounds                = "rounds"
	flagRPCMultiplier         = "rpc-multiplier"
	flagPersistenceMultiplier = "persistence-multiplier"
	flagRPCErrorMultiplier    = "rpc-error-multiplier"
)

// Base latency in milliseconds at multiplier 1.0. Deliberately low: on a contended parallel
// suite even small latency surfaces timing flakes, while high latency makes every test blow
// its deadline. Escalate with the per-layer multipliers (e.g. for isolated single-test runs,
// which have no contention and need much more).
const (
	baseRPCMeanMs    = 25
	baseRPCStddevMs  = 15
	baseRPCMaxMs     = 250
	basePersMeanMs   = 25
	basePersStddevMs = 15
	basePersMaxMs    = 250
)

// baseRPCErrorRate is the transient-error probability at --rpc-error-multiplier 1.0; scaled by
// that multiplier (default 0 = off) the same way latency is scaled by its multipliers.
const baseRPCErrorRate = 0.05

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
		&cli.Float64Flag{Name: flagRPCErrorMultiplier, Usage: "scale the transient RPC-error rate from its base (0 = off, like the latency multipliers)", Value: 0},
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
	rpcErrorMul := c.Float64(flagRPCErrorMultiplier)
	if rpcErrorMul < 0 {
		return fmt.Errorf("--%s must be >= 0", flagRPCErrorMultiplier)
	}
	rpcErrorRate := min(baseRPCErrorRate*rpcErrorMul, 1.0)
	rounds := max(c.Int(flagRounds), 1)
	// Each run gets its own timestamped folder under --output.
	outDir := filepath.Join(c.String(flagOutput), "flakeshake-"+time.Now().Format("2006-01-02T15-04-05"))
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	faults := faultFlags(rpcMul, persMul, rpcErrorRate)
	goArgs := append([]string{"test"}, testArgs...)
	if !slices.Contains(goArgs, "-v") {
		goArgs = append(goArgs, "-v") // required so go-junit-report can parse per-test results
	}
	if !slices.Contains(goArgs, "-count=1") {
		goArgs = append(goArgs, "-count=1") // each round must re-run, not hit the test cache
	}
	goArgs = append(goArgs, faults...)

	reportPath := filepath.Join(outDir, "report.md")
	meta := reportMeta{
		rounds:   rounds,
		goArgs:   goArgs,
		rpcMul:   rpcMul,
		persMul:  persMul,
		baseNote: fmt.Sprintf("rpc %g/%g/%g, persistence %g/%g/%g (mean/stddev/max ms); rpc transient-error rate %g; GOMAXPROCS=1; per-test timeout %s", baseRPCMeanMs*rpcMul, baseRPCStddevMs*rpcMul, baseRPCMaxMs*rpcMul, basePersMeanMs*persMul, basePersStddevMs*persMul, basePersMaxMs*persMul, rpcErrorRate, time.Duration(float64(defaultPerTestTimeout)*max(rpcMul, persMul, 1))),
	}

	childEnv := testEnv(rpcMul, persMul)

	agg := newAggregator()
	start := time.Now()
	for round := 1; round <= rounds; round++ {
		cmd := exec.Command("go", goArgs...)
		cmd.Env = childEnv
		out, runErr := cmd.CombinedOutput()
		logPath := filepath.Join(outDir, fmt.Sprintf("round-%d.log", round))
		_ = os.WriteFile(logPath, out, 0o644)
		agg.addRound(string(out), runErr != nil)
		fmt.Printf("round %d/%d: %s\n", round, rounds, agg.lastStatus)

		// Write the report after every round so a long run's progress is never lost.
		meta.elapsed = time.Since(start)
		if err := os.WriteFile(reportPath, []byte(agg.report(meta)), 0o644); err != nil {
			return err
		}
	}

	fmt.Printf("flakeshake: %d/%d rounds had failures; %d distinct flake(s)\nreport: %s\n",
		agg.roundsFailed, rounds, len(agg.failedTests), reportPath)
	return nil
}

// defaultPerTestTimeout mirrors testcore's defaultTestTimeout (90s); the injected latency
// scales a test's work, so we scale this per-test budget by the latency multiplier to avoid
// blanket "context deadline exceeded" failures while still surfacing ordering flakes.
const defaultPerTestTimeout = 90 * time.Second

// testEnv returns the child environment for the test process. It pins GOMAXPROCS=1 to
// oversubscribe the scheduler — starving in-process timers and background loops the way a
// contended CI machine does, which surfaces contention flakes that pure RPC/persistence
// latency cannot. It also scales TEMPORAL_TEST_TIMEOUT by the larger multiplier (floored at
// 1x) so injected latency doesn't blow per-test deadlines. Either value the caller already
// set in the environment is left untouched.
func testEnv(rpcMul, persMul float64) []string {
	env := os.Environ()
	if !hasEnv(env, "GOMAXPROCS") {
		env = append(env, "GOMAXPROCS=1")
	}
	if !hasEnv(env, "TEMPORAL_TEST_TIMEOUT") {
		mul := max(rpcMul, persMul, 1)
		timeout := time.Duration(float64(defaultPerTestTimeout) * mul)
		env = append(env, "TEMPORAL_TEST_TIMEOUT="+timeout.String())
	}
	return env
}

func hasEnv(env []string, key string) bool {
	for _, e := range env {
		if strings.HasPrefix(e, key+"=") {
			return true
		}
	}
	return false
}

// faultFlags renders the go test fault-injection flags for the given multipliers and
// transient-error rate.
func faultFlags(rpcMul, persMul, rpcErrorRate float64) []string {
	flags := []string{
		"-faultRPC=true",
		fmt.Sprintf("-faultRPCLatencyMeanMs=%g", baseRPCMeanMs*rpcMul),
		fmt.Sprintf("-faultRPCLatencyStddevMs=%g", baseRPCStddevMs*rpcMul),
		fmt.Sprintf("-faultRPCLatencyMaxMs=%g", baseRPCMaxMs*rpcMul),
		"-faultPersistenceLatency=true",
		fmt.Sprintf("-faultPersistenceLatencyMeanMs=%g", basePersMeanMs*persMul),
		fmt.Sprintf("-faultPersistenceLatencyStddevMs=%g", basePersStddevMs*persMul),
		fmt.Sprintf("-faultPersistenceLatencyMaxMs=%g", basePersMaxMs*persMul),
	}
	if rpcErrorRate > 0 {
		flags = append(flags, fmt.Sprintf("-faultRPCErrorRate=%g", rpcErrorRate))
	}
	return flags
}
