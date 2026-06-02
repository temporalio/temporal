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
	"bufio"
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
	flagDuration              = "duration"
	flagRounds                = "rounds"
	flagRPCMultiplier         = "rpc-multiplier"
	flagPersistenceMultiplier = "persistence-multiplier"
	flagRPCErrorMultiplier    = "rpc-error-multiplier"
	flagMatchingMultiplier    = "matching-multiplier"
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

// baseMatchingUserDataFetchLatencyMs is the in-process matching user-data fetch latency at
// --matching-multiplier 1.0 (delays versioning-data propagation); 0 = off.
const baseMatchingUserDataFetchLatencyMs = 75

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
	app.UsageText = "flakeshake --output DIR [--duration 30m | --rounds N] [--rpc-multiplier X] -- <go test args>"
	app.Flags = []cli.Flag{
		&cli.StringFlag{Name: flagOutput, Usage: "directory for the report and per-round logs", Required: true},
		&cli.DurationFlag{Name: flagDuration, Usage: "keep running rounds until this much time has elapsed (0 = use --rounds)", Value: 0},
		&cli.IntFlag{Name: flagRounds, Usage: "max number of rounds (0 = unlimited; defaults to 1 only when --duration is also unset)", Value: 0},
		&cli.Float64Flag{Name: flagRPCMultiplier, Usage: "scale gRPC latency (1.0 = aggressive base)", Value: 1.0},
		&cli.Float64Flag{Name: flagPersistenceMultiplier, Usage: "scale persistence latency (1.0 = aggressive base)", Value: 1.0},
		&cli.Float64Flag{Name: flagRPCErrorMultiplier, Usage: "scale the transient RPC-error rate from its base (0 = off, like the latency multipliers)", Value: 0},
		&cli.Float64Flag{Name: flagMatchingMultiplier, Usage: "scale in-process matching user-data fetch latency from its base (0 = off)", Value: 0},
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
	matchingMul := c.Float64(flagMatchingMultiplier)
	if matchingMul < 0 {
		return fmt.Errorf("--%s must be >= 0", flagMatchingMultiplier)
	}
	matchingFetchLatencyMs := baseMatchingUserDataFetchLatencyMs * matchingMul

	// Run budget: time-based (--duration), count-based (--rounds), or both (whichever ends
	// first). When neither is set, default to a single round.
	maxRounds := c.Int(flagRounds)
	duration := c.Duration(flagDuration)
	if maxRounds == 0 && duration == 0 {
		maxRounds = 1
	}
	budget := runBudgetNote(maxRounds, duration)

	// Each run gets its own timestamped folder under --output.
	outDir := filepath.Join(c.String(flagOutput), "flakeshake-"+time.Now().Format("2006-01-02T15-04-05"))
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	faults := faultFlags(rpcMul, persMul, rpcErrorRate, matchingFetchLatencyMs)
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
		budget:   budget,
		goArgs:   goArgs,
		rpcMul:   rpcMul,
		persMul:  persMul,
		baseNote: fmt.Sprintf("rpc %g/%g/%g, persistence %g/%g/%g (mean/stddev/max ms); rpc transient-error rate %g; matching user-data fetch latency %gms; GOMAXPROCS=1; per-test timeout %s", baseRPCMeanMs*rpcMul, baseRPCStddevMs*rpcMul, baseRPCMaxMs*rpcMul, basePersMeanMs*persMul, basePersStddevMs*persMul, basePersMaxMs*persMul, rpcErrorRate, matchingFetchLatencyMs, time.Duration(float64(defaultPerTestTimeout)*max(rpcMul, persMul, 1))),
	}

	childEnv := testEnv(rpcMul, persMul)

	agg := newAggregator()
	start := time.Now()
	writeReport := func() {
		meta.elapsed = time.Since(start)
		_ = os.WriteFile(reportPath, []byte(agg.report(meta)), 0o644)
	}

	for round := 1; ; round++ {
		if maxRounds > 0 && round > maxRounds {
			break
		}
		if duration > 0 && time.Since(start) >= duration {
			break
		}
		logPath := filepath.Join(outDir, fmt.Sprintf("round-%d.log", round))
		out, _ := runRound(goArgs, childEnv, logPath, func(buf string) {
			// Streamed live update: refresh the in-progress round's failures and rewrite the
			// report so flakes show up the moment they happen, not at round end.
			agg.setLive(round, parseFailures(buf))
			writeReport()
		})
		agg.commitRound(out) // go test exit code ignored; only reported test failures count
		fmt.Printf("round %d: %s\n", round, agg.lastStatus)
		writeReport()
	}

	fmt.Printf("flakeshake: %d/%d rounds had failures; %d distinct flake(s)\nreport: %s\n",
		agg.roundsFailed, agg.roundsDone, len(agg.failedTests), reportPath)
	return nil
}

// runBudgetNote describes the run budget for the report.
func runBudgetNote(maxRounds int, duration time.Duration) string {
	switch {
	case duration > 0 && maxRounds > 0:
		return fmt.Sprintf("duration %s, max %d rounds", duration, maxRounds)
	case duration > 0:
		return fmt.Sprintf("duration %s", duration)
	default:
		return fmt.Sprintf("%d rounds", maxRounds)
	}
}

// runRound runs one `go test` invocation, tees its combined output to logPath, and invokes
// onNewFailure(currentBuffer) each time a new "--- FAIL: <name>" line appears so the caller
// can refresh a live report. It returns the full output and the command's exit error.
func runRound(goArgs, env []string, logPath string, onNewFailure func(buffer string)) (string, error) {
	cmd := exec.Command("go", goArgs...)
	cmd.Env = env
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	cmd.Stderr = cmd.Stdout // merge stderr so test output + injected-latency logs are one stream
	if err := cmd.Start(); err != nil {
		return "", err
	}

	logFile, _ := os.Create(logPath)
	var buf strings.Builder
	seen := map[string]struct{}{}
	scanner := bufio.NewScanner(pipe)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024) // tests emit very long lines
	for scanner.Scan() {
		line := scanner.Text()
		buf.WriteString(line)
		buf.WriteByte('\n')
		if logFile != nil {
			fmt.Fprintln(logFile, line)
		}
		// Trigger a live update only when a not-yet-seen test fails — bounds re-parsing to the
		// number of distinct failures (cheap even with very verbose debug logs).
		if name, ok := failLineTestName(line); ok {
			if _, dup := seen[name]; !dup {
				seen[name] = struct{}{}
				if onNewFailure != nil {
					onNewFailure(buf.String())
				}
			}
		}
	}
	if logFile != nil {
		_ = logFile.Close()
	}
	return buf.String(), cmd.Wait()
}

// failLineTestName returns the test name from a "    --- FAIL: TestX (0.00s)" line.
func failLineTestName(line string) (string, bool) {
	line = strings.TrimSpace(line)
	rest, ok := strings.CutPrefix(line, "--- FAIL:")
	if !ok {
		return "", false
	}
	name, _, _ := strings.Cut(strings.TrimSpace(rest), " ")
	return name, name != ""
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

// faultFlags renders the go test fault-injection flags for the given multipliers, transient
// RPC-error rate, and in-process matching user-data fetch latency.
func faultFlags(rpcMul, persMul, rpcErrorRate, matchingFetchLatencyMs float64) []string {
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
	if matchingFetchLatencyMs > 0 {
		flags = append(flags, fmt.Sprintf("-faultMatchingUserDataFetchLatencyMs=%g", matchingFetchLatencyMs))
	}
	return flags
}
