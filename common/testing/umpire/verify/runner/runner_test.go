package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire/verify"
)

type recordingExecutor struct {
	commands []command
	results  []execution
}

func (e *recordingExecutor) run(_ context.Context, specification command) execution {
	e.commands = append(e.commands, specification)
	result := e.results[0]
	e.results = e.results[1:]
	return result
}

func TestExecuteApalacheProofRunsThreeObligations(t *testing.T) {
	executor := &recordingExecutor{results: []execution{
		{output: "Checker reports no error", stdout: "init"},
		{output: "Checker reports no error", stdout: "consecution"},
		{output: "Checker reports no error", stdout: "safety"},
	}}
	request := Request{Backend: ApalacheProof, ToolPath: "/tools/apalache", ModelDir: t.TempDir(), ArtifactDir: t.TempDir()}

	actual, replay, err := execute(context.Background(), executor, request)
	require.NoError(t, err)
	require.Len(t, executor.commands, 3)
	require.Contains(t, executor.commands[0].args, "--init=Init")
	require.Contains(t, executor.commands[0].args, "--length=0")
	require.Contains(t, executor.commands[1].args, "--init=InductiveInvariant")
	require.Contains(t, executor.commands[1].args, "--next=Next")
	require.Contains(t, executor.commands[1].args, "--inv=InductiveInvariant")
	require.Contains(t, executor.commands[1].args, "--length=1")
	require.Contains(t, executor.commands[2].args, "--init=InductiveInvariant")
	require.Contains(t, executor.commands[2].args, "--inv=DeclaredSafety")
	require.Contains(t, actual.output, "UMPIRE_PROOF_OBLIGATION safety")
	require.Len(t, replay, 3)
}

func TestFindPAssemblyFindsPExJar(t *testing.T) {
	directory := t.TempDir()
	jar := filepath.Join(directory, "PGenerated", "PEx", "target", "Umpire-jar-with-dependencies.jar")
	require.NoError(t, os.MkdirAll(filepath.Dir(jar), 0o755))
	require.NoError(t, os.WriteFile(jar, nil, 0o600))

	actual, err := findPAssembly(directory, "pex")
	require.NoError(t, err)
	require.Equal(t, jar, actual)
}

func TestJavaEnvironmentUsesRequestedExecutable(t *testing.T) {
	require.Equal(t, []string{
		"JAVA_HOME=/tools/jdk",
		"PATH=/tools/jdk/bin:" + os.Getenv("PATH"),
	}, javaEnvironment("/tools/jdk/bin/java"))
}

func TestCollectPTraceEvidenceIncludesCheckerLogs(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "trace.log"), []byte("UMPIRE_ACTION seed.bug\nproperty reciprocal-link failed"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "trace.schedule"), []byte("opaque"), 0o600))

	evidence, err := collectPTraceEvidence(directory)
	require.NoError(t, err)
	require.Contains(t, evidence, "UMPIRE_ACTION seed.bug")
	require.NotContains(t, evidence, "opaque")
}

func TestClassifyTLCFiniteExhaustive(t *testing.T) {
	result := classify(Request{Backend: TLC, ToolVersion: "1.7.4"}, execution{
		output: "Model checking completed. No error has been found.\n123 states generated, 45 distinct states found",
	})
	require.Equal(t, verify.FiniteExhaustive, result.Status)
	require.Equal(t, uint64(123), result.GeneratedStates)
	require.Equal(t, uint64(45), result.DistinctStates)
}

func TestClassifyCounterexampleNormalizesPActions(t *testing.T) {
	result := classify(Request{Backend: P, ToolVersion: "3.1.0"}, execution{
		output: "UMPIRE_ACTION schedule op=NexusOperation#0\nError: Assertion Failed: property terminal-link",
		err:    errors.New("exit status 1"),
	})
	require.Equal(t, verify.Counterexample, result.Status)
	require.Equal(t, "terminal-link", result.FailedProperty)
	require.Equal(t, []verify.TraceStep{{Action: "schedule", Bindings: verify.Bindings{"op": "NexusOperation#0"}}}, result.Trace)
}

func TestClassifyPCheckerBugMarkerAsCounterexample(t *testing.T) {
	result := classify(Request{Backend: P}, execution{
		output: "UMPIRE_ACTION seed.bug source=source#0 target=target#0\nProperty violated: property reciprocal-link failed\nFound 1 bug.",
		err:    errors.New("exit status 1"),
	})
	require.Equal(t, verify.Counterexample, result.Status)
	require.Equal(t, "reciprocal-link", result.FailedProperty)
	require.Equal(t, "seed.bug", result.Trace[0].Action)
}

func TestClassifyPExImplicitCycleAsInconclusive(t *testing.T) {
	result := classify(Request{Backend: PEx}, execution{
		output: "Property violated: Cycle detected: Infinite loop found due to revisiting a state multiple times in the same schedule\nFound 1 bug.",
		err:    errors.New("exit status 1"),
	})
	require.Equal(t, verify.Inconclusive, result.Status)
	require.Equal(t, verify.ToolError, result.Termination)
	require.Contains(t, result.Diagnostic, "implicit cycle")
}

func TestClassifyPExNativeChoiceLimitAsInconclusive(t *testing.T) {
	result := classify(Request{Backend: PEx}, execution{
		output: "TooManyChoicesException: too many choices generated from this statement",
		err:    errors.New("exit status 1"),
	})
	require.Equal(t, verify.Inconclusive, result.Status)
	require.Equal(t, verify.StepLimit, result.Termination)
	require.Contains(t, result.Diagnostic, "choice limit")
}

func TestPCheckArgsTreatsMaxStepsAsABound(t *testing.T) {
	args := pCheckArgs("/tmp/Umpire.jar", "pex", Request{Bounds: verify.Bounds{MaxDepth: 20, Schedules: 30}})
	require.Contains(t, args, "--max-steps")
	require.Contains(t, args, "20")
	require.NotContains(t, args, "--fail-on-maxsteps")
}

func TestClassifyCounterexampleNormalizesTLCActions(t *testing.T) {
	result := classify(Request{
		Backend:     TLC,
		ToolVersion: "1.7.4",
		ActionNames: map[string]string{"ScheduleNexus": "nexus.schedule"},
	}, execution{
		output: "State 1: <Initial predicate>\nState 2: <ScheduleNexus(op = \"NexusOperation#0\") line 1>\nError: Invariant terminal-link is violated.",
		err:    errors.New("exit status 12"),
	})
	require.Equal(t, verify.Counterexample, result.Status)
	require.Equal(t, "terminal-link", result.FailedProperty)
	require.Equal(t, []verify.TraceStep{{Action: "nexus.schedule", Bindings: verify.Bindings{"op": "NexusOperation#0"}}}, result.Trace)
}

func TestClassifyCounterexampleNormalizesIvyFailedProof(t *testing.T) {
	result := classify(Request{
		Backend:     Ivy,
		ToolVersion: "1.8.26",
		ActionNames: map[string]string{"seed_bug": "seed.bug"},
	}, execution{
		output: "        (internal) seed_bug\n            Umpire.ivy: line 20: seeded_property ... FAIL\nerror: failed checks: 1",
		err:    errors.New("exit status 1"),
	})
	require.Equal(t, verify.Counterexample, result.Status)
	require.Equal(t, "seeded_property", result.FailedProperty)
	require.Equal(t, []verify.TraceStep{{Action: "seed.bug"}}, result.Trace)
}

func TestClassifyLimitsAndTimeoutsAsInconclusive(t *testing.T) {
	limited := classify(Request{Backend: P}, execution{output: "max scheduling steps bound reached"})
	require.Equal(t, verify.Inconclusive, limited.Status)
	require.Equal(t, verify.StepLimit, limited.Termination)

	timedOut := classify(Request{Backend: Ivy}, execution{err: context.DeadlineExceeded})
	require.Equal(t, verify.Inconclusive, timedOut.Status)
	require.Equal(t, verify.Timeout, timedOut.Termination)
}

func TestClassifyDoesNotTreatProtocolTimeoutActionAsRunnerTimeout(t *testing.T) {
	result := classify(Request{Backend: Ivy}, execution{output: "checking action nexus_timeout\nOK"})
	require.Equal(t, verify.InvariantProved, result.Status)
	require.Equal(t, verify.Completed, result.Termination)
}

func TestClassifyBackendGuarantees(t *testing.T) {
	tests := []struct {
		backend Backend
		output  string
		status  verify.Status
	}{
		{backend: SANY, output: "Semantic processing of module Umpire", status: verify.Generated},
		{backend: Apalache, output: "The outcome is: NoError", status: verify.BoundedNoCounterexample},
		{backend: ApalacheProof, output: "UMPIRE_PROOF_OBLIGATION init\nChecker reports no error\nUMPIRE_PROOF_OBLIGATION consecution\nChecker reports no error\nUMPIRE_PROOF_OBLIGATION safety\nChecker reports no error", status: verify.InvariantProved},
		{backend: P, output: "... Testing statistics: Found 0 bugs", status: verify.BoundedNoCounterexample},
		{backend: Ivy, output: "OK", status: verify.InvariantProved},
	}
	for _, test := range tests {
		t.Run(string(test.backend), func(t *testing.T) {
			result := classify(Request{Backend: test.backend}, execution{output: test.output})
			require.Equal(t, test.status, result.Status)
		})
	}
}

func TestClassifyCarriesModelAssumptions(t *testing.T) {
	request := Request{
		Backend:      SANY,
		Fairness:     []string{"weak-schedule"},
		Abstractions: []verify.Abstraction{{Name: "environment", Reason: "unrealized"}},
		Unsupported:  []verify.Unsupported{{Backend: "ivy", Construct: "progress", Reason: "not inductive"}},
	}
	result := classify(request, execution{output: "Semantic processing of module Umpire"})
	require.Equal(t, request.Fairness, result.Fairness)
	require.Equal(t, request.Abstractions, result.Abstractions)
	require.Equal(t, request.Unsupported, result.Unsupported)
}
