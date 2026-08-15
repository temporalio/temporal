package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire/verify"
)

type recordingExecutor struct {
	commands  []command
	results   []execution
	beforeRun func(command)
}

func (e *recordingExecutor) run(_ context.Context, specification command) execution {
	e.commands = append(e.commands, specification)
	if e.beforeRun != nil {
		e.beforeRun(specification)
	}
	result := e.results[0]
	e.results = e.results[1:]
	return result
}

func TestExecuteApalacheCollectsNativeITFTrace(t *testing.T) {
	artifactDirectory := t.TempDir()
	nativeTrace := `{"#meta":{"format":"ITF"},"states":[]}`
	executor := &recordingExecutor{
		results: []execution{{output: "Error: Invariant Safety is violated", err: errors.New("exit status 1")}},
		beforeRun: func(command) {
			nativeDirectory := filepath.Join(artifactDirectory, "apalache-native", "Umpire.tla", "run")
			require.NoError(t, os.MkdirAll(nativeDirectory, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(nativeDirectory, "example.itf.json"), []byte(nativeTrace), 0o600))
		},
	}

	actual, _, err := execute(context.Background(), executor, Request{
		Backend: Apalache, ToolPath: "/tools/apalache", ModelDir: t.TempDir(), ArtifactDir: artifactDirectory,
	})
	require.NoError(t, err)
	require.Equal(t, nativeTrace, actual.nativeTrace)
}

func TestExecuteApalacheCollectsNativeTraceWithoutArtifactDirectory(t *testing.T) {
	nativeTrace := `{"#meta":{"format":"ITF"},"states":[]}`
	executor := &recordingExecutor{
		results: []execution{{output: "Error: Invariant Safety is violated", err: errors.New("exit status 1")}},
		beforeRun: func(specification command) {
			for _, argument := range specification.args {
				if !strings.HasPrefix(argument, "--out-dir=") {
					continue
				}
				nativeDirectory := filepath.Join(strings.TrimPrefix(argument, "--out-dir="), "Umpire.tla", "run")
				require.NoError(t, os.MkdirAll(nativeDirectory, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(nativeDirectory, "example.itf.json"), []byte(nativeTrace), 0o600))
			}
		},
	}

	actual, _, err := execute(context.Background(), executor, Request{
		Backend: Apalache, ToolPath: "/tools/apalache", ModelDir: t.TempDir(),
	})
	require.NoError(t, err)
	require.Equal(t, nativeTrace, actual.nativeTrace)
	require.Condition(t, func() bool {
		return slices.ContainsFunc(executor.commands[0].args, func(argument string) bool {
			return strings.HasPrefix(argument, "--out-dir=")
		})
	})
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

func TestExecuteApalacheProofCollectsFailedObligationNativeTrace(t *testing.T) {
	artifactDirectory := t.TempDir()
	executor := &recordingExecutor{
		results: []execution{
			{output: "Checker reports no error"},
			{output: "Error: Invariant InductiveInvariant is violated", err: errors.New("exit status 1")},
		},
		beforeRun: func(specification command) {
			for _, argument := range specification.args {
				if !strings.HasPrefix(argument, "--out-dir=") {
					continue
				}
				outputDirectory := strings.TrimPrefix(argument, "--out-dir=")
				nativeDirectory := filepath.Join(outputDirectory, "Umpire.tla", "run")
				require.NoError(t, os.MkdirAll(nativeDirectory, 0o755))
				require.NoError(t, os.WriteFile(
					filepath.Join(nativeDirectory, "example.itf.json"),
					[]byte(filepath.Base(outputDirectory)),
					0o600,
				))
			}
		},
	}

	actual, _, err := execute(context.Background(), executor, Request{
		Backend: ApalacheProof, ToolPath: "/tools/apalache", ModelDir: t.TempDir(), ArtifactDir: artifactDirectory,
	})
	require.NoError(t, err)
	require.Equal(t, "consecution", actual.nativeTrace)
}

func TestExecuteApalacheProofCollectsNativeTraceWithoutArtifactDirectory(t *testing.T) {
	executor := &recordingExecutor{
		results: []execution{{output: "Error: Invariant InductiveInvariant is violated", err: errors.New("exit status 1")}},
		beforeRun: func(specification command) {
			for _, argument := range specification.args {
				if !strings.HasPrefix(argument, "--out-dir=") {
					continue
				}
				nativeDirectory := filepath.Join(strings.TrimPrefix(argument, "--out-dir="), "Umpire.tla", "run")
				require.NoError(t, os.MkdirAll(nativeDirectory, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(nativeDirectory, "example.itf.json"), []byte("init"), 0o600))
			}
		},
	}

	actual, _, err := execute(context.Background(), executor, Request{
		Backend: ApalacheProof, ToolPath: "/tools/apalache", ModelDir: t.TempDir(),
	})
	require.NoError(t, err)
	require.Equal(t, "init", actual.nativeTrace)
}

func TestExecuteIvyRequestsTextualCounterexampleTrace(t *testing.T) {
	executor := &recordingExecutor{results: []execution{{output: "OK"}}}

	_, _, err := execute(context.Background(), executor, Request{
		Backend: Ivy, ToolPath: "/tools/ivy_check", ModelDir: t.TempDir(),
	})
	require.NoError(t, err)
	require.Contains(t, executor.commands[0].args, "trace=true")
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

func TestCollectPTraceEvidenceRejectsOversizedCombinedLogs(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "first.log"), []byte(strings.Repeat("a", maxNativeTraceBytes/2)), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "second.log"), []byte(strings.Repeat("b", maxNativeTraceBytes/2)), 0o600))

	_, err := collectPTraceEvidence(directory)
	require.ErrorIs(t, err, errNativeTraceTooLarge)
}

func TestCollectApalacheTraceEvidenceReadsCanonicalITFFile(t *testing.T) {
	directory := t.TempDir()
	nested := filepath.Join(directory, "Umpire.tla", "run")
	require.NoError(t, os.MkdirAll(nested, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(nested, "example.itf.json"), []byte(`{"#meta":{"format":"ITF"}}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(nested, "example0.itf.json"), []byte("duplicate"), 0o600))

	evidence, err := collectApalacheTraceEvidence(directory)
	require.NoError(t, err)
	require.Equal(t, `{"#meta":{"format":"ITF"}}`, evidence)
}

func TestCollectApalacheTraceEvidenceRejectsOversizedITFFile(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(directory, "example.itf.json"),
		[]byte(strings.Repeat("x", maxNativeTraceBytes+1)),
		0o600,
	))

	_, err := collectApalacheTraceEvidence(directory)
	require.ErrorIs(t, err, errNativeTraceTooLarge)
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
	result := classify(Request{Backend: P, ToolVersion: "3.1.0", Model: runnerCounterexampleModel()}, execution{
		output: "UMPIRE_ACTION schedule op=NexusOperation#0\nError: Assertion Failed: property terminal-link",
		err:    errors.New("exit status 1"),
	})
	require.Equal(t, verify.Counterexample, result.Status)
	require.Equal(t, "terminal-link", result.FailedProperty)
	require.Equal(t, []verify.TraceStep{{
		Action:   "schedule",
		Bindings: verify.Bindings{"op": "NexusOperation#0"},
		Deltas: []verify.StateDelta{{
			Entity: "NexusOperation", ID: "NexusOperation#0", ToState: "scheduled",
		}},
	}}, result.Trace)
}

func TestClassifyPRejectsCounterexampleWithoutNativeTrace(t *testing.T) {
	output := "Error: Assertion Failed: property terminal-link"
	result := classify(Request{Backend: P, Model: runnerCounterexampleModel()}, execution{
		output: output,
		err:    errors.New("exit status 1"),
	})

	require.Equal(t, verify.Inconclusive, result.Status)
	require.Equal(t, verify.EvidenceFailure, result.Termination)
	require.Contains(t, result.Diagnostic, "native-trace-missing")
	require.Equal(t, output, result.NativeTrace)
}

func TestClassifyPRejectsOversizedNativeTrace(t *testing.T) {
	output := "UMPIRE_ACTION schedule op=NexusOperation#0\nError: Assertion Failed: property terminal-link\n" +
		strings.Repeat("x", maxNativeTraceBytes)
	result := classify(Request{Backend: P, Model: runnerCounterexampleModel()}, execution{
		output: output,
		err:    errors.New("exit status 1"),
	})

	require.Equal(t, verify.Inconclusive, result.Status)
	require.Equal(t, verify.EvidenceFailure, result.Termination)
	require.Contains(t, result.Diagnostic, "native-trace-too-large")
}

func TestClassifyPRejectsUnmappedFailedProperty(t *testing.T) {
	result := classify(Request{Backend: P, Model: runnerCounterexampleModel()}, execution{
		output: "UMPIRE_ACTION schedule op=NexusOperation#0\nError: Assertion Failed",
		err:    errors.New("exit status 1"),
	})

	require.Equal(t, verify.Inconclusive, result.Status)
	require.Equal(t, verify.EvidenceFailure, result.Termination)
	require.Contains(t, result.Diagnostic, "property-unmapped")
}

func TestFailedPropertyCandidatesRequireGeneratedCardinalityInvariant(t *testing.T) {
	request := Request{Model: verify.Model{Relations: []verify.Relation{{
		Name: "link", Source: "source", Target: "target",
		SourceCardinality: verify.Many, TargetCardinality: verify.Many,
	}}}}

	require.Empty(t, failedPropertyCandidates(request, "relation link source cardinality"))
	request.Model.Relations[0].SourceCardinality = verify.One
	require.Equal(t, []string{"relation link source cardinality"}, failedPropertyCandidates(request, "relation link source cardinality"))
}

func TestClassifyPRejectsCounterexampleWithoutCanonicalModel(t *testing.T) {
	result := classify(Request{Backend: P}, execution{
		output: "UMPIRE_ACTION schedule op=NexusOperation#0\nError: Assertion Failed: property terminal-link",
		err:    errors.New("exit status 1"),
	})

	require.Equal(t, verify.Inconclusive, result.Status)
	require.Equal(t, verify.EvidenceFailure, result.Termination)
	require.Contains(t, result.Diagnostic, "native-trace-malformed")
}

func runnerCounterexampleModel() verify.Model {
	return verify.Model{
		Version: "runner-counterexample-test/v1",
		Entities: []verify.EntityType{{
			Name: "NexusOperation", IDs: []string{"NexusOperation#0"}, Initial: "unscheduled",
			States: []verify.State{{Name: "unscheduled"}, {Name: "scheduled"}},
		}},
		Actions: []verify.Action{{
			Name:       "schedule",
			Parameters: []verify.Parameter{{Name: "op", Type: "NexusOperation", Binding: verify.FreshBinding}},
			Effects: []verify.Effect{{
				Kind: verify.CreateEffect, Entity: "NexusOperation", Ref: "op", State: "scheduled",
			}},
		}},
		Properties: []verify.Property{{
			Name: "terminal-link", Kind: verify.SafetyProperty,
			Expr: verify.Expr{Op: verify.ForAllExpr, Entity: "NexusOperation", Var: "op", Args: []verify.Expr{{
				Op: verify.NotExpr, Args: []verify.Expr{verify.StateIs("NexusOperation", "op", "scheduled")},
			}}},
		}},
	}
}

func TestNormalizeActionsReadsPCheckerBugTrace(t *testing.T) {
	trace := normalizeActions(
		Request{Backend: P},
		"UMPIRE_ACTION seed.bug source=source#0 target=target#0\nProperty violated: property reciprocal-link failed\nFound 1 bug.",
	)

	require.Equal(t, []verify.TraceStep{{
		Action: "seed.bug",
		Bindings: verify.Bindings{
			"source": "source#0",
			"target": "target#0",
		},
	}}, trace)
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
	require.Equal(t, verify.ToolLimit, result.Termination)
	require.Contains(t, result.Diagnostic, "choice limit")
}

func TestPCheckArgsTreatsMaxStepsAsABound(t *testing.T) {
	args := pCheckArgs("/tmp/Umpire.jar", "pex", Request{Bounds: verify.Bounds{MaxDepth: 20, Schedules: 30}})
	require.Contains(t, args, "--max-steps")
	require.Contains(t, args, "20")
	require.NotContains(t, args, "--fail-on-maxsteps")
}

func TestNormalizeActionsReadsTLCCounterexample(t *testing.T) {
	trace := normalizeActions(Request{
		Backend:     TLC,
		ToolVersion: "1.7.4",
		ActionNames: map[string]string{"ScheduleNexus": "nexus.schedule"},
	}, "State 1: <Initial predicate>\nState 2: <ScheduleNexus(op = \"NexusOperation#0\") line 1>\nError: Invariant terminal-link is violated.")

	require.Equal(t, []verify.TraceStep{{Action: "nexus.schedule", Bindings: verify.Bindings{"op": "NexusOperation#0"}}}, trace)
}

func TestClassifyTLCCounterexampleReplaysNativeStates(t *testing.T) {
	output := `State 1: <Initial predicate>
/\ exists_NexusOperation = {}
/\ state_NexusOperation = ("NexusOperation#0" :> "unscheduled")
State 2: <Action_schedule(op = "NexusOperation#0") line 1, col 1 to line 1, col 1 of module Umpire>
/\ exists_NexusOperation = {"NexusOperation#0"}
/\ state_NexusOperation = ("NexusOperation#0" :> "scheduled")
Error: Invariant Property_terminal_link is violated.`
	result := classify(Request{
		Backend: TLC,
		Model:   runnerCounterexampleModel(),
		TraceVocabulary: verify.TraceVocabulary{
			Properties:   map[string][]string{"Property_terminal_link": {"terminal-link"}},
			EntityExists: map[string]string{"exists_NexusOperation": "NexusOperation"},
			EntityStates: map[string]string{"state_NexusOperation": "NexusOperation"},
		},
		ActionNames: map[string]string{"Action_schedule": "schedule"},
	}, execution{output: output, err: errors.New("exit status 12")})

	require.Equal(t, verify.Counterexample, result.Status, result.Diagnostic)
	require.Equal(t, "terminal-link", result.FailedProperty)
	require.Equal(t, []verify.TraceStep{{
		Action:   "schedule",
		Bindings: verify.Bindings{"op": "NexusOperation#0"},
		Deltas: []verify.StateDelta{{
			Entity: "NexusOperation", ID: "NexusOperation#0", ToState: "scheduled",
		}},
	}}, result.Trace)
	require.Equal(t, output, result.NativeTrace)
}

func TestClassifyTLCRejectsOversizedNativeTrace(t *testing.T) {
	output := `State 1: <Initial predicate>
/\ exists_NexusOperation = {}
/\ state_NexusOperation = ("NexusOperation#0" :> "unscheduled")
State 2: <Action_schedule(op = "NexusOperation#0")>
/\ exists_NexusOperation = {"NexusOperation#0"}
/\ state_NexusOperation = ("NexusOperation#0" :> "scheduled")
Error: Invariant Property_terminal_link is violated.
` + strings.Repeat("x", maxNativeTraceBytes)
	result := classify(Request{
		Backend: TLC,
		Model:   runnerCounterexampleModel(),
		TraceVocabulary: verify.TraceVocabulary{
			Properties:   map[string][]string{"Property_terminal_link": {"terminal-link"}},
			EntityExists: map[string]string{"exists_NexusOperation": "NexusOperation"},
			EntityStates: map[string]string{"state_NexusOperation": "NexusOperation"},
		},
		ActionNames: map[string]string{"Action_schedule": "schedule"},
	}, execution{output: output, err: errors.New("exit status 12")})

	require.Equal(t, verify.Inconclusive, result.Status)
	require.Equal(t, verify.EvidenceFailure, result.Termination)
	require.Contains(t, result.Diagnostic, "native-trace-too-large")
}

func TestClassifyApalacheCounterexampleReplaysITFStates(t *testing.T) {
	nativeTrace := runnerITFCounterexample()
	result := classify(Request{
		Backend: Apalache,
		Model:   runnerCounterexampleModel(),
		TraceVocabulary: verify.TraceVocabulary{
			Properties:   map[string][]string{"Safety": {"terminal-link"}},
			EntityExists: map[string]string{"exists_NexusOperation": "NexusOperation"},
			EntityStates: map[string]string{"state_NexusOperation": "NexusOperation"},
		},
	}, execution{
		output:      "Error: Invariant Safety is violated.",
		nativeTrace: nativeTrace,
		err:         errors.New("exit status 1"),
	})

	require.Equal(t, verify.Counterexample, result.Status, result.Diagnostic)
	require.Equal(t, "terminal-link", result.FailedProperty)
	require.Equal(t, []verify.TraceStep{{
		Action:   "schedule",
		Bindings: verify.Bindings{"op": "NexusOperation#0"},
		Deltas: []verify.StateDelta{{
			Entity: "NexusOperation", ID: "NexusOperation#0", ToState: "scheduled",
		}},
	}}, result.Trace)
	require.Equal(t, nativeTrace, result.NativeTrace)
}

func TestClassifyApalacheReadsPinnedInvariantFailureMarker(t *testing.T) {
	result := classify(Request{
		Backend: Apalache,
		Model:   runnerCounterexampleModel(),
		TraceVocabulary: verify.TraceVocabulary{
			Properties:   map[string][]string{"Safety": {"terminal-link"}},
			EntityExists: map[string]string{"exists_NexusOperation": "NexusOperation"},
			EntityStates: map[string]string{"state_NexusOperation": "NexusOperation"},
		},
	}, execution{
		output:      "State 1: state invariant 0 [Safety] violated.\nFound 1 error(s)\nThe outcome is: Error",
		nativeTrace: runnerITFCounterexample(),
		err:         errors.New("exit status 1"),
	})

	require.Equal(t, verify.Counterexample, result.Status, result.Diagnostic)
	require.Equal(t, "terminal-link", result.FailedProperty)
}

func runnerITFCounterexample() string {
	return `{
  "#meta": {"format": "ITF"},
  "vars": ["exists_NexusOperation", "state_NexusOperation"],
  "states": [
    {
      "exists_NexusOperation": {"#set": []},
      "state_NexusOperation": {"#map": [["NexusOperation#0", "unscheduled"]]}
    },
    {
      "exists_NexusOperation": {"#set": ["NexusOperation#0"]},
      "state_NexusOperation": {"#map": [["NexusOperation#0", "scheduled"]]}
    }
  ]
}`
}

func TestClassifyApalacheRejectsOversizedNativeTrace(t *testing.T) {
	result := classify(Request{
		Backend: Apalache,
		Model:   runnerCounterexampleModel(),
		TraceVocabulary: verify.TraceVocabulary{
			Properties: map[string][]string{"Safety": {"terminal-link"}},
		},
	}, execution{
		output:      "Error: Invariant Safety is violated.",
		nativeTrace: strings.Repeat("x", 4<<20+1),
		err:         errors.New("exit status 1"),
	})

	require.Equal(t, verify.Inconclusive, result.Status)
	require.Equal(t, verify.EvidenceFailure, result.Termination)
	require.Contains(t, result.Diagnostic, "native-trace-too-large")
}

func TestClassifyIvyCounterexampleReplaysTextualTrace(t *testing.T) {
	output := runnerIvyCounterexampleOutput()
	result := classify(runnerIvyCounterexampleRequest(), execution{
		output: output,
		err:    errors.New("exit status 1"),
	})

	require.Equal(t, verify.Counterexample, result.Status, result.Diagnostic)
	require.Equal(t, "terminal-link", result.FailedProperty)
	require.Equal(t, []verify.TraceStep{{
		Action:   "schedule",
		Bindings: verify.Bindings{"op": "NexusOperation#0"},
		Deltas: []verify.StateDelta{{
			Entity: "NexusOperation", ID: "NexusOperation#0", ToState: "scheduled",
		}},
	}}, result.Trace)
	require.Equal(t, output, result.NativeTrace)
}

func runnerIvyCounterexampleOutput() string {
	return `        (internal) schedule
            Umpire.ivy: line 20: terminal_link ... FAIL
error: failed checks: 1
Trace follows...
********************************************************************************
[
    exists_nexusoperation(nexusoperation_0) = false
	 op = nexusoperation_0
    state_nexusoperation(nexusoperation_0) = nexusoperation_state_unscheduled
]
call schedule
[
    exists_nexusoperation(nexusoperation_0) = true
    state_nexusoperation(nexusoperation_0) = nexusoperation_state_scheduled
]`
}

func TestClassifyIvyCounterexampleResolvesSolverIdentityAliases(t *testing.T) {
	output := `Umpire.ivy: line 20: terminal_link ... FAIL
error: failed checks: 1
Trace follows...
********************************************************************************
[
    exists_nexusoperation(nexusoperation:0) = false
    nexusoperation_0 = nexusoperation:0
    op = nexusoperation:0
    state_nexusoperation(nexusoperation:0) = nexusoperation_state_unscheduled
]
call schedule
[
    exists_nexusoperation(nexusoperation:0) = true
    state_nexusoperation(nexusoperation:0) = nexusoperation_state_scheduled
]`
	result := classify(runnerIvyCounterexampleRequest(), execution{
		output: output,
		err:    errors.New("exit status 1"),
	})

	require.Equal(t, verify.Counterexample, result.Status, result.Diagnostic)
	require.Equal(t, verify.Bindings{"op": "NexusOperation#0"}, result.Trace[0].Bindings)
}

func runnerIvyCounterexampleRequest() Request {
	return Request{
		Backend: Ivy,
		Model:   runnerCounterexampleModel(),
		TraceVocabulary: verify.TraceVocabulary{
			Actions:      map[string]string{"schedule": "schedule"},
			Bindings:     map[string]map[string]string{"schedule": {"op": "op"}},
			Properties:   map[string][]string{"terminal_link": {"terminal-link"}},
			EntityExists: map[string]string{"exists_nexusoperation": "NexusOperation"},
			EntityStates: map[string]string{"state_nexusoperation": "NexusOperation"},
			Identities:   map[string]string{"nexusoperation_0": "NexusOperation#0"},
			States: map[string]string{
				"nexusoperation_state_unscheduled": "unscheduled",
				"nexusoperation_state_scheduled":   "scheduled",
			},
		},
	}
}

func TestClassifyReportedLimitsAsInconclusive(t *testing.T) {
	tests := []struct {
		name        string
		backend     Backend
		execution   execution
		termination verify.TerminationReason
	}{
		{name: "depth", backend: PEx, execution: execution{output: "Result: correct up to step 100\nFound 0 bugs"}, termination: verify.DepthLimit},
		{name: "state", backend: TLC, execution: execution{output: "State limit reached\nModel checking completed. No error has been found"}, termination: verify.StateLimit},
		{name: "step", backend: P, execution: execution{output: "Scheduling steps bound of 100 reached.\nFound 0 bugs"}, termination: verify.StepLimit},
		{name: "memory", backend: PEx, execution: execution{output: "Max memory limit reached: 1024 MB\nFound 0 bugs"}, termination: verify.MemoryLimit},
		{name: "schedule", backend: PEx, execution: execution{output: "Found 0 bugs\nFinished 100 search tasks (2 pending)"}, termination: verify.ScheduleLimit},
		{name: "timeout output", backend: Apalache, execution: execution{output: "State 3: invariant => TIMEOUT. Assuming it holds true.\nThe outcome is: NoError"}, termination: verify.Timeout},
		{name: "timeout context", backend: Ivy, execution: execution{err: context.DeadlineExceeded}, termination: verify.Timeout},
		{name: "interruption", backend: TLC, execution: execution{err: context.Canceled}, termination: verify.Interrupted},
		{name: "tool", backend: PEx, execution: execution{output: "TooManyChoicesException: too many choices generated from this statement"}, termination: verify.ToolLimit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := classify(Request{Backend: test.backend}, test.execution)
			require.Equal(t, verify.Inconclusive, result.Status)
			require.Equal(t, test.termination, result.Termination)
		})
	}
}

func TestClassifyCompletedBoundedExplorationAsSuccess(t *testing.T) {
	result := classify(Request{Backend: PEx, Bounds: verify.Bounds{MaxDepth: 100, Schedules: 100}}, execution{
		output: "Found 0 bugs\nFinished 100 search tasks (0 pending)",
	})

	require.Equal(t, verify.BoundedNoCounterexample, result.Status)
	require.Equal(t, verify.Completed, result.Termination)
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

func TestClassifyReturnsUnsupportedForRelevantSemanticGap(t *testing.T) {
	unsupported := []verify.Unsupported{{Backend: "ivy", Construct: "property delivery.progress", Reason: "not inductive"}}
	result := classify(Request{Backend: Ivy, Unsupported: unsupported}, execution{output: "OK"})

	require.Equal(t, verify.UnsupportedStatus, result.Status)
	require.Equal(t, verify.Completed, result.Termination)
	require.Equal(t, unsupported, result.Unsupported)
	require.Contains(t, result.Diagnostic, "delivery.progress")
}

func TestClassifyPreservesCounterexampleDespiteUnsupportedSemantics(t *testing.T) {
	unsupported := []verify.Unsupported{{Backend: "ivy", Construct: "property delivery.progress", Reason: "not inductive"}}
	request := runnerIvyCounterexampleRequest()
	request.Unsupported = unsupported
	result := classify(request, execution{
		output: runnerIvyCounterexampleOutput(),
		err:    errors.New("exit status 1"),
	})

	require.Equal(t, verify.Counterexample, result.Status)
	require.Equal(t, "terminal-link", result.FailedProperty)
	require.Equal(t, unsupported, result.Unsupported)
}

func TestClassifyOmitsOtherBackendSemanticGaps(t *testing.T) {
	result := classify(Request{
		Backend:     SANY,
		Unsupported: []verify.Unsupported{{Backend: "ivy", Construct: "progress", Reason: "not inductive"}},
	}, execution{output: "Semantic processing of module Umpire"})

	require.Equal(t, verify.Generated, result.Status)
	require.Empty(t, result.Unsupported)
}

func TestClassifyCarriesModelAssumptions(t *testing.T) {
	request := Request{
		Backend:      SANY,
		Target:       "protocol-atomic",
		Profile:      "smoke",
		Fairness:     []string{"weak-schedule"},
		Abstractions: []verify.Abstraction{{Name: "environment", Reason: "unrealized"}},
	}
	result := classify(request, execution{output: "Semantic processing of module Umpire"})
	require.Equal(t, request.Fairness, result.Fairness)
	require.Equal(t, request.Abstractions, result.Abstractions)
	require.Equal(t, "protocol-atomic", result.Target)
	require.Equal(t, "smoke", result.Profile)
}
