package testrunner

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGoTestRecorderBuildsPackageAwareAttemptResult(t *testing.T) {
	input := `{"Time":"2026-08-11T01:00:00Z","Action":"start","Package":"example.com/one"}
{"Action":"run","Package":"example.com/one","Test":"TestSame"}
{"Action":"run","Package":"example.com/two","Test":"TestSame"}
{"Action":"output","Package":"example.com/two","Test":"TestSame","Output":"two output\n"}
{"Action":"output","Package":"example.com/one","Test":"TestSame","Output":"one output\n"}
{"Action":"pass","Package":"example.com/two","Test":"TestSame","Elapsed":0.25}
{"Action":"fail","Package":"example.com/one","Test":"TestSame","Elapsed":0.5}
{"Action":"pass","Package":"example.com/two","Elapsed":0.3}
{"Action":"fail","Package":"example.com/one","Elapsed":0.6}
`
	var console bytes.Buffer
	recorder := newGoTestRecorder(&console)
	_, err := recorder.Write([]byte(input))
	require.NoError(t, err)
	result := recorder.finish(processResult{state: processExited, exitCode: 1, duration: time.Second})

	require.Equal(t, []packageResult{
		{
			name:      "example.com/one",
			startedAt: time.Date(2026, 8, 11, 1, 0, 0, 0, time.UTC),
			duration:  600 * time.Millisecond,
			outcome:   packageFailed,
			executions: []testExecution{{
				id:       testID{"example.com/one", "TestSame"},
				outcome:  testFailed,
				duration: 500 * time.Millisecond,
				output:   "one output\n",
			}},
		},
		{
			name:     "example.com/two",
			duration: 300 * time.Millisecond,
			outcome:  packagePassed,
			executions: []testExecution{{
				id:       testID{"example.com/two", "TestSame"},
				outcome:  testPassed,
				duration: 250 * time.Millisecond,
				output:   "two output\n",
			}},
		},
	}, result.packages)
	require.Contains(t, console.String(), "one output")
	require.NotContains(t, console.String(), "two output")
}

func TestGoTestRecorderRetainsOccurrencesBuildsAndCoverage(t *testing.T) {
	input := `{"Action":"run","Package":"example.com/tests","Test":"TestRepeated"}
{"Action":"pass","Package":"example.com/tests","Test":"TestRepeated","Elapsed":0.1}
{"Action":"run","Package":"example.com/tests","Test":"TestRepeated"}
{"Action":"fail","Package":"example.com/tests","Test":"TestRepeated","Elapsed":0.2}
{"Action":"output","Package":"example.com/tests","Output":"coverage: 0.0% of statements\n"}
{"ImportPath":"example.com/broken.test","Action":"build-output","Output":"compile failed\n"}
{"ImportPath":"example.com/broken.test","Action":"build-fail"}
{"Action":"fail","Package":"example.com/tests","FailedBuild":"example.com/broken.test"}
`
	recorder := newGoTestRecorder(&bytes.Buffer{})
	_, err := recorder.Write([]byte(input))
	require.NoError(t, err)
	result := recorder.finish(processResult{state: processExited, exitCode: 1})

	require.Len(t, result.packages, 1)
	require.Equal(t, 0, result.packages[0].executions[0].occurrence)
	require.Equal(t, 1, result.packages[0].executions[1].occurrence)
	require.NotNil(t, result.packages[0].coverage)
	require.InDelta(t, 0, *result.packages[0].coverage, 0)
	require.Equal(t, "example.com/broken.test", result.packages[0].failedBuild)
	require.Equal(t, []buildResult{{
		importPath: "example.com/broken.test",
		failed:     true,
		output:     "compile failed\n",
	}}, result.builds)
}

func TestGoTestRecorderFinalizesPartialAndMalformedLines(t *testing.T) {
	var console bytes.Buffer
	recorder := newGoTestRecorder(&console)
	_, err := recorder.Write([]byte("not json\n"))
	require.NoError(t, err)
	_, err = recorder.Write([]byte(`{"Action":"run","Package":"example.com/tests","Test":"TestPartial"}`))
	require.NoError(t, err)
	result := recorder.finish(processResult{state: processExited, exitCode: 1})

	require.Equal(t, "not json\n", result.unstructuredOutput)
	require.Equal(t, testIncomplete, result.packages[0].executions[0].outcome)
	require.Contains(t, console.String(), "not json")
}

func TestGoTestRecorderExcludesBenchmarks(t *testing.T) {
	input := `{"Action":"run","Package":"example.com/tests","Test":"BenchmarkExample"}
{"Action":"output","Package":"example.com/tests","Test":"BenchmarkExample","Output":"BenchmarkExample-12  1  100 ns/op\n"}
{"Action":"pass","Package":"example.com/tests"}
`
	recorder := newGoTestRecorder(&bytes.Buffer{})
	_, err := recorder.Write([]byte(input))
	require.NoError(t, err)
	result := recorder.finish(processResult{state: processExited})

	require.Empty(t, result.packages[0].executions)
	require.True(t, result.successful())

	failing := newGoTestRecorder(&bytes.Buffer{})
	_, err = failing.Write([]byte(`{"Action":"run","Package":"example.com/tests","Test":"BenchmarkFailure"}
{"Action":"output","Package":"example.com/tests","Test":"BenchmarkFailure","Output":"benchmark failure detail\n"}
{"Action":"fail","Package":"example.com/tests","Test":"BenchmarkFailure"}
{"Action":"fail","Package":"example.com/tests"}
`))
	require.NoError(t, err)
	failedResult := failing.finish(processResult{state: processExited, exitCode: 1})
	require.Empty(t, failedResult.packages[0].executions)
	require.Contains(t, failedResult.packages[0].output, "benchmark failure detail")
	require.Len(t, failedResult.runtimeFailures(), 1)
}

func TestGoTestRecorderCreatesOccurrenceForTerminalWithoutRun(t *testing.T) {
	recorder := newGoTestRecorder(&bytes.Buffer{})
	_, err := recorder.Write([]byte(`{"Action":"skip","Package":"example.com/tests","Test":"TestCached","Elapsed":0.1}`))
	require.NoError(t, err)
	result := recorder.finish(processResult{state: processExited})

	require.Equal(t, []testExecution{{
		id:       testID{"example.com/tests", "TestCached"},
		outcome:  testSkipped,
		duration: 100 * time.Millisecond,
	}}, result.packages[0].executions)
}

func TestGoTestRecorderCorrelatesConditionalChildWithParentOccurrence(t *testing.T) {
	input := `{"Action":"run","Package":"example.com/tests","Test":"TestRepeated"}
{"Action":"pass","Package":"example.com/tests","Test":"TestRepeated"}
{"Action":"run","Package":"example.com/tests","Test":"TestRepeated"}
{"Action":"run","Package":"example.com/tests","Test":"TestRepeated/ConditionalChild"}
{"Action":"fail","Package":"example.com/tests","Test":"TestRepeated/ConditionalChild"}
{"Action":"fail","Package":"example.com/tests","Test":"TestRepeated"}
{"Action":"fail","Package":"example.com/tests"}
`
	recorder := newGoTestRecorder(&bytes.Buffer{})
	_, err := recorder.Write([]byte(input))
	require.NoError(t, err)
	result := recorder.finish(processResult{state: processExited, exitCode: 1})

	require.Equal(t, 1, result.packages[0].executions[1].occurrence)
	require.Equal(t, 1, result.packages[0].executions[2].occurrence)
	require.Equal(t, []testID{{"example.com/tests", "TestRepeated/ConditionalChild"}}, result.failedLeafTests())
}

func TestGoTestRecorderAttributesCleanupFailureWithoutTerminalTestEvent(t *testing.T) {
	input := `{"Action":"run","Package":"example.com/tests","Test":"TestSuite"}
{"Action":"run","Package":"example.com/tests","Test":"TestSuite/TestCleanupFailure"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite/TestCleanupFailure","Output":"    context.go:132: \n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite/TestCleanupFailure","Output":"        Error Trace: cleanup_test.go:42\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite/TestCleanupFailure","Output":"        Error: test exceeded timeout\n"}
{"Action":"fail","Package":"example.com/tests","Test":"TestSuite"}
{"Action":"fail","Package":"example.com/tests"}
`
	recorder := newGoTestRecorder(&bytes.Buffer{})
	_, err := recorder.Write([]byte(input))
	require.NoError(t, err)
	result := recorder.finish(processResult{state: processExited, exitCode: 1})

	require.Equal(t, []testID{{"example.com/tests", "TestSuite/TestCleanupFailure"}}, result.failedLeafTests())
	require.Empty(t, result.abortedPackages())
	require.True(t, result.canTargetFailures())
}

func TestGoTestRecorderDoesNotInventOccurrenceForPostTerminalMetadata(t *testing.T) {
	input := `{"Action":"run","Package":"example.com/tests","Test":"TestFinished"}
{"Action":"pass","Package":"example.com/tests","Test":"TestFinished"}
{"Action":"attr","Package":"example.com/tests","Test":"TestFinished"}
{"Action":"artifacts","Package":"example.com/tests","Test":"TestFinished"}
{"Action":"fail","Package":"example.com/tests"}
`
	recorder := newGoTestRecorder(&bytes.Buffer{})
	_, err := recorder.Write([]byte(input))
	require.NoError(t, err)
	result := recorder.finish(processResult{state: processExited, exitCode: 1})

	require.Len(t, result.packages[0].executions, 1)
	require.Equal(t, testPassed, result.packages[0].executions[0].outcome)
	require.Empty(t, result.abortedPackages())
}

func TestGoTestRecorderFindsDiagnosticInHiddenPassingOutputAndStderr(t *testing.T) {
	input := `{"Action":"run","Package":"example.com/tests","Test":"TestPass"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"panic: hidden panic\n"}
{"Action":"pass","Package":"example.com/tests","Test":"TestPass"}
{"Action":"pass","Package":"example.com/tests"}
`
	var console bytes.Buffer
	recorder := newGoTestRecorder(&console)
	_, err := recorder.Write([]byte(input))
	require.NoError(t, err)
	result := recorder.finish(processResult{
		state:    processExited,
		exitCode: 1,
		stderr:   "fatal error: stderr failure\n",
	})

	require.Len(t, result.diagnostics, 2)
	require.Equal(t, diagnosticPanic, result.diagnostics[0].kind)
	require.Equal(t, []testID{{"example.com/tests", "TestPass"}}, result.diagnostics[0].tests)
	require.Equal(t, diagnosticFatal, result.diagnostics[1].kind)
	require.Empty(t, result.diagnostics[1].tests)
	require.NotContains(t, console.String(), "hidden panic")
}
