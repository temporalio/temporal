package testrunner

import (
	"bytes"
	"testing"

	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/stretchr/testify/require"
)

func TestGoTestJSONOutput_Output(t *testing.T) {
	testCases := []struct {
		name           string
		input          string
		expectedOutput string
		expectedStdout string // by default, expectedStdout is the same as expectedOutput
	}{
		{
			name: "buffers test output",
			input: `{"Time":"2026-07-28T00:00:00Z","Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestIncomplete"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"A1\n"}
{"Action":"run","Package":"example.com/tests","Test":"TestPass"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"B1\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"A2\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"B2\n"}
{"Action":"pass","Package":"example.com/tests","Test":"TestPass"}
{"Action":"output","Package":"example.com/tests","Output":"package output\n"}
{"Time":"2026-07-28T00:00:02Z","Action":"fail","Package":"example.com/tests","Elapsed":2}
`,
			expectedOutput: `B1
B2
package output
A1
A2

DONE 1 tests, 1 failure in 2.000s
`,
			expectedStdout: `package output
A1
A2

DONE 1 tests, 1 failure in 2.000s
`,
		},
		{
			name: "benchmark output",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"BenchmarkExample"}
{"Action":"output","Package":"example.com/tests","Test":"BenchmarkExample","Output":"benchmark log\n"}
{"Action":"bench","Package":"example.com/tests","Test":"BenchmarkExample","Output":"BenchmarkExample-12  1  100 ns/op\n"}
{"Action":"pass","Package":"example.com/tests","Elapsed":0.1}
`,
			expectedOutput: `benchmark log
BenchmarkExample-12  1  100 ns/op

DONE 0 tests in 0.100s
`,
		},
		{
			name: "hides skipped output",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestSkip"}
{"Action":"output","Package":"example.com/tests","Test":"TestSkip","Output":"=== RUN   TestSkip\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSkip","Output":"skip reason\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSkip","Output":"--- SKIP: TestSkip (0.00s)\n"}
{"Action":"skip","Package":"example.com/tests","Test":"TestSkip"}
{"Action":"pass","Package":"example.com/tests","Elapsed":0.1}
`,
			expectedOutput: `=== RUN   TestSkip
skip reason
--- SKIP: TestSkip (0.00s)

DONE 1 tests, 1 skipped in 0.100s
`,
			expectedStdout: `
DONE 1 tests, 1 skipped in 0.100s
`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output := newGoTestJSONOutput()
			var stdout bytes.Buffer
			output.stdout = &stdout
			_, err := output.Write([]byte(tc.input))
			require.NoError(t, err)
			require.Equal(t, tc.expectedOutput, output.String())
			expectedStdout := tc.expectedStdout
			if expectedStdout == "" {
				expectedStdout = tc.expectedOutput
			}
			require.Equal(t, expectedStdout, stdout.String())
		})
	}
}

func TestGoTestJSONOutput_ChunkedWrites(t *testing.T) {
	output := newGoTestJSONOutput()
	var stdout bytes.Buffer
	output.stdout = &stdout

	_, err := output.Write([]byte(`{"Action":"start","Package":"example.`))
	require.NoError(t, err)
	_, err = output.Write([]byte(`com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestPass"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"chunked\n"}
{"Action":"pass","Package":"example.com/tests","Test":"TestPass"}
{"Action":"pass","Package":"example.com/tests","Elapsed":0.1}`))
	require.NoError(t, err)

	expected := `chunked

DONE 1 tests in 0.100s
`
	require.Equal(t, expected, output.String())
	// The passing test's output is hidden from the live console.
	require.Equal(t, "\nDONE 1 tests in 0.100s\n", stdout.String())
}

func TestGoTestJSONOutput_MultiplePackages(t *testing.T) {
	input := `{"Time":"2026-07-28T00:00:00Z","Action":"start","Package":"example.com/one"}
{"Time":"2026-07-28T00:00:00.5Z","Action":"start","Package":"example.com/two"}
{"Action":"run","Package":"example.com/one","Test":"TestOne"}
{"Action":"output","Package":"example.com/one","Test":"TestOne","Output":"one-1\n"}
{"Action":"run","Package":"example.com/two","Test":"TestTwo"}
{"Action":"output","Package":"example.com/two","Test":"TestTwo","Output":"two-1\n"}
{"Action":"output","Package":"example.com/one","Test":"TestOne","Output":"one-2\n"}
{"Time":"2026-07-28T00:00:01.5Z","Action":"pass","Package":"example.com/two","Test":"TestTwo"}
{"Time":"2026-07-28T00:00:01.75Z","Action":"pass","Package":"example.com/two","Elapsed":1.25}
{"Time":"2026-07-28T00:00:02Z","Action":"pass","Package":"example.com/one","Test":"TestOne"}
{"Time":"2026-07-28T00:00:03Z","Action":"pass","Package":"example.com/one","Elapsed":3}
`
	expected := `two-1
one-1
one-2

DONE 2 tests in 3.000s
`

	output := newGoTestJSONOutput()
	output.stdout = &bytes.Buffer{}
	_, err := output.Write([]byte(input))
	require.NoError(t, err)
	require.Equal(t, expected, output.String())
}

func TestGoTestJSONOutput_Done(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "success",
			input: `{"Time":"2026-07-28T00:00:00Z","Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestPass"}
{"Action":"pass","Package":"example.com/tests","Test":"TestPass"}
{"Time":"2026-07-28T00:00:01Z","Action":"pass","Package":"example.com/tests","Elapsed":1}
`,
			expected: `
DONE 1 tests in 1.000s
`,
		},
		{
			name: "skipped",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestSkip"}
{"Action":"skip","Package":"example.com/tests","Test":"TestSkip"}
{"Action":"pass","Package":"example.com/tests","Elapsed":0.25}
`,
			expected: `
DONE 1 tests, 1 skipped in 0.250s
`,
		},
		{
			name: "failure",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestFail"}
{"Action":"fail","Package":"example.com/tests","Test":"TestFail"}
{"Action":"fail","Package":"example.com/tests","Elapsed":0.5}
`,
			expected: `
DONE 1 tests, 1 failure in 0.500s
`,
		},
		{
			name: "failures",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestFailOne"}
{"Action":"fail","Package":"example.com/tests","Test":"TestFailOne"}
{"Action":"run","Package":"example.com/tests","Test":"TestFailTwo"}
{"Action":"fail","Package":"example.com/tests","Test":"TestFailTwo"}
{"Action":"fail","Package":"example.com/tests","Elapsed":0.75}
`,
			expected: `
DONE 2 tests, 2 failures in 0.750s
`,
		},
		{
			name: "build errors",
			input: `{"ImportPath":"example.com/broken [example.com/broken.test]","Action":"build-output","Output":"# example.com/broken [example.com/broken.test]\n"}
{"ImportPath":"example.com/broken [example.com/broken.test]","Action":"build-output","Output":"broken.go:1: first error\n"}
{"ImportPath":"example.com/broken [example.com/broken.test]","Action":"build-output","Output":"\tadditional detail\n"}
{"ImportPath":"example.com/broken [example.com/broken.test]","Action":"build-output","Output":"broken.go:2: second error\n"}
{"Action":"start","Package":"example.com/broken"}
{"Action":"fail","Package":"example.com/broken","Elapsed":0.1}
`,
			expected: `# example.com/broken [example.com/broken.test]
broken.go:1: first error
	additional detail
broken.go:2: second error

DONE 0 tests, 1 failure, 2 errors in 0.100s
`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output := newGoTestJSONOutput()
			output.stdout = &bytes.Buffer{}
			_, err := output.Write([]byte(tc.input))
			require.NoError(t, err)
			require.Equal(t, tc.expected, output.String())
		})
	}
}

func TestGoTestJSONOutput_GeneratesReport(t *testing.T) {
	testCases := []struct {
		name           string
		input          string
		expectedOutput string
		expectedReport *junitReport
	}{
		{
			name: "aborted package",
			input: `{"Time":"2026-07-28T00:00:00Z","Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestIncomplete"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"=== RUN   TestIncomplete\n"}
{"Time":"2026-07-28T00:00:01Z","Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"unfinished\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"=== PAUSE TestIncomplete\n"}
{"Action":"pause","Package":"example.com/tests","Test":"TestIncomplete"}
{"Action":"output","Package":"example.com/tests","Output":"fatal LoadSchema: gocql: no response received from cassandra within timeout period\n"}
{"Action":"output","Package":"example.com/tests","Output":"FAIL\texample.com/tests\t1.000s\n"}
{"Time":"2026-07-28T00:00:01Z","Action":"fail","Package":"example.com/tests","Elapsed":1}
`,
			expectedOutput: `fatal LoadSchema: gocql: no response received from cassandra within timeout period
FAIL	example.com/tests	1.000s
=== RUN   TestIncomplete
unfinished
=== PAUSE TestIncomplete

DONE 0 tests, 1 failure in 1.000s
`,
			expectedReport: &junitReport{
				Testsuites: junit.Testsuites{
					Tests:    1,
					Failures: 1,
					Suites: []junit.Testsuite{
						{
							Name:      "example.com/tests",
							ID:        0,
							Time:      "1.000",
							SystemOut: &junit.Output{Data: "fatal LoadSchema: gocql: no response received from cassandra within timeout period"},
						},
						{
							Name:     testrunnerSuiteName,
							Tests:    1,
							Failures: 1,
							Testcases: []junit.Testcase{
								{
									Name: "testrunner.PackageAborted: example.com/tests",
									Failure: &junit.Result{
										Message: string(failureTypeAborted),
										Type:    string(failureTypeAborted),
										Data: "package example.com/tests exited before 1 tests produced terminal results\n\n" +
											"Recent package output:\n" +
											"unfinished\n" +
											"fatal LoadSchema: gocql: no response received from cassandra within timeout period\n",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "completed tests and build failure",
			input: `{"Action":"run","Package":"example.com/tests","Test":"TestIncomplete"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"incomplete\n"}
{"Action":"pause","Package":"example.com/tests","Test":"TestIncomplete"}
{"Action":"run","Package":"example.com/tests","Test":"TestFail"}
{"Action":"output","Package":"example.com/tests","Test":"TestFail","Output":"=== RUN   TestFail\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestFail","Output":"    test.go:1: failed\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestFail","Output":"--- FAIL: TestFail (0.01s)\n"}
{"Action":"fail","Package":"example.com/tests","Test":"TestFail","Elapsed":0.01}
{"Action":"fail","Package":"example.com/tests","Elapsed":0.01}
{"ImportPath":"example.com/broken [example.com/broken.test]","Action":"build-output","Output":"# example.com/broken [example.com/broken.test]\n"}
{"ImportPath":"example.com/broken [example.com/broken.test]","Action":"build-output","Output":"broken.go:1: compile error\n"}
{"ImportPath":"example.com/broken [example.com/broken.test]","Action":"build-fail"}
{"Action":"start","Package":"example.com/broken"}
{"Action":"output","Package":"example.com/broken","Output":"FAIL\texample.com/broken [build failed]\n"}
{"Action":"fail","Package":"example.com/broken","FailedBuild":"example.com/broken [example.com/broken.test]"}
`,
			expectedOutput: `=== RUN   TestFail
    test.go:1: failed
--- FAIL: TestFail (0.01s)
# example.com/broken [example.com/broken.test]
broken.go:1: compile error
FAIL	example.com/broken [build failed]
incomplete

DONE 1 tests, 2 failures, 1 error in 0.010s
`,
			expectedReport: &junitReport{
				Testsuites: junit.Testsuites{
					Tests:    2,
					Errors:   1,
					Failures: 1,
					Suites: []junit.Testsuite{
						{
							Name:   "example.com/broken",
							Tests:  1,
							Errors: 1,
							ID:     0,
							Time:   "0.000",
							Testcases: []junit.Testcase{
								{
									Name:      "[build failed]",
									Classname: "example.com/broken",
									Time:      "0.000",
									Error: &junit.Result{
										Message: "Build error",
										Data:    "broken.go:1: compile error",
									},
								},
							},
						},
						{
							Tests:    1,
							Failures: 1,
							ID:       1,
							Time:     "0.010",
							Testcases: []junit.Testcase{
								{
									Name:      "TestFail",
									Classname: "",
									Time:      "0.010",
									Failure: &junit.Result{
										Message: "Failed",
										Data:    "    test.go:1: failed",
									},
								},
							},
							SystemOut: &junit.Output{Data: "incomplete"},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output := newGoTestJSONOutput()
			output.stdout = &bytes.Buffer{}
			_, err := output.Write([]byte(tc.input))
			require.NoError(t, err)
			require.Equal(t, tc.expectedOutput, output.String())

			report, err := output.junitReport()
			require.NoError(t, err)
			for i := range report.Suites {
				report.Suites[i].Timestamp = ""
			}
			require.Equal(t, tc.expectedReport, report)
		})
	}
}
