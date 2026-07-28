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
		expectedStdout string
	}{
		{
			name: "shows failing tests and hides passing tests from stdout",
			input: `{"Time":"2026-07-28T00:00:00Z","Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestFail"}
{"Action":"output","Package":"example.com/tests","Test":"TestFail","Output":"=== RUN   TestFail\n"}
{"Action":"run","Package":"example.com/tests","Test":"TestPass"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"=== RUN   TestPass\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestFail","Output":"=== PAUSE TestFail\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestFail","Output":"=== CONT  TestFail\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestFail","Output":"=== NAME  TestFail\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestFail","Output":"    foo_test.go:10: boom\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"    pass log\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"--- PASS: TestPass (0.00s)\n"}
{"Action":"pass","Package":"example.com/tests","Test":"TestPass"}
{"Action":"output","Package":"example.com/tests","Test":"TestFail","Output":"--- FAIL: TestFail (0.00s)\n"}
{"Action":"fail","Package":"example.com/tests","Test":"TestFail"}
{"Action":"output","Package":"example.com/tests","Output":"FAIL\n"}
{"Time":"2026-07-28T00:00:02Z","Action":"fail","Package":"example.com/tests","Elapsed":2}
`,
			expectedOutput: `=== RUN   TestPass
    pass log
--- PASS: TestPass (0.00s)
=== RUN   TestFail
=== PAUSE TestFail
=== CONT  TestFail
=== NAME  TestFail
    foo_test.go:10: boom
--- FAIL: TestFail (0.00s)
FAIL

DONE 2 tests, 1 failure in 2.000s
`,
			expectedStdout: `=== RUN   TestFail
    foo_test.go:10: boom
--- FAIL: TestFail (0.00s)
FAIL

DONE 2 tests, 1 failure in 2.000s
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
			name: "hides skipped output from stdout",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestSkip"}
{"Action":"output","Package":"example.com/tests","Test":"TestSkip","Output":"=== RUN   TestSkip\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSkip","Output":"    skip_test.go:5: not now\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSkip","Output":"--- SKIP: TestSkip (0.00s)\n"}
{"Action":"skip","Package":"example.com/tests","Test":"TestSkip"}
{"Action":"pass","Package":"example.com/tests","Elapsed":0.1}
`,
			expectedOutput: `=== RUN   TestSkip
    skip_test.go:5: not now
--- SKIP: TestSkip (0.00s)

DONE 1 tests, 1 skipped in 0.100s
`,
			expectedStdout: `
DONE 1 tests, 1 skipped in 0.100s
`,
		},
		{
			name: "hides framing-only incomplete tests from stdout",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"=== RUN   TestIncomplete\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"=== PAUSE TestIncomplete\n"}
{"Action":"pause","Package":"example.com/tests","Test":"TestIncomplete"}
{"Action":"output","Package":"example.com/tests","Output":"fatal package error\n"}
{"Action":"fail","Package":"example.com/tests","Elapsed":0.1}
`,
			expectedOutput: `fatal package error
=== RUN   TestIncomplete
=== PAUSE TestIncomplete

DONE 0 tests, 1 failure in 0.100s
`,
			expectedStdout: `fatal package error

DONE 0 tests, 1 failure in 0.100s
`,
		},
		{
			name: "shows incomplete test alerts on stdout",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"=== RUN   TestIncomplete\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"panic: setup failed\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"goroutine 1 [running]:\n"}
{"Action":"fail","Package":"example.com/tests","Elapsed":0.1}
`,
			expectedOutput: `=== RUN   TestIncomplete
panic: setup failed
goroutine 1 [running]:

DONE 0 tests, 1 failure in 0.100s
`,
			expectedStdout: `=== RUN   TestIncomplete
panic: setup failed
goroutine 1 [running]:

DONE 0 tests, 1 failure in 0.100s
`,
		},
		{
			name: "hides incomplete test diagnostics from stdout",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"=== RUN   TestIncomplete\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"    test.go:10: setup failed\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"=== PAUSE TestIncomplete\n"}
{"Action":"pause","Package":"example.com/tests","Test":"TestIncomplete"}
{"Action":"output","Package":"example.com/tests","Output":"fatal package error\n"}
{"Action":"fail","Package":"example.com/tests","Elapsed":0.1}
`,
			expectedOutput: `fatal package error
=== RUN   TestIncomplete
    test.go:10: setup failed
=== PAUSE TestIncomplete

DONE 0 tests, 1 failure in 0.100s
`,
			expectedStdout: `fatal package error

DONE 0 tests, 1 failure in 0.100s
`,
		},
		{
			name: "hides framing-only failed ancestors from stdout",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite/TestParent/child","Output":"=== RUN   TestSuite/TestParent/child\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite/TestParent/child","Output":"    child_test.go:10: boom\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite/TestParent/child","Output":"--- FAIL: TestSuite/TestParent/child (0.01s)\n"}
{"Action":"fail","Package":"example.com/tests","Test":"TestSuite/TestParent/child"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite/TestParent","Output":"=== RUN   TestSuite/TestParent\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite/TestParent","Output":"=== PAUSE TestSuite/TestParent\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite/TestParent","Output":"=== CONT  TestSuite/TestParent\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite/TestParent","Output":"--- FAIL: TestSuite/TestParent (0.00s)\n"}
{"Action":"fail","Package":"example.com/tests","Test":"TestSuite/TestParent"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite","Output":"=== RUN   TestSuite\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite","Output":"=== PAUSE TestSuite\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite","Output":"=== CONT  TestSuite\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestSuite","Output":"--- FAIL: TestSuite (0.00s)\n"}
{"Action":"fail","Package":"example.com/tests","Test":"TestSuite"}
{"Action":"fail","Package":"example.com/tests","Elapsed":0.01}
`,
			expectedOutput: `=== RUN   TestSuite/TestParent/child
    child_test.go:10: boom
--- FAIL: TestSuite/TestParent/child (0.01s)
=== RUN   TestSuite/TestParent
=== PAUSE TestSuite/TestParent
=== CONT  TestSuite/TestParent
--- FAIL: TestSuite/TestParent (0.00s)
=== RUN   TestSuite
=== PAUSE TestSuite
=== CONT  TestSuite
--- FAIL: TestSuite (0.00s)

DONE 3 tests, 3 failures in 0.010s
`,
			expectedStdout: `=== RUN   TestSuite/TestParent/child
    child_test.go:10: boom
--- FAIL: TestSuite/TestParent/child (0.01s)

DONE 3 tests, 3 failures in 0.010s
`,
		},
		{
			name: "shows failed ancestor diagnostics on stdout",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent/child","Output":"--- FAIL: TestParent/child (0.01s)\n"}
{"Action":"fail","Package":"example.com/tests","Test":"TestParent/child"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent","Output":"=== RUN   TestParent\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent","Output":"    parent_test.go:20: cleanup failed\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent","Output":"--- FAIL: TestParent (0.01s)\n"}
{"Action":"fail","Package":"example.com/tests","Test":"TestParent"}
{"Action":"fail","Package":"example.com/tests","Elapsed":0.01}
`,
			expectedOutput: `--- FAIL: TestParent/child (0.01s)
=== RUN   TestParent
    parent_test.go:20: cleanup failed
--- FAIL: TestParent (0.01s)

DONE 2 tests, 2 failures in 0.010s
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
			require.Equal(t, tc.expectedOutput, output.finish())
			expectedStdout := tc.expectedStdout
			if expectedStdout == "" {
				expectedStdout = tc.expectedOutput
			}
			require.Equal(t, expectedStdout, stdout.String())
		})
	}
}

func TestGoTestJSONOutput_RetainsHiddenOutputForAlertParsing(t *testing.T) {
	input := `{"Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestPass"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"==================\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"WARNING: DATA RACE\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"test.TestPass()\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"==================\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"--- PASS: TestPass (0.00s)\n"}
{"Action":"pass","Package":"example.com/tests","Test":"TestPass"}
{"Action":"fail","Package":"example.com/tests","Elapsed":0.1}
`

	output := newGoTestJSONOutput()
	var stdout bytes.Buffer
	output.stdout = &stdout
	_, err := output.Write([]byte(input))
	require.NoError(t, err)

	alerts := parseAlerts(output.finish())
	require.Len(t, alerts, 1)
	require.Equal(t, failureTypeDataRace, alerts[0].Type)
	require.NotContains(t, stdout.String(), "WARNING: DATA RACE")
}

func TestGoTestJSONOutput_ChunkedWrites(t *testing.T) {
	output := newGoTestJSONOutput()
	var stdout bytes.Buffer
	output.stdout = &stdout

	_, err := output.Write([]byte(`{"Action":"start","Package":"example.`))
	require.NoError(t, err)
	_, err = output.Write([]byte(`com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestFail"}
{"Action":"output","Package":"example.com/tests","Test":"TestFail","Output":"chunked\n"}
{"Action":"fail","Package":"example.com/tests","Test":"TestFail"}
{"Action":"fail","Package":"example.com/tests","Elapsed":0.1}`))
	require.NoError(t, err)

	expected := `chunked

DONE 1 tests, 1 failure in 0.100s
`
	require.Equal(t, expected, output.finish())
	require.Equal(t, expected, stdout.String())
}

func TestGoTestJSONOutput_MultiplePackages(t *testing.T) {
	input := `{"Time":"2026-07-28T00:00:00Z","Action":"start","Package":"example.com/one"}
{"Time":"2026-07-28T00:00:00.5Z","Action":"start","Package":"example.com/two"}
{"Action":"run","Package":"example.com/one","Test":"TestOne"}
{"Action":"output","Package":"example.com/one","Test":"TestOne","Output":"=== RUN   TestOne\n"}
{"Action":"output","Package":"example.com/one","Test":"TestOne","Output":"    one_test.go:1: one-1\n"}
{"Action":"run","Package":"example.com/two","Test":"TestTwo"}
{"Action":"output","Package":"example.com/two","Test":"TestTwo","Output":"=== RUN   TestTwo\n"}
{"Action":"output","Package":"example.com/two","Test":"TestTwo","Output":"    two_test.go:1: two-1\n"}
{"Action":"output","Package":"example.com/one","Test":"TestOne","Output":"    one_test.go:2: one-2\n"}
{"Action":"output","Package":"example.com/two","Test":"TestTwo","Output":"--- FAIL: TestTwo (1.00s)\n"}
{"Time":"2026-07-28T00:00:01.5Z","Action":"fail","Package":"example.com/two","Test":"TestTwo"}
{"Time":"2026-07-28T00:00:01.75Z","Action":"fail","Package":"example.com/two","Elapsed":1.25}
{"Action":"output","Package":"example.com/one","Test":"TestOne","Output":"--- FAIL: TestOne (2.00s)\n"}
{"Time":"2026-07-28T00:00:02Z","Action":"fail","Package":"example.com/one","Test":"TestOne"}
{"Time":"2026-07-28T00:00:03Z","Action":"fail","Package":"example.com/one","Elapsed":3}
`
	expected := `=== RUN   TestTwo
    two_test.go:1: two-1
--- FAIL: TestTwo (1.00s)
=== RUN   TestOne
    one_test.go:1: one-1
    one_test.go:2: one-2
--- FAIL: TestOne (2.00s)

DONE 2 tests, 2 failures in 3.000s
`

	output := newGoTestJSONOutput()
	output.stdout = &bytes.Buffer{}
	_, err := output.Write([]byte(input))
	require.NoError(t, err)
	require.Equal(t, expected, output.finish())
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
			require.Equal(t, tc.expected, output.finish())
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
										Data: "package example.com/tests aborted; 1 test node had no final result, and others may not have started\n\n" +
											"Tests without final results:\n" +
											"- TestIncomplete",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "hard-killed package",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestIncomplete"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"=== RUN   TestIncomplete\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"    context.go:132:\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"        Error Trace:\tcontext.go:132\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"        Error:      \ttest exceeded timeout\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"        Test:       \tTestIncomplete\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"        Messages:   \ttimeout: 1m30s\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"=== PAUSE TestIncomplete\n"}
{"Action":"pause","Package":"example.com/tests","Test":"TestIncomplete"}
`,
			expectedOutput: `=== RUN   TestIncomplete
    context.go:132:
        Error Trace:	context.go:132
        Error:      	test exceeded timeout
        Test:       	TestIncomplete
        Messages:   	timeout: 1m30s
=== PAUSE TestIncomplete

DONE 0 tests in 0.000s
`,
			expectedReport: &junitReport{
				Testsuites: junit.Testsuites{
					Tests:    1,
					Failures: 1,
					Suites: []junit.Testsuite{
						{
							Name: "example.com/tests",
							ID:   0,
							Time: "0.000",
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
										Data: "package example.com/tests aborted; 1 test node had no final result, and others may not have started\n\n" +
											"Tests without final results:\n" +
											"- TestIncomplete\n" +
											"  Details:\n" +
											"        context.go:132:\n" +
											"            Error Trace:\tcontext.go:132\n" +
											"            Error:      \ttest exceeded timeout\n" +
											"            Test:       \tTestIncomplete\n" +
											"            Messages:   \ttimeout: 1m30s",
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
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestIncomplete"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"=== RUN   TestIncomplete\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"incomplete\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"=== PAUSE TestIncomplete\n"}
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
=== RUN   TestIncomplete
incomplete
=== PAUSE TestIncomplete

DONE 1 tests, 2 failures, 1 error in 0.010s
`,
			expectedReport: &junitReport{
				Testsuites: junit.Testsuites{
					Tests:    3,
					Errors:   1,
					Failures: 2,
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
							Name:     "example.com/tests",
							Tests:    1,
							Failures: 1,
							ID:       1,
							Time:     "0.010",
							Testcases: []junit.Testcase{
								{
									Name:      "TestFail",
									Classname: "example.com/tests",
									Time:      "0.010",
									Failure: &junit.Result{
										Message: "Failed",
										Data: "    test.go:1: failed\n" +
											"--- FAIL: TestFail (0.01s)",
									},
								},
							},
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
										Data: "package example.com/tests aborted; 1 test node had no final result, and others may not have started\n\n" +
											"Tests without final results:\n" +
											"- TestIncomplete",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "buffered parent failure details",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestParent"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent","Output":"=== RUN   TestParent\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent","Output":"=== PAUSE TestParent\n"}
{"Action":"pause","Package":"example.com/tests","Test":"TestParent"}
{"Action":"run","Package":"example.com/tests","Test":"TestParent/Child"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent/Child","Output":"=== RUN   TestParent/Child\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent/Child","Output":"--- PASS: TestParent/Child (0.01s)\n"}
{"Action":"pass","Package":"example.com/tests","Test":"TestParent/Child","Elapsed":0.01}
{"Action":"cont","Package":"example.com/tests","Test":"TestParent"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent","Output":"=== CONT  TestParent\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent","Output":"    test_env.go:647: Running TestParent in test shard 1/3\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent","Output":"    context.go:130: test exceeded timeout of 1m30s\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent","Output":"--- FAIL: TestParent (0.01s)\n"}
{"Action":"fail","Package":"example.com/tests","Test":"TestParent","Elapsed":0.01}
{"Action":"output","Package":"example.com/tests","Output":"FAIL\texample.com/tests\t0.010s\n"}
{"Action":"fail","Package":"example.com/tests","Elapsed":0.01}
`,
			expectedOutput: `=== RUN   TestParent/Child
--- PASS: TestParent/Child (0.01s)
=== RUN   TestParent
=== PAUSE TestParent
=== CONT  TestParent
    test_env.go:647: Running TestParent in test shard 1/3
    context.go:130: test exceeded timeout of 1m30s
--- FAIL: TestParent (0.01s)
FAIL	example.com/tests	0.010s

DONE 2 tests, 1 failure in 0.010s
`,
			expectedReport: &junitReport{
				Testsuites: junit.Testsuites{
					Tests:    2,
					Failures: 1,
					Suites: []junit.Testsuite{
						{
							Name:     "example.com/tests",
							Tests:    2,
							Failures: 1,
							ID:       0,
							Time:     "0.010",
							Testcases: []junit.Testcase{
								{
									Name:      "TestParent",
									Classname: "example.com/tests",
									Time:      "0.010",
									Failure: &junit.Result{
										Message: "Failed",
										Data: "    context.go:130: test exceeded timeout of 1m30s\n" +
											"--- FAIL: TestParent (0.01s)",
									},
								},
								{
									Name:      "TestParent/Child",
									Classname: "example.com/tests",
									Time:      "0.010",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "unknown parent with completed child",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestParent"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent","Output":"=== RUN   TestParent\n"}
{"Action":"run","Package":"example.com/tests","Test":"TestParent/Child"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent/Child","Output":"=== RUN   TestParent/Child\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent/Child","Output":"--- PASS: TestParent/Child (0.01s)\n"}
{"Action":"pass","Package":"example.com/tests","Test":"TestParent/Child","Elapsed":0.01}
{"Action":"output","Package":"example.com/tests","Output":"ok  \texample.com/tests\t0.010s\n"}
{"Action":"pass","Package":"example.com/tests","Elapsed":0.01}
`,
			expectedOutput: `=== RUN   TestParent/Child
--- PASS: TestParent/Child (0.01s)
ok  	example.com/tests	0.010s
=== RUN   TestParent

DONE 1 tests in 0.010s
`,
			expectedReport: &junitReport{
				Testsuites: junit.Testsuites{
					Tests: 1,
					Suites: []junit.Testsuite{
						{
							Name:  "example.com/tests",
							Tests: 1,
							ID:    0,
							Time:  "0.010",
							Testcases: []junit.Testcase{
								{
									Name:      "TestParent/Child",
									Classname: "example.com/tests",
									Time:      "0.010",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "unknown parent with abort alert",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestParent"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent","Output":"=== RUN   TestParent\n"}
{"Action":"run","Package":"example.com/tests","Test":"TestParent/Child"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent/Child","Output":"=== RUN   TestParent/Child\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestParent/Child","Output":"--- PASS: TestParent/Child (0.01s)\n"}
{"Action":"pass","Package":"example.com/tests","Test":"TestParent/Child","Elapsed":0.01}
{"Action":"output","Package":"example.com/tests","Test":"TestParent","Output":"panic: setup failed\n"}
`,
			expectedOutput: `=== RUN   TestParent/Child
--- PASS: TestParent/Child (0.01s)
=== RUN   TestParent
panic: setup failed

DONE 1 tests in 0.000s
`,
			expectedReport: &junitReport{
				Testsuites: junit.Testsuites{
					Tests:    2,
					Failures: 1,
					Suites: []junit.Testsuite{
						{
							Name:  "example.com/tests",
							Tests: 1,
							ID:    0,
							Time:  "0.010",
							Testcases: []junit.Testcase{
								{
									Name:      "TestParent/Child",
									Classname: "example.com/tests",
									Time:      "0.010",
								},
							},
							SystemOut: &junit.Output{Data: "panic: setup failed"},
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
										Data: "package example.com/tests aborted; 1 test node had no final result, and others may not have started\n\n" +
											"Tests without final results:\n" +
											"- TestParent",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "terminal actions resolve parser-unknown parents",
			input: `{"Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestPassParent"}
{"Action":"output","Package":"example.com/tests","Test":"TestPassParent","Output":"=== RUN   TestPassParent\n"}
{"Action":"run","Package":"example.com/tests","Test":"TestPassParent/Child"}
{"Action":"output","Package":"example.com/tests","Test":"TestPassParent/Child","Output":"=== RUN   TestPassParent/Child\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestPassParent/Child","Output":"--- PASS: TestPassParent/Child (0.01s)\n"}
{"Action":"pass","Package":"example.com/tests","Test":"TestPassParent/Child","Elapsed":0.01}
{"Action":"pass","Package":"example.com/tests","Test":"TestPassParent","Elapsed":0.01}
{"Action":"run","Package":"example.com/tests","Test":"TestFailParent"}
{"Action":"output","Package":"example.com/tests","Test":"TestFailParent","Output":"=== RUN   TestFailParent\n"}
{"Action":"run","Package":"example.com/tests","Test":"TestFailParent/Child"}
{"Action":"output","Package":"example.com/tests","Test":"TestFailParent/Child","Output":"=== RUN   TestFailParent/Child\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestFailParent/Child","Output":"--- PASS: TestFailParent/Child (0.01s)\n"}
{"Action":"pass","Package":"example.com/tests","Test":"TestFailParent/Child","Elapsed":0.01}
{"Action":"output","Package":"example.com/tests","Test":"TestFailParent","Output":"    parent_test.go:20: cleanup failed\n"}
{"Action":"fail","Package":"example.com/tests","Test":"TestFailParent","Elapsed":0.01}
{"Action":"output","Package":"example.com/tests","Output":"FAIL\texample.com/tests\t0.010s\n"}
{"Action":"fail","Package":"example.com/tests","Elapsed":0.01}
`,
			expectedOutput: `=== RUN   TestPassParent/Child
--- PASS: TestPassParent/Child (0.01s)
=== RUN   TestPassParent
=== RUN   TestFailParent/Child
--- PASS: TestFailParent/Child (0.01s)
=== RUN   TestFailParent
    parent_test.go:20: cleanup failed
FAIL	example.com/tests	0.010s

DONE 4 tests, 1 failure in 0.010s
`,
			expectedReport: &junitReport{
				Testsuites: junit.Testsuites{
					Tests:    4,
					Failures: 1,
					Suites: []junit.Testsuite{
						{
							Name:     "example.com/tests",
							Tests:    4,
							Failures: 1,
							ID:       0,
							Time:     "0.010",
							Testcases: []junit.Testcase{
								{
									Name:      "TestPassParent",
									Classname: "example.com/tests",
									Time:      "0.000",
								},
								{
									Name:      "TestPassParent/Child",
									Classname: "example.com/tests",
									Time:      "0.010",
								},
								{
									Name:      "TestFailParent",
									Classname: "example.com/tests",
									Time:      "0.000",
									Failure: &junit.Result{
										Message: "Failed",
										Data:    "    parent_test.go:20: cleanup failed",
									},
								},
								{
									Name:      "TestFailParent/Child",
									Classname: "example.com/tests",
									Time:      "0.010",
								},
							},
							SystemOut: &junit.Output{Data: "    parent_test.go:20: cleanup failed"},
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
			require.Equal(t, tc.expectedOutput, output.finish())

			report, err := output.junitReport()
			require.NoError(t, err)
			for i := range report.Suites {
				report.Suites[i].Timestamp = ""
			}
			require.Equal(t, tc.expectedReport, report)
		})
	}
}

func TestPackageAbortLogSummary(t *testing.T) {
	testCases := []struct {
		name     string
		details  string
		expected string
	}{
		{
			name: "fatal",
			details: "package example.com/tests aborted; 707 test nodes had no final result, and others may not have started\n\n" +
				"Tests without final results:\n" +
				"- TestOne\n" +
				"- TestTwo\n" +
				"  Details:\n" +
				"    2026-07-28T20:12:54.860Z\tfatal\tloadSchemaVersion\t{\"error\":\"cassandra unavailable\"}",
			expected: "likely cause: 2026-07-28T20:12:54.860Z\tfatal\tloadSchemaVersion\t{\"error\":\"cassandra unavailable\"}\n" +
				"package example.com/tests aborted; 707 test nodes had no final result, and others may not have started",
		},
		{
			name: "panic",
			details: "package example.com/tests aborted; 1 test node had no final result, and others may not have started\n\n" +
				"Tests without final results:\n" +
				"- TestOne\n" +
				"  Details:\n" +
				"    panic: setup failed",
			expected: "likely cause: panic: setup failed\n" +
				"package example.com/tests aborted; 1 test node had no final result, and others may not have started",
		},
		{
			name: "no cause",
			details: "package example.com/tests aborted; 2 test nodes had no final result, and others may not have started\n\n" +
				"Tests without final results:\n" +
				"- TestOne\n" +
				"- TestTwo",
			expected: "package example.com/tests aborted; 2 test nodes had no final result, and others may not have started",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, packageAbortLogSummary(tc.details))
		})
	}
}
