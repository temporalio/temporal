package testrunner

import (
	"bytes"
	"testing"

	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/stretchr/testify/require"
)

func TestGoTestJSONOutput_BuffersTestOutput(t *testing.T) {
	input := `{"Time":"2026-07-28T00:00:00Z","Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestIncomplete"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"A1\n"}
{"Action":"run","Package":"example.com/tests","Test":"TestPass"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"B1\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"A2\n"}
{"Action":"output","Package":"example.com/tests","Test":"TestPass","Output":"B2\n"}
{"Action":"pass","Package":"example.com/tests","Test":"TestPass"}
{"Action":"output","Package":"example.com/tests","Output":"package output\n"}
{"Time":"2026-07-28T00:00:02Z","Action":"fail","Package":"example.com/tests","Elapsed":2}
`
	expected := `B1
B2
package output
A1
A2

DONE 1 tests, 1 failure in 2.000s
`

	output := newGoTestJSONOutput()
	var stdout bytes.Buffer
	output.stdout = &stdout
	_, err := output.Write([]byte(input))
	require.NoError(t, err)
	require.Equal(t, expected, output.String())
	require.Equal(t, expected, stdout.String())
}

func TestGoTestJSONOutput_AbortedPackage(t *testing.T) {
	input := `{"Time":"2026-07-28T00:00:00Z","Action":"start","Package":"example.com/tests"}
{"Action":"run","Package":"example.com/tests","Test":"TestIncomplete"}
{"Time":"2026-07-28T00:00:01Z","Action":"output","Package":"example.com/tests","Test":"TestIncomplete","Output":"unfinished\n"}
`
	expected := `unfinished

DONE 0 tests in 1.000s
`

	output := newGoTestJSONOutput()
	output.stdout = &bytes.Buffer{}
	_, err := output.Write([]byte(input))
	require.NoError(t, err)
	require.Equal(t, expected, output.String())
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
	require.Equal(t, expected, stdout.String())
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
	input := `{"Action":"run","Package":"example.com/tests","Test":"TestIncomplete"}
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
`
	expected := &junitReport{
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
	}

	output := newGoTestJSONOutput()
	output.stdout = &bytes.Buffer{}
	_, err := output.Write([]byte(input))
	require.NoError(t, err)
	report, err := output.junitReport()
	require.NoError(t, err)

	for i := range report.Suites {
		report.Suites[i].Timestamp = ""
	}
	require.Equal(t, expected, report)
}
