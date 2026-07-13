package testrunner

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/jstemmer/go-junit-report/v2/junit"
)

type goTestEvent struct {
	Action  string  `json:"Action"`
	Package string  `json:"Package"`
	Test    string  `json:"Test"`
	Output  string  `json:"Output"`
	Elapsed float64 `json:"Elapsed"`
}

type goTestJSONOutput struct {
	line     strings.Builder
	output   strings.Builder
	stdout   io.Writer
	packages map[string]*goTestPackage
	order    []string
}

type goTestPackage struct {
	output  strings.Builder
	tests   map[string]*goTestCase
	order   []string
	failed  bool
	elapsed float64
}

type goTestCase struct {
	output  strings.Builder
	action  string
	elapsed float64
}

func newGoTestJSONOutput() *goTestJSONOutput {
	return &goTestJSONOutput{
		stdout:   os.Stdout,
		packages: make(map[string]*goTestPackage),
	}
}

func (o *goTestJSONOutput) Write(p []byte) (int, error) {
	for _, b := range p {
		o.line.WriteByte(b)
		if b == '\n' {
			o.writeLine(strings.TrimSuffix(o.line.String(), "\n"))
			o.line.Reset()
		}
	}
	return len(p), nil
}

func (o *goTestJSONOutput) String() string {
	if o.line.Len() > 0 {
		o.writeLine(o.line.String())
		o.line.Reset()
	}
	return o.output.String()
}

func (o *goTestJSONOutput) writeLine(line string) {
	var event goTestEvent
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		fmt.Fprintln(o.stdout, line)
		o.output.WriteString(line)
		o.output.WriteByte('\n')
		return
	}
	o.record(event)
	if event.Output != "" {
		fmt.Fprint(o.stdout, event.Output)
		o.output.WriteString(event.Output)
	}
}

func (o *goTestJSONOutput) record(event goTestEvent) {
	pkg := o.packages[event.Package]
	if pkg == nil {
		pkg = &goTestPackage{tests: make(map[string]*goTestCase)}
		o.packages[event.Package] = pkg
		o.order = append(o.order, event.Package)
	}
	if event.Output != "" {
		pkg.output.WriteString(event.Output)
	}
	if event.Test != "" {
		test := pkg.tests[event.Test]
		if test == nil {
			test = &goTestCase{}
			pkg.tests[event.Test] = test
			pkg.order = append(pkg.order, event.Test)
		}
		if event.Output != "" {
			test.output.WriteString(event.Output)
		}
		if event.Action != "output" {
			test.action = event.Action
			test.elapsed = event.Elapsed
		}
	}
	if event.Test == "" && event.Action == "fail" {
		pkg.failed = true
		pkg.elapsed = event.Elapsed
	}
}

func (o *goTestJSONOutput) junitReport() *junitReport {
	report := &junitReport{}
	for _, pkgName := range o.order {
		pkg := o.packages[pkgName]
		suite := junit.Testsuite{
			Name: pkgName,
			ID:   len(report.Suites),
		}
		if pkg.elapsed > 0 {
			suite.Time = fmt.Sprintf("%.6f", pkg.elapsed)
		}
		var failedTests int
		for _, testName := range pkg.order {
			test := pkg.tests[testName]
			switch test.action {
			case "pass", "fail", "skip":
			default:
				continue
			}
			tc := junit.Testcase{
				Name:      testName,
				Classname: pkgName,
			}
			if test.elapsed > 0 {
				tc.Time = fmt.Sprintf("%.6f", test.elapsed)
			}
			switch test.action {
			case "fail":
				failedTests++
				tc.Failure = generateFailure(failureTypeFailed, test.output.String())
			case "skip":
				tc.Skipped = generateFailure("Skipped", test.output.String())
			}
			suite.AddTestcase(tc)
		}
		if pkg.failed && failedTests == 0 {
			suite.AddTestcase(junit.Testcase{
				Name:      pkgName,
				Classname: pkgName,
				Failure:   generateFailure(failureTypeFailed, pkg.output.String()),
			})
		}
		if len(suite.Testcases) > 0 {
			report.AddSuite(suite)
		}
	}
	return report
}
