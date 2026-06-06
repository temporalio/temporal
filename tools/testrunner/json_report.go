package testrunner

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/jstemmer/go-junit-report/v2/gtr"
	"github.com/jstemmer/go-junit-report/v2/junit"
)

func newJUnitReportFromGoTestJSON(output string) (*junitReport, error) {
	report, err := parseGoTestJSONOutput(output)
	if err != nil {
		return nil, err
	}
	hostname, _ := os.Hostname()
	return &junitReport{
		Testsuites: junit.CreateFromReport(report, hostname),
	}, nil
}

type goTestJSONEvent struct {
	Time    time.Time `json:"Time"`
	Action  string    `json:"Action"`
	Package string    `json:"Package"`
	Test    string    `json:"Test"`
	Elapsed float64   `json:"Elapsed"`
	Output  string    `json:"Output"`
}

type goTestJSONPackage struct {
	pkg           gtr.Package
	testOrder     []string
	tests         map[string]*gtr.Test
	packageFailed bool
}

func parseGoTestJSONOutput(output string) (gtr.Report, error) {
	packages := make(map[string]*goTestJSONPackage)
	var packageOrder []string

	decoder := json.NewDecoder(strings.NewReader(output))
	for {
		var ev goTestJSONEvent
		err := decoder.Decode(&ev)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return gtr.Report{}, err
		}
		if ev.Package == "" {
			continue
		}
		state := packages[ev.Package]
		if state == nil {
			state = &goTestJSONPackage{
				pkg:   gtr.Package{Name: ev.Package},
				tests: make(map[string]*gtr.Test),
			}
			packages[ev.Package] = state
			packageOrder = append(packageOrder, ev.Package)
		}
		state.apply(ev)
	}

	report := gtr.Report{Packages: make([]gtr.Package, 0, len(packageOrder))}
	for _, name := range packageOrder {
		report.Packages = append(report.Packages, packages[name].reportPackage())
	}
	return report, nil
}

func (p *goTestJSONPackage) apply(ev goTestJSONEvent) {
	if p.pkg.Timestamp.IsZero() && !ev.Time.IsZero() {
		p.pkg.Timestamp = ev.Time
	}
	if ev.Test == "" {
		if ev.Output != "" {
			p.pkg.Output = appendOutputLines(p.pkg.Output, ev.Output)
		}
		switch ev.Action {
		case "pass", "fail", "skip":
			p.pkg.Duration = secondsDuration(ev.Elapsed)
			if ev.Action == "fail" {
				p.packageFailed = true
			}
		default:
		}
		return
	}

	test := p.test(ev.Test)
	if ev.Output != "" {
		test.Output = appendOutputLines(test.Output, ev.Output)
	}
	switch ev.Action {
	case "pass":
		test.Result = gtr.Pass
		test.Duration = secondsDuration(ev.Elapsed)
	case "fail":
		test.Result = gtr.Fail
		test.Duration = secondsDuration(ev.Elapsed)
	case "skip":
		test.Result = gtr.Skip
		test.Duration = secondsDuration(ev.Elapsed)
	default:
	}
}

func (p *goTestJSONPackage) test(name string) *gtr.Test {
	if test := p.tests[name]; test != nil {
		return test
	}
	test := gtr.NewTest(len(p.testOrder), name)
	p.tests[name] = &test
	p.testOrder = append(p.testOrder, name)
	return &test
}

func (p *goTestJSONPackage) reportPackage() gtr.Package {
	pkg := p.pkg
	leafNames := filterParentNames(p.testOrder)
	leafSet := make(map[string]struct{}, len(leafNames))
	for _, name := range leafNames {
		leafSet[name] = struct{}{}
	}
	hasFailure := false
	for _, name := range p.testOrder {
		if _, ok := leafSet[name]; !ok {
			continue
		}
		test := *p.tests[name]
		if test.Result == gtr.Fail && len(test.Output) == 0 {
			test.Output = p.ancestorOutput(name)
		}
		if test.Result == gtr.Fail {
			hasFailure = true
		}
		pkg.Tests = append(pkg.Tests, test)
	}
	if p.packageFailed && !hasFailure {
		pkg.RunError = gtr.Error{
			Name:   pkg.Name,
			Cause:  "go test failed",
			Output: pkg.Output,
		}
	}
	return pkg
}

func (p *goTestJSONPackage) ancestorOutput(name string) []string {
	var output []string
	for _, ancestor := range ancestorNames(name) {
		test := p.tests[ancestor]
		if test == nil {
			continue
		}
		output = append(output, test.Output...)
	}
	return output
}

func ancestorNames(name string) []string {
	var names []string
	for {
		idx := strings.LastIndexByte(name, '/')
		if idx < 0 {
			break
		}
		name = name[:idx]
		names = append(names, name)
	}
	slices.Reverse(names)
	return names
}

func appendOutputLines(lines []string, output string) []string {
	output = strings.TrimSuffix(output, "\n")
	if output == "" {
		return lines
	}
	return append(lines, strings.Split(output, "\n")...)
}

func secondsDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}
