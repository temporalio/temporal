package testrunner

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/jstemmer/go-junit-report/v2/gtr"
	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/jstemmer/go-junit-report/v2/parser/gotest"
)

type goTestEvent struct {
	Output string `json:"Output"`
}

type goTestJSONOutput struct {
	line   strings.Builder
	input  strings.Builder
	output strings.Builder
	stdout io.Writer
}

func newGoTestJSONOutput() *goTestJSONOutput {
	return &goTestJSONOutput{
		stdout: os.Stdout,
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
	if err := o.decodeLine(line, &event); err != nil {
		_, _ = fmt.Fprintln(o.stdout, line)
		o.output.WriteString(line)
		o.output.WriteByte('\n')
		return
	}
	o.input.WriteString(line)
	o.input.WriteByte('\n')
	if event.Output != "" {
		_, _ = fmt.Fprint(o.stdout, event.Output)
		o.output.WriteString(event.Output)
	}
}

func (o *goTestJSONOutput) decodeLine(line string, event *goTestEvent) error {
	return json.Unmarshal([]byte(line), event)
}

func (o *goTestJSONOutput) junitReport() (*junitReport, error) {
	report, err := gotest.NewJSONParser().Parse(strings.NewReader(o.input.String()))
	if err != nil {
		return &junitReport{}, err
	}
	for i := range report.Packages {
		tests := report.Packages[i].Tests
		report.Packages[i].Tests = tests[:0]
		for _, test := range tests {
			// Incomplete tests from a runner abort have run/pause output but no terminal result.
			if test.Result != gtr.Unknown {
				report.Packages[i].Tests = append(report.Packages[i].Tests, test)
			}
		}
	}
	return &junitReport{Testsuites: junit.CreateFromReport(report, "")}, nil
}
