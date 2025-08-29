package printer_test

import (
	"bytes"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/tdbg/printer"
)

// TODO(cretz): Test:
// * Text printer specific fields
// * Text printer specific and non-specific fields and all sorts of table options
// * JSON printer

func TestPrinter_Text(t *testing.T) {
	type MyStruct struct {
		Foo              string
		Bar              bool
		unexportedBaz    string
		ReallyLongField  any
		Omitted          string `cli:",omit"`
		OmittedCardEmpty string `cli:",cardOmitEmpty"`
	}
	var buf bytes.Buffer
	p := printer.Printer{Output: &buf}
	// Simple struct non-table no fields set
	require.NoError(t, p.PrintStructured([]*MyStruct{
		{
			Foo:           "1",
			unexportedBaz: "2",
			ReallyLongField: struct {
				Key any `json:"key"`
			}{Key: 123},
			Omitted:          "value",
			OmittedCardEmpty: "value",
		},
		{
			Foo:             "not-a-number",
			Bar:             true,
			ReallyLongField: map[string]int{"": 0},
		},
	}, printer.StructuredOptions{}))
	// Check
	require.Equal(t, normalizeMultiline(`
  Foo               1
  Bar               false
  ReallyLongField   {"key":123}
  OmittedCardEmpty  value

  Foo              not-a-number
  Bar              true
  ReallyLongField  map[:0]`), normalizeMultiline(buf.String()))

	// TODO(cretz): Tables and more options
}

func normalizeMultiline(s string) string {
	// Split lines, trim trailing space on each (also removes \r), remove empty
	// lines, re-join
	var ret string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimRightFunc(line, unicode.IsSpace)
		// Only non-empty lines
		if line != "" {
			if ret != "" {
				ret += "\n"
			}
			ret += line
		}
	}
	return ret
}

func TestPrinter_JSON(t *testing.T) {
	var buf bytes.Buffer

	// With indentation
	p := printer.Printer{Output: &buf, JSON: true, JSONIndent: "  "}
	p.Println("should not print")
	require.NoError(t, p.PrintStructured(map[string]string{"foo": "bar"}, printer.StructuredOptions{}))
	require.Equal(t, `{
  "foo": "bar"
}
`, buf.String())

	// Without indentation
	buf.Reset()
	p = printer.Printer{Output: &buf, JSON: true}
	p.Println("should not print")
	require.NoError(t, p.PrintStructured(map[string]string{"foo": "bar"}, printer.StructuredOptions{}))
	require.Equal(t, "{\"foo\":\"bar\"}\n", buf.String())
}

func TestPrinter_JSONList(t *testing.T) {
	var buf bytes.Buffer

	// With indentation
	p := printer.Printer{Output: &buf, JSON: true, JSONIndent: "  "}
	p.StartList()
	p.Println("should not print")
	require.NoError(t, p.PrintStructured(map[string]string{"foo": "bar"}, printer.StructuredOptions{}))
	require.NoError(t, p.PrintStructured(map[string]string{"baz": "qux"}, printer.StructuredOptions{}))
	p.EndList()
	require.Equal(t, `[
{
  "foo": "bar"
},
{
  "baz": "qux"
}
]
`, buf.String())

	// Without indentation
	buf.Reset()
	p = printer.Printer{Output: &buf, JSON: true}
	p.StartList()
	p.Println("should not print")
	require.NoError(t, p.PrintStructured(map[string]string{"foo": "bar"}, printer.StructuredOptions{}))
	require.NoError(t, p.PrintStructured(map[string]string{"baz": "qux"}, printer.StructuredOptions{}))
	p.EndList()
	require.Equal(t, "{\"foo\":\"bar\"}\n{\"baz\":\"qux\"}\n", buf.String())

	// Empty with indentation
	buf.Reset()
	p = printer.Printer{Output: &buf, JSON: true, JSONIndent: "  "}
	p.StartList()
	p.Println("should not print")
	p.EndList()
	require.Equal(t, "[\n]\n", buf.String())

	// Empty without indentation
	buf.Reset()
	p = printer.Printer{Output: &buf, JSON: true}
	p.StartList()
	p.Println("should not print")
	p.EndList()
	require.Equal(t, "", buf.String())
}

// Asserts the printer package don't panic if the CLI is run without a STDOUT.
// This is a tricky thing to validate, as it must be done in a subprocess and as
// `go test` has its own internal fix for improper STDOUT. This was fixed in
// Go 1.22, but keeping this here as a regression test.
// See https://github.com/temporalio/cli/issues/544.
func TestPrinter_NoPanicIfNoStdout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipped on Windows")
		return
	}

	goPath, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("Error finding go executable: %v", err)
	}
	// Don't use exec.Command here, as it silently replaces nil file descriptors
	// with /dev/null on the parent side. We specifically want to test what
	// happens when stdout is nil.
	p, err := os.StartProcess(
		goPath,
		[]string{"go", "run", "./test/main.go"},
		&os.ProcAttr{
			Files: []*os.File{os.Stdin, nil, os.Stderr},
		},
	)
	if err != nil {
		t.Fatalf("Error running command: %v", err)
	}
	state, err := p.Wait()
	if err != nil {
		t.Fatalf("Error running command: %v", err)
	}
	if state.ExitCode() != 0 {
		t.Fatalf("Error running command; exit code = %d", state.ExitCode())
	}
}
