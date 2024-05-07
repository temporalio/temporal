// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"flag"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"log"
	"os"
	"strings"
	"text/template"
)

type (
	settingType struct {
		Name   string
		GoType string
		Index  int
	}
	settingPrecedence struct {
		Name       string
		GoArgs     string
		GoArgNames string
		Index      int
	}
)

var (
	types = []*settingType{
		{
			Name:   "Bool",
			GoType: "bool",
		},
		{
			Name:   "Int",
			GoType: "int",
		},
		{
			Name:   "Float",
			GoType: "float64",
		},
		{
			Name:   "String",
			GoType: "string",
		},
		{
			Name:   "Duration",
			GoType: "time.Duration",
		},
		{
			Name:   "Map",
			GoType: "map[string]any",
		},
	}
	precedences = []*settingPrecedence{
		{
			Name:   "Global",
			GoArgs: "",
		},
		{
			Name:   "Namespace",
			GoArgs: "namespace string",
		},
		{
			Name:   "NamespaceID",
			GoArgs: "namespaceID string",
		},
		{
			Name:   "TaskQueue",
			GoArgs: "namespace string, taskQueue string, taskQueueType enumspb.TaskQueueType",
		},
		{
			Name:   "ShardID",
			GoArgs: "shardID int32",
		},
		{
			Name:   "TaskType",
			GoArgs: "taskType enumsspb.TaskType",
		},
		{
			Name:   "Destination",
			GoArgs: "namespaceID string, destination string",
		},
	}
)

func fatalIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func writeTemplatedCode(w io.Writer, text string, data any) {
	fatalIfErr(template.Must(template.New("code").Parse(text)).Execute(w, data))
}

func generateTypeEnum(w io.Writer, tp *settingType) {
	writeTemplatedCode(w, `
const Type{{.Name}} Type = {{.Index}} // go type: {{.GoType}}
`, tp)
}

func generatePrecEnum(w io.Writer, prec *settingPrecedence) {
	writeTemplatedCode(w, `
const Precedence{{.Name}} Precedence = {{.Index}}
`, prec)
}

func generateType(w io.Writer, tp *settingType, prec *settingPrecedence) {
	writeTemplatedCode(w, `
type {{.P.Name}}{{.T.Name}}Setting setting[{{.T.GoType}}, func({{.P.GoArgs}})]

func New{{.P.Name}}{{.T.Name}}Setting(key Key, def {{.T.GoType}}, description string) {{.P.Name}}{{.T.Name}}Setting {
	s := {{.P.Name}}{{.T.Name}}Setting{
		key:         key,
		def:         def,
		description: description,
	}
	return s
}

func New{{.P.Name}}{{.T.Name}}SettingWithConstrainedDefault(key Key, cdef []TypedConstrainedValue[{{.T.GoType}}], description string) {{.P.Name}}{{.T.Name}}Setting {
	s := {{.P.Name}}{{.T.Name}}Setting{
		key:         key,
		cdef:        cdef,
		description: description,
	}
	return s
}

func (s {{.P.Name}}{{.T.Name}}Setting) Key() Key               { return s.key }
func (s {{.P.Name}}{{.T.Name}}Setting) Type() Type             { return Type{{.T.Name}} }
func (s {{.P.Name}}{{.T.Name}}Setting) Precedence() Precedence { return Precedence{{.P.Name}} }

func (s {{.P.Name}}{{.T.Name}}Setting) WithDefault(v {{.T.GoType}}) {{.P.Name}}{{.T.Name}}Setting {
	newS := s
	newS.def = v
	return newS
}

{{if eq .P.Name "Global" -}}
type {{.T.Name}}PropertyFn func({{.P.GoArgs}}) {{.T.GoType}}
{{- else -}}
type {{.T.Name}}PropertyFnWith{{.P.Name}}Filter func({{.P.GoArgs}}) {{.T.GoType}}
{{- end}}

{{if eq .P.Name "Global" -}}
func (s {{.P.Name}}{{.T.Name}}Setting) Get(c *Collection) {{.T.Name}}PropertyFn {
{{- else -}}
func (s {{.P.Name}}{{.T.Name}}Setting) Get(c *Collection) {{.T.Name}}PropertyFnWith{{.P.Name}}Filter {
{{- end}}
	return func({{.P.GoArgs}}) {{.T.GoType}} {
		return matchAndConvert(
			c,
			(setting[{{.T.GoType}}, func({{.P.GoArgs}})])(s),
			precedence{{.P.Name}}({{.P.GoArgNames}}),
			convert{{.T.Name}},
		)
	}
}

{{if eq .P.Name "Global" -}}
func Get{{.T.Name}}PropertyFn(value {{.T.GoType}}) {{.T.Name}}PropertyFn {
{{- else -}}
func Get{{.T.Name}}PropertyFnFilteredBy{{.P.Name}}(value {{.T.GoType}}) {{.T.Name}}PropertyFnWith{{.P.Name}}Filter {
{{- end}}
	return func({{.P.GoArgs}}) {{.T.GoType}} {
		return value
	}
}
`, map[string]any{"T": tp, "P": prec})
}

func generate(w io.Writer) {
	writeTemplatedCode(w, `
package dynamicconfig

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
)
`, nil)
	for idx, tp := range types {
		// fill in Index
		tp.Index = idx
		generateTypeEnum(w, tp)
	}
	for idx, prec := range precedences {
		// fill in Index and GoArgNames
		prec.Index = idx
		var argNames []string
		for _, argAndType := range strings.Split(prec.GoArgs, ",") {
			argNames = append(argNames, strings.Split(strings.TrimSpace(argAndType), " ")[0])
		}
		prec.GoArgNames = strings.Join(argNames, ", ")
		generatePrecEnum(w, prec)
	}
	for _, tp := range types {
		for _, prec := range precedences {
			generateType(w, tp, prec)
		}
	}
}

func checkParses(filename string) {
	_, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.SkipObjectResolution)
	fatalIfErr(err)
}

func callWithFile(f func(io.Writer), filename string, licenseText string) {
	w, err := os.Create(filename + "_gen.go")
	if err != nil {
		panic(err)
	}
	defer func() {
		fatalIfErr(w.Close())
		checkParses(w.Name())
	}()
	if _, err := fmt.Fprintf(w, "%s\n// Code generated by cmd/tools/gendynamicconfig. DO NOT EDIT.\n", licenseText); err != nil {
		panic(err)
	}
	f(w)
}

func readLicenseFile(path string) string {
	text, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	var lines []string
	for _, line := range strings.Split(string(text), "\n") {
		lines = append(lines, strings.TrimRight("// "+line, " "))
	}
	return strings.Join(lines, "\n") + "\n"
}

func main() {
	licenseFlag := flag.String("licence_file", "../../LICENSE", "path to license to copy into header")
	flag.Parse()

	licenseText := readLicenseFile(*licenseFlag)

	callWithFile(generate, "setting", licenseText)
}
