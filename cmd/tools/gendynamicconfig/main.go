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
		Name      string
		GoType    string
		IsGeneric bool
	}
	settingPrecedence struct {
		Name   string
		GoArgs string
		Expr   string
		Index  int
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
		{
			Name:      "Typed",
			GoType:    "<generic>",
			IsGeneric: true, // this one is treated differently
		},
	}
	precedences = []*settingPrecedence{
		{
			Name:   "Global",
			GoArgs: "",
			Expr:   "[]Constraints{{}}",
		},
		{
			Name:   "Namespace",
			GoArgs: "namespace string",
			Expr:   "[]Constraints{{Namespace: namespace}, {}}",
		},
		{
			Name:   "NamespaceID",
			GoArgs: "namespaceID string",
			Expr:   "[]Constraints{{NamespaceID: namespaceID}, {}}",
		},
		{
			Name:   "TaskQueue",
			GoArgs: "namespace string, taskQueue string, taskQueueType enumspb.TaskQueueType",
			// A task-queue-name-only filter applies to a single task queue name across all
			// namespaces, with higher precedence than a namespace-only filter. This is intended to
			// be used by the default partition count and is probably not useful otherwise.
			Expr: `[]Constraints{
			{Namespace: namespace, TaskQueueName: taskQueue, TaskQueueType: taskQueueType},
			{Namespace: namespace, TaskQueueName: taskQueue},
			{TaskQueueName: taskQueue},
			{Namespace: namespace},
			{},
		}`,
		},
		{
			Name:   "ShardID",
			GoArgs: "shardID int32",
			Expr:   "[]Constraints{{ShardID: shardID}, {}}",
		},
		{
			Name:   "TaskType",
			GoArgs: "taskType enumsspb.TaskType",
			Expr:   "[]Constraints{{TaskType: taskType}, {}}",
		},
		{
			Name:   "Destination",
			GoArgs: "namespace string, destination string",
			Expr: `[]Constraints{
			{Namespace: namespace, Destination: destination},
			{Destination: destination},
			{Namespace: namespace},
			{},
		}`,
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

func generatePrecEnum(w io.Writer, prec *settingPrecedence) {
	writeTemplatedCode(w, `
const Precedence{{.Name}} Precedence = {{.Index}}
`, prec)
}

func generateType(w io.Writer, tp *settingType, prec *settingPrecedence) {
	writeTemplatedCode(w, `
{{ if .T.IsGeneric -}}
type {{.P.Name}}TypedSetting[T any] setting[T, func({{.P.GoArgs}})]

// New{{.P.Name}}TypedSetting creates a setting that uses mapstructure to handle complex structured
// values. The value from dynamic config will be copied over a shallow copy of 'def', which means
// 'def' must not contain any non-nil slices, maps, or pointers.
func New{{.P.Name}}TypedSetting[T any](key Key, def T, description string) {{.P.Name}}TypedSetting[T] {
	s := {{.P.Name}}TypedSetting[T]{
		key:         key,
		def:         def,
		convert:     ConvertStructure[T](def),
		description: description,
	}
	register(s)
	return s
}

// New{{.P.Name}}TypedSettingWithConverter creates a setting with a custom converter function.
func New{{.P.Name}}TypedSettingWithConverter[T any](key Key, convert func(any) (T, error), def T, description string) {{.P.Name}}TypedSetting[T] {
	s := {{.P.Name}}TypedSetting[T]{
		key:         key,
		def:         def,
		convert:     convert,
		description: description,
	}
	register(s)
	return s
}

// New{{.P.Name}}TypedSettingWithConstrainedDefault creates a setting with a compound default value.
func New{{.P.Name}}TypedSettingWithConstrainedDefault[T any](key Key, convert func(any) (T, error), cdef []TypedConstrainedValue[T], description string) {{.P.Name}}TypedSetting[T] {
	s := {{.P.Name}}TypedSetting[T]{
		key:         key,
		cdef:        &cdef,
		convert:     convert,
		description: description,
	}
	register(s)
	return s
}

func (s {{.P.Name}}TypedSetting[T]) Key() Key               { return s.key }
func (s {{.P.Name}}TypedSetting[T]) Precedence() Precedence { return Precedence{{.P.Name}} }
func (s {{.P.Name}}TypedSetting[T]) Validate(v any) error {
	_, err := s.convert(v)
	return err
}

func (s {{.P.Name}}TypedSetting[T]) WithDefault(v T) {{.P.Name}}TypedSetting[T] {
	newS := s
	newS.def = v
	{{/* The base setting should be registered so we do not register the return value here */ -}}
	return newS
}

{{if eq .P.Name "Global" -}}
type TypedPropertyFn[T any] func({{.P.GoArgs}}) T
{{- else -}}
type TypedPropertyFnWith{{.P.Name}}Filter[T any] func({{.P.GoArgs}}) T
{{- end}}

{{if eq .P.Name "Global" -}}
func (s {{.P.Name}}TypedSetting[T]) Get(c *Collection) TypedPropertyFn[T] {
{{- else -}}
func (s {{.P.Name}}TypedSetting[T]) Get(c *Collection) TypedPropertyFnWith{{.P.Name}}Filter[T] {
{{- end}}
	return func({{.P.GoArgs}}) T {
		prec := {{.P.Expr}}
		return matchAndConvert(
			c,
			s.key,
			s.def,
			s.cdef,
			s.convert,
			prec,
		)
	}
}

{{if eq .P.Name "Global" -}}
type TypedSubscribable[T any] func(callback func(T)) (v T, cancel func())
{{- else -}}
type TypedSubscribableWith{{.P.Name}}Filter[T any] func({{.P.GoArgs}}, callback func(T)) (v T, cancel func())
{{- end}}

{{if eq .P.Name "Global" -}}
func (s {{.P.Name}}TypedSetting[T]) Subscribe(c *Collection) TypedSubscribable[T] {
	return func(callback func(T)) (T, func()) {
{{- else -}}
func (s {{.P.Name}}TypedSetting[T]) Subscribe(c *Collection) TypedSubscribableWith{{.P.Name}}Filter[T] {
	return func({{.P.GoArgs}}, callback func(T)) (T, func()) {
{{- end}}
		prec := {{.P.Expr}}
		return subscribe(c, s.key, s.def, s.cdef, s.convert, prec, callback)
	}
}

func (s {{.P.Name}}TypedSetting[T]) dispatchUpdate(c *Collection, sub any, cvs []ConstrainedValue) {
	dispatchUpdate(
		c,
		s.key,
		s.convert,
		sub.(*subscription[T]),
		cvs,
	)
}

{{if eq .P.Name "Global" -}}
func GetTypedPropertyFn[T any](value T) TypedPropertyFn[T] {
{{- else -}}
func GetTypedPropertyFnFilteredBy{{.P.Name}}[T any](value T) TypedPropertyFnWith{{.P.Name}}Filter[T] {
{{- end}}
	return func({{.P.GoArgs}}) T {
		return value
	}
}
{{- else -}}
type {{.P.Name}}{{.T.Name}}Setting = {{.P.Name}}TypedSetting[{{.T.GoType}}]

func New{{.P.Name}}{{.T.Name}}Setting(key Key, def {{.T.GoType}}, description string) {{.P.Name}}{{.T.Name}}Setting {
	return New{{.P.Name}}TypedSettingWithConverter[{{.T.GoType}}](key, convert{{.T.Name}}, def, description)
}

func New{{.P.Name}}{{.T.Name}}SettingWithConstrainedDefault(key Key, cdef []TypedConstrainedValue[{{.T.GoType}}], description string) {{.P.Name}}{{.T.Name}}Setting {
	return New{{.P.Name}}TypedSettingWithConstrainedDefault[{{.T.GoType}}](key, convert{{.T.Name}}, cdef, description)
}

{{if eq .P.Name "Global" -}}
type {{.T.Name}}PropertyFn = TypedPropertyFn[{{.T.GoType}}]
{{- else -}}
type {{.T.Name}}PropertyFnWith{{.P.Name}}Filter = TypedPropertyFnWith{{.P.Name}}Filter[{{.T.GoType}}]
{{- end}}

{{if eq .P.Name "Global" -}}
func Get{{.T.Name}}PropertyFn(value {{.T.GoType}}) {{.T.Name}}PropertyFn {
	return GetTypedPropertyFn(value)
}
{{- else -}}
func Get{{.T.Name}}PropertyFnFilteredBy{{.P.Name}}(value {{.T.GoType}}) {{.T.Name}}PropertyFnWith{{.P.Name}}Filter {
	return GetTypedPropertyFnFilteredBy{{.P.Name}}(value)
}
{{- end}}
{{- end }}
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

const PrecedenceUnknown Precedence = 0
`, nil)
	for idx, prec := range precedences {
		prec.Index = idx + 1
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
