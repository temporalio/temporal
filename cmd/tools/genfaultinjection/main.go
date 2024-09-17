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
	"io"
	"log"
	"os"
	"path"
	"reflect"
	"strings"
	"text/template"

	"go.temporal.io/server/common/persistence"
)

var extraImports = map[string][]string{
	"Queue": {"go.temporal.io/api/common/v1"},
}

func main() {
	outPathFlag := flag.String("out", ".", "path to write generated files")
	licenseFlag := flag.String("copyright_file", "../../../LICENSE", "path to license to copy into header")
	flag.Parse()

	licenseText := readLicenseFile(*licenseFlag)

	for _, store := range stores() {
		callWithFile(generateFaultInjectionStore, store, *outPathFlag, licenseText)
	}
}

func stores() []reflect.Type {
	factoryT := reflect.TypeOf((*persistence.DataStoreFactory)(nil)).Elem()
	var storeTs []reflect.Type
	for i := 0; i < factoryT.NumMethod(); i++ {
		ctor := factoryT.Method(i)
		if !strings.HasPrefix(ctor.Name, "New") {
			continue
		}
		if ctor.Type.NumOut() == 0 {
			continue
		}
		storeT := ctor.Type.Out(0)
		storeTs = append(storeTs, storeT)
	}
	return storeTs
}

func callWithFile(generator func(io.Writer, reflect.Type), store reflect.Type, outPath string, licenseText string) {
	filename := path.Join(outPath, camelCaseToSnakeCase(store.Name())+"_gen.go")
	w, err := os.Create(filename)
	fatalIfErr(err)
	defer func() {
		fatalIfErr(w.Close())
	}()
	_, err = fmt.Fprintf(w, "%s\n// Code generated by cmd/tools/genfaultinjection. DO NOT EDIT.\n", licenseText)
	fatalIfErr(err)
	generator(w, store)
}

func generateFaultInjectionStore(w io.Writer, store reflect.Type) {
	writeStoreHeader(w, store, `
package faultinjection

import (
	"context"
{{range .ExtraImports}}
	{{printf "%q" .}}
{{- end}}
	"go.temporal.io/server/common/persistence"
)

type (
	faultInjection{{.StoreName}} struct {
		baseStore persistence.{{.StoreName}}
		generator faultGenerator
	}
)

func newFaultInjection{{.StoreName}}(
	baseStore persistence.{{.StoreName}},
	generator faultGenerator,
) *faultInjection{{.StoreName}} {
	return &faultInjection{{.StoreName}}{
		baseStore: baseStore,
		generator: generator,
	}
}
`)

	writeStoreMethods(w, store, `
func (c *faultInjection{{.StoreName}}) {{.MethodName}}({{.InParams}}){{.OutParams}} {
	return inject{{.NumOutParams}}(c.generator.generate("{{.MethodName}}"), func(){{.OutParams}} {
		return c.baseStore.{{.MethodName}}({{.InVars}})
	})
}
`, `
func (c *faultInjection{{.StoreName}}) {{.MethodName}}({{.InParams}}){{.OutParams}} {
	{{if ne .OutParams ""}}return {{end}}c.baseStore.{{.MethodName}}({{.InVars}})
}
`)
}

func writeStoreHeader(w io.Writer, store reflect.Type, tmpl string) {
	fatalIfErr(template.Must(template.New("code").Parse(tmpl)).Execute(w, map[string]any{
		"StoreName":    store.Name(),
		"ExtraImports": extraImports[store.Name()],
	}))
}

func writeStoreMethods(w io.Writer, store reflect.Type, tmpl string, passThroughTmpl string) {
	for i := 0; i < store.NumMethod(); i++ {
		writeStoreMethod(w, store, store.Method(i), tmpl, passThroughTmpl)
	}
}

func writeStoreMethod(w io.Writer, store reflect.Type, m reflect.Method, tmpl string, passThroughTmpl string) {

	mT := m.Type
	inParams := ""
	inVars := ""
	for i := 0; i < mT.NumIn(); i++ {
		pName := fmt.Sprintf("p%d", i)
		// Special names for ctx and request for better intellisense support.
		if mT.In(i).String() == "context.Context" {
			pName = "ctx"
		} else if strings.HasSuffix(mT.In(i).String(), "Request") {
			pName = "request"
		}
		inParams += fmt.Sprintf("\n\t%s %s,", pName, mT.In(i).String())
		inVars += fmt.Sprintf("%s, ", pName)
	}
	if mT.NumIn() > 0 {
		// Remove trailing ", " if any.
		inVars = inVars[:len(inVars)-2]
		inParams += "\n"
	}

	outParams := ""
	for i := 0; i < mT.NumOut(); i++ {
		outParams += fmt.Sprintf("%s, ", mT.Out(i).String())
	}
	if mT.NumOut() > 0 {
		// Remove trailing ", " if any.
		outParams = outParams[:len(outParams)-2]
		if mT.NumOut() > 1 {
			outParams = fmt.Sprintf(" (%s)", outParams)
		} else {
			outParams = fmt.Sprintf(" %s", outParams)
		}
	}

	fields := map[string]any{
		"StoreName":    store.Name(),
		"MethodName":   m.Name,
		"InParams":     inParams,
		"InVars":       inVars,
		"OutParams":    outParams,
		"NumOutParams": mT.NumOut() - 1,
	}

	var t string
	if mT.NumIn() > 1 && mT.In(0).String() == "context.Context" &&
		mT.NumOut() > 0 && mT.Out(mT.NumOut()-1).String() == "error" {
		// Fault injection only for methods which look like:
		// ListClusterMetadata(ctx context.Context, ...) (..., error)
		t = tmpl
	} else {
		// Others (i.e. GetName, Close, etc.) just pass through.
		t = passThroughTmpl
	}

	fatalIfErr(template.Must(template.New("code").Parse(t)).Execute(w, fields))
}

func readLicenseFile(filePath string) string {
	text, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}
	var lines []string
	for _, line := range strings.Split(string(text), "\n") {
		lines = append(lines, strings.TrimRight("// "+line, " "))
	}
	return strings.Join(lines, "\n") + "\n"
}
func fatalIfErr(err error) {
	if err != nil {
		//nolint:revive // calls to log.Fatal only in main() or init() functions (revive)
		log.Fatal(err)
	}
}

func camelCaseToSnakeCase(s string) string {
	if s == "" {
		return ""
	}
	t := make([]rune, 0, len(s)+5)
	for i, c := range s {
		if isASCIIUpper(c) {
			if i != 0 {
				t = append(t, '_')
			}
			c ^= ' ' // Make it a lower letter.
		}
		t = append(t, c)
	}
	return string(t)
}

func isASCIIUpper(c rune) bool {
	return 'A' <= c && c <= 'Z'
}
