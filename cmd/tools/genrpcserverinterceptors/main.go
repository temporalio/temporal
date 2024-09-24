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
	"sort"
	"strconv"
	"strings"
	"text/template"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
)

const maxRequestDepth = 5

type (
	requestData struct {
		Type string

		WorkflowIdGetter string
		RunIdGetter      string
		TaskTokenGetter  string
	}

	grpcServerData struct {
		Server   string
		Imports  []string
		Requests []requestData
	}
)

var (
	// List of types for which Workflow tag getters are generated.
	grpcServers = []reflect.Type{
		reflect.TypeOf((*workflowservice.WorkflowServiceServer)(nil)).Elem(),
		reflect.TypeOf((*adminservice.AdminServiceServer)(nil)).Elem(),
		reflect.TypeOf((*historyservice.HistoryServiceServer)(nil)).Elem(),
		reflect.TypeOf((*matchingservice.MatchingServiceServer)(nil)).Elem(),
	}

	// Only request fields that end with suffixes or match exact name are eligible for deeper inspection.
	fieldNameSuffixes = []string{
		"Request",
	}
	fieldNames = map[string]struct{}{
		"Completion":      {},
		"UpdateRef":       {},
		"ParentExecution": {},
		"WorkflowState":   {},
		"ExecutionInfo":   {},
		"ExecutionState":  {},
	}

	// These types have task_token field, but it is not of type *tokenspb.Task and doesn't have Workflow tags.
	excludeTaskTokenTypes = []reflect.Type{
		reflect.TypeOf((*workflowservice.RespondQueryTaskCompletedRequest)(nil)),
		reflect.TypeOf((*workflowservice.RespondNexusTaskCompletedRequest)(nil)),
		reflect.TypeOf((*workflowservice.RespondNexusTaskFailedRequest)(nil)),
	}

	executionGetterT = reflect.TypeOf((*interface {
		GetExecution() *commonpb.WorkflowExecution
	})(nil)).Elem()

	workflowExecutionGetterT = reflect.TypeOf((*interface {
		GetWorkflowExecution() *commonpb.WorkflowExecution
	})(nil)).Elem()

	taskTokenGetterT = reflect.TypeOf((*interface {
		GetTaskToken() []byte
	})(nil)).Elem()

	workflowIdGetterT = reflect.TypeOf((*interface {
		GetWorkflowId() string
	})(nil)).Elem()

	runIdGetterT = reflect.TypeOf((*interface {
		GetRunId() string
	})(nil)).Elem()
)

func main() {
	outPathFlag := flag.String("out", ".", "path to write generated files")
	licenseFlag := flag.String("copyright_file", "../../../LICENSE", "path to license to copy into header")
	flag.Parse()

	licenseText := readLicenseFile(*licenseFlag)

	for _, grpcServerT := range grpcServers {
		callWithFile(generateWorkflowTagGetters, grpcServerT, *outPathFlag, licenseText)
	}
}

func generateWorkflowTagGetters(w io.Writer, grpcServerT reflect.Type) {

	writeGrpcServerData(w, grpcServerT, `
package logtags

import (
{{- range .Imports}}
	{{printf "%q" .}}
{{- end}}
	"go.temporal.io/server/common/log/tag"
)

func (wt *WorkflowTags) extractFrom{{.Server}}Request(req any) []tag.Tag {
	switch r := req.(type) {
	{{- range .Requests}}
	case {{.Type}}:
	{{- if or .TaskTokenGetter .WorkflowIdGetter .RunIdGetter}}
		{{- if .TaskTokenGetter}}
		return wt.fromTaskToken(r.{{ .TaskTokenGetter}})
		{{- else}}
		return []tag.Tag{
		{{if .WorkflowIdGetter}}	tag.WorkflowID(r.{{.WorkflowIdGetter}}),
		{{end -}}
		{{if .RunIdGetter}}	tag.WorkflowRunID(r.{{.RunIdGetter}}),
		{{end -}}
		}
		{{- end}}
	{{- else}}
		return nil
	{{- end -}}
	{{- end}}
	default:
		return nil
	}
}
`)
}

func writeGrpcServerData(w io.Writer, grpcServerT reflect.Type, tmpl string) {
	sd := grpcServerData{
		Server:  grpcServerT.Name(),
		Imports: []string{grpcServerT.PkgPath()},
	}

	for i := 0; i < grpcServerT.NumMethod(); i++ {
		rpcT := grpcServerT.Method(i).Type
		if rpcT.NumIn() < 2 {
			continue
		}
		requestT := rpcT.In(1) // Assume request is always the second parameter.

		rd := workflowTagGetters(requestT, 0)
		rd.Type = requestT.String()
		// processOverrides(requestT, &rd)
		sd.Requests = append(sd.Requests, rd)
	}

	fatalIfErr(template.Must(template.New("code").Parse(tmpl)).Execute(w, sd))
}

//nolint:revive // cognitive complexity 37 (> max enabled 25)
func workflowTagGetters(requestT reflect.Type, depth int) requestData {
	rd := requestData{}
	if depth > maxRequestDepth {
		return rd
	}

	switch {
	case requestT.AssignableTo(executionGetterT):
		rd.WorkflowIdGetter = "GetExecution().GetWorkflowId()"
		rd.RunIdGetter = "GetExecution().GetRunId()"
	case requestT.AssignableTo(workflowExecutionGetterT):
		rd.WorkflowIdGetter = "GetWorkflowExecution().GetWorkflowId()"
		rd.RunIdGetter = "GetWorkflowExecution().GetRunId()"
	case requestT.AssignableTo(taskTokenGetterT):
		for _, ert := range excludeTaskTokenTypes {
			if requestT.AssignableTo(ert) {
				return rd
			}
		}
		rd.TaskTokenGetter = "GetTaskToken()"
	default:
		// Might be one of these, both, or neither.
		if requestT.AssignableTo(workflowIdGetterT) {
			rd.WorkflowIdGetter = "GetWorkflowId()"
		}
		if requestT.AssignableTo(runIdGetterT) {
			rd.RunIdGetter = "GetRunId()"
		}
	}

	for _, nestedRequest := range subFields(requestT) {
		if nestedRequest.Type.Kind() != reflect.Ptr {
			continue
		}
		if nestedRequest.Type.Elem().Kind() != reflect.Struct {
			continue
		}
		hasAllowedSuffix := false
		for _, suffix := range fieldNameSuffixes {
			if strings.HasSuffix(nestedRequest.Name, suffix) {
				hasAllowedSuffix = true
				break
			}
		}
		if _, hasAllowedName := fieldNames[nestedRequest.Name]; !hasAllowedName && !hasAllowedSuffix {
			continue
		}

		nestedRd := workflowTagGetters(nestedRequest.Type, depth+1)
		// First match wins. If getter is already set, it won't be overwritten.
		if rd.WorkflowIdGetter == "" && nestedRd.WorkflowIdGetter != "" {
			rd.WorkflowIdGetter = fmt.Sprintf("Get%s().%s", nestedRequest.Name, nestedRd.WorkflowIdGetter)
		}
		if rd.RunIdGetter == "" && nestedRd.RunIdGetter != "" {
			rd.RunIdGetter = fmt.Sprintf("Get%s().%s", nestedRequest.Name, nestedRd.RunIdGetter)
		}
		if rd.TaskTokenGetter == "" && nestedRd.TaskTokenGetter != "" {
			rd.TaskTokenGetter = fmt.Sprintf("Get%s().%s", nestedRequest.Name, nestedRd.TaskTokenGetter)
		}
	}
	return rd
}

func subFields(t reflect.Type) []reflect.StructField {
	// This function returns subfields ordered in proto index order. Tag is the string like:
	// `protobuf:"bytes,2,opt,name=workflow_id,json=workflowId,proto3" json:"workflow_id,omitempty"`
	//                  ^ - this number is used for ordering.

	var fields []reflect.StructField
	for fieldNum := 0; fieldNum < t.Elem().NumField(); fieldNum++ {
		f := t.Elem().Field(fieldNum)
		if _, ok := f.Tag.Lookup("protobuf"); ok {
			fields = append(fields, f)
		}
	}
	protoOrder := func(tag reflect.StructTag) int {
		o, _ := strconv.Atoi(strings.Split(tag.Get("protobuf"), ",")[1])
		return o
	}
	sort.Slice(fields, func(i, j int) bool {
		return protoOrder(fields[i].Tag) < protoOrder(fields[j].Tag)
	})
	return fields
}

func callWithFile(generator func(io.Writer, reflect.Type), server reflect.Type, outPath string, licenseText string) {
	filename := path.Join(outPath, camelCaseToSnakeCase(server.Name())+"_gen.go")
	w, err := os.Create(filename)
	fatalIfErr(err)
	defer func() {
		fatalIfErr(w.Close())
	}()
	_, err = fmt.Fprintf(w, "%s\n// Code generated by cmd/tools/genrpcserverinterceptors. DO NOT EDIT.\n", licenseText)
	fatalIfErr(err)
	generator(w, server)
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
