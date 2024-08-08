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
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	expmaps "golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

var (
	matchImport   = regexp.MustCompile(`^\s*import\s+"([^"]+\.proto)"\s*;\s*$`)
	versionSuffix = regexp.MustCompile(`^(.*)/v\d+$`)

	// set by files.go if present
	importMap map[string]protoreflect.FileDescriptor
)

func fatalIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func findProtoImports() []string {
	importMap := make(map[string]struct{})
	fatalIfErr(filepath.WalkDir("proto/internal", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() && strings.HasSuffix(path, ".proto") {
			protoFile, err := os.ReadFile(path)
			fatalIfErr(err)
			for _, line := range strings.Split(string(protoFile), "\n") {
				if match := matchImport.FindStringSubmatch(line); len(match) > 0 {
					i := match[1]
					if strings.HasPrefix(i, "temporal/api/") ||
						strings.HasPrefix(i, "google/") {
						importMap[i] = struct{}{}
					}
				}
			}
		}
		return nil
	}))
	return expmaps.Keys(importMap)
}

func getImportName(i string) string {
	withoutV := i
	if match := versionSuffix.FindStringSubmatch(i); match != nil {
		withoutV = match[1]
	}
	return filepath.Base(withoutV)
}

func mangle(p string) string {
	mangled := strings.ReplaceAll(p, "/", "_")
	return "File_" + strings.ReplaceAll(mangled, ".", "_")
}

func genFileList(protoImports []string) {
	sort.Strings(protoImports)

	goImportsMap := make(map[string]string)
	protoToPackage := make(map[string]string)

	for _, i := range protoImports {
		if strings.HasPrefix(i, "temporal/api/") {
			goImport := filepath.Dir(strings.Replace(i, "temporal/api/", "go.temporal.io/api/", 1))
			importName := getImportName(goImport)
			goImportsMap[goImport] = importName
			protoToPackage[i] = importName
		} else if strings.HasPrefix(i, "google/") {
			base := strings.TrimSuffix(filepath.Base(i), ".proto") + "pb"
			goImport := "google.golang.org/protobuf/types/known/" + base
			goImportsMap[goImport] = base
			protoToPackage[i] = base
		}
	}
	goImports := expmaps.Keys(goImportsMap)
	sort.Strings(goImports)

	out, err := os.Create("cmd/tools/getproto/files.go")
	fatalIfErr(err)
	defer out.Close()
	fmt.Fprintf(out, `
// Code generated by getproto. DO NOT EDIT.
// If you get build errors in this file, just delete it. It will be regenerated.

package main

import (
	"google.golang.org/protobuf/reflect/protoreflect"

`)
	for _, i := range goImports {
		fmt.Fprintf(out, "\t%s %q\n", goImportsMap[i], i)
	}
	fmt.Fprintf(out, `)

func init() {
	importMap = make(map[string]protoreflect.FileDescriptor)
`)
	for _, i := range protoImports {
		fmt.Fprintf(out, "\timportMap[%q] = %s.%s\n", i, protoToPackage[i], mangle(i))
	}
	out.WriteString("}\n")
}

func addImports(missing []string) {
	newImportMap := make(map[string]struct{})
	for i, _ := range importMap {
		newImportMap[i] = struct{}{}
	}
	for _, i := range missing {
		newImportMap[i] = struct{}{}
	}

	genFileList(expmaps.Keys(newImportMap))
	fmt.Println("<rerun>")
	os.Exit(0)
}

func initSeeds() {
	genFileList(findProtoImports())
	fmt.Println("<rerun>")
	os.Exit(0)
}

func checkImports(files map[string]protoreflect.FileDescriptor) {
	missing := make(map[string]struct{})
	for _, fd := range files {
		imports := fd.Imports()
		num := imports.Len()
		for i := 0; i < num; i++ {
			imp := imports.Get(i).Path()
			if strings.HasPrefix(imp, "temporal/api/") || strings.HasPrefix(imp, "google/") {
				if _, ok := files[imp]; !ok {
					missing[imp] = struct{}{}
				}
			}
		}
	}
	if len(missing) > 0 {
		addImports(expmaps.Keys(missing)) // doesn't return
	}
}

func main() {
	out := flag.String("out", "", "where to put the serialized FileDescriptorSet")
	flag.Parse()

	if *out == "" {
		flag.Usage()
		os.Exit(1)
	}

	if len(importMap) == 0 {
		initSeeds() // doesn't return
	}

	checkImports(importMap) // doesn't return if any errors

	set := &descriptorpb.FileDescriptorSet{}
	for _, fd := range importMap {
		set.File = append(set.File, protodesc.ToFileDescriptorProto(fd))
	}

	b, err := proto.Marshal(set)
	fatalIfErr(err)
	fatalIfErr(os.WriteFile(*out, b, 0644))
}
