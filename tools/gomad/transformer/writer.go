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

package transformer

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/packages"
)

// TODO: pick up golang version dynamically
const golangVersion = "1.22.5"

func CreateFileWriter(outDir string) func(pkg *packages.Package, files map[string]string) string {
	fmt.Printf("writing output to: %v\n", outDir)
	if err := os.MkdirAll(outDir, os.ModePerm); err != nil {
		panic(err)
	}

	return func(pkg *packages.Package, files map[string]string) string {
		pkgIdParts := strings.Split(pkg.PkgPath, "/")
		pkgDir := pkgIdParts[len(pkgIdParts)-1]

		for srcPath, content := range files {
			relPath := filepath.Base(srcPath)
			for {
				parentDir := filepath.Base(filepath.Dir(strings.TrimSuffix(srcPath, relPath)))
				if strings.Contains(parentDir, "@v") {
					break // root of external module reached
				}
				if parentDir == strings.TrimSuffix(pkgDir, "_test") {
					break // found the right parent folder for the package
				}
				if parentDir == "." {
					panic("failed to find relative output path for " + srcPath)
				}
				relPath = filepath.Join(parentDir, relPath)
			}

			dstPath := filepath.Join(outDir, simPkgPrefix, pkg.PkgPath, relPath)
			if err := os.MkdirAll(filepath.Dir(dstPath), os.ModePerm); err != nil {
				panic(err)
			}
			if err := os.WriteFile(dstPath, []byte(content), os.ModePerm); err != nil {
				panic(err)
			}
		}

		dstPkgDir := filepath.Join(outDir, simPkgPrefix, pkg.PkgPath)
		modPath := filepath.Join(simPkgPrefix, pkg.PkgPath)

		goMod := fmt.Sprintf("module %s\n\ngo %s", modPath, golangVersion)
		if err := os.WriteFile(filepath.Join(dstPkgDir, "go.mod"), []byte(goMod), os.ModePerm); err != nil {
			panic(err)
		}

		return modPath
	}
}
