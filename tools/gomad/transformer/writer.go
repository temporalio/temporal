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
	"runtime"
	"strings"

	"golang.org/x/tools/go/packages"
)

var golangVersion = strings.TrimPrefix(runtime.Version(), "go")

func CreateFileWriter(outDir string) func(pkg *packages.Package, files map[string]string) string {
	fmt.Printf("writing output to: %v\n", outDir)
	if err := os.MkdirAll(outDir, os.ModePerm); err != nil {
		panic(err)
	}

	// Track modules whose go.mod has already been written. ResultFunc is called
	// sequentially so no mutex is needed.
	writtenMods := make(map[string]bool)

	return func(pkg *packages.Package, files map[string]string) string {
		// Internal test packages share PkgPath with their base package but have
		// an ID like "foo [foo.test]". Strip the bracket suffix so they are
		// written into the same output directory as the regular package.
		pkgPath := pkg.PkgPath
		if idx := strings.Index(pkgPath, " ["); idx != -1 {
			pkgPath = pkgPath[:idx]
		}
		pkgIdParts := strings.Split(pkgPath, "/")
		pkgDir := pkgIdParts[len(pkgIdParts)-1]

		for srcPath, content := range files {
			relPath := filepath.Base(srcPath)
			resolved := false
			for {
				parentDir := filepath.Base(filepath.Dir(strings.TrimSuffix(srcPath, relPath)))
				if strings.Contains(parentDir, "@v") {
					resolved = true
					break // root of external module reached
				}
				if parentDir == strings.TrimSuffix(pkgDir, "_test") {
					resolved = true
					break // found the right parent folder for the package
				}
				if parentDir == "." {
					// Build-cache or generated artifact with no traceable source
					// path. The Go build system will regenerate it; skip here.
					fmt.Printf("skipping unresolvable path for %s: %s\n", pkg.PkgPath, srcPath)
					break
				}
				relPath = filepath.Join(parentDir, relPath)
			}
			if !resolved {
				continue
			}

			dstPath := filepath.Join(outDir, simPkgPrefix, pkgPath, relPath)
			if err := os.MkdirAll(filepath.Dir(dstPath), os.ModePerm); err != nil {
				panic(err)
			}
			if err := os.WriteFile(dstPath, []byte(content), os.ModePerm); err != nil {
				panic(err)
			}
		}

		// Derive the module path from the original module info so that all
		// packages belonging to the same upstream module share one go.mod.
		// This preserves the module boundary required by Go's internal-package
		// rule (e.g. cloud.google.com/go/storage can import
		// cloud.google.com/go/internal because both map to modules that keep
		// their original parent-child relationship under the gomad.local/ prefix).
		modulePkgPath := pkgPath
		if pkg.Module != nil {
			modulePkgPath = pkg.Module.Path
		}
		modPath := filepath.Join(simPkgPrefix, modulePkgPath)

		if !writtenMods[modPath] {
			writtenMods[modPath] = true
			dstModDir := filepath.Join(outDir, modPath)
			if err := os.MkdirAll(dstModDir, os.ModePerm); err != nil {
				panic(err)
			}
			goMod := fmt.Sprintf("module %s\n\ngo %s", modPath, golangVersion)
			if err := os.WriteFile(filepath.Join(dstModDir, "go.mod"), []byte(goMod), os.ModePerm); err != nil {
				panic(err)
			}
		}

		return modPath
	}
}
