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
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/tools/go/packages"
)

type (
	Config struct {
		Dir        string
		BuildFlags []string
		Skip       func(pkg *packages.Package) bool
		CacheFunc  func(*packages.Package, func())
		ResultFunc func(*packages.Package, map[string]string) string
	}
	transformResult struct {
		pkgPath string
		files   map[string]string
	}
)

var (
	simulatorPackage           = "go.temporal.io/server/tools/gomad"
	simulatorApiPackage        = simulatorPackage + "/api"
	simulatorLangPackage       = simulatorApiPackage + "/lang"
	simulatorLibPackage        = simulatorApiPackage + "/lib"
	simPkgPrefix               = "gomad.local" // Go requires root to contain a dot
	defaultSkipPackagePatterns = []string{
		// to prevent import cycles
		simulatorLangPackage,
		simulatorLibPackage,
		simulatorPackage + "/runtime",
		simulatorPackage + "/util",
		"golang.org/x/text/.*",
		"github.com/rivo/uniseg",
		"github.com/spf13/afero.*",
		"github.com/pingcap/.*",
		"github.com/davecgh/go-spew/.*",

		// to prevent ... issues?
		"modernc.org/libc/.*",
		"modernc.org/memory/.*",
	}
	DefaultSkip = func(pkg *packages.Package) bool {
		for _, pattern := range defaultSkipPackagePatterns {
			if match, _ := regexp.MatchString(pattern, pkg.PkgPath); match {
				fmt.Println("skipping package", pkg.PkgPath)
				return true
			}
		}
		return false
	}
)

func Run(config *Config) ([]string, error) {
	if config.Dir == "" {
		panic("missing `dir` config parameter")
	}
	fmt.Printf("transforming packages under %v\n", config.Dir)

	if config.Skip == nil {
		config.Skip = DefaultSkip
	}

	cfg := &packages.Config{
		Dir:        config.Dir,
		BuildFlags: config.BuildFlags,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedImports |
			packages.NeedTypes | packages.NeedTypesSizes | packages.NeedSyntax | packages.NeedTypesInfo |
			packages.NeedDeps | packages.NeedEmbedFiles,
		Tests: true,
	}

	todo := make(map[string]*packages.Package)
	var collect func([]*packages.Package)
	collect = func(pkgs []*packages.Package) {
		for _, pkg := range pkgs {
			path := pkg.PkgPath
			if pkg.Name == "" {
				panic(fmt.Sprintf("⚠️invalid package '%v': %v", path, pkg.Errors))
			}

			// ignore test package
			if strings.HasSuffix(path, ".test") {
				continue
			}

			// ignore previously added
			if _, exists := todo[path]; exists {
				continue
			}

			// ignore Go's standard library
			if isStandardImportPath(path) {
				continue
			}

			todo[path] = pkg
			collect(maps.Values(pkg.Imports))
		}
	}

	pkgs, err := packages.Load(cfg,
		"./...",
		//simulatorLangPackage, // always need to include the Simulator itself - WHY?
	)
	if err != nil {
		panic(err)
	}
	collect(pkgs)

	fmt.Printf("transforming %d packages\n", len(todo))

	pkgPaths := maps.Keys(todo)
	sort.Slice(pkgPaths, func(i, j int) bool {
		return pkgPaths[i] < pkgPaths[j]
	})

	var wg sync.WaitGroup
	workChan := make(chan string, len(pkgPaths))
	resultChan := make(chan transformResult, len(pkgPaths))

	// start workers
	for i := 0; i <= runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pkgPath := range workChan {
				fmt.Printf("transforming package %v\n", pkgPath)
				pkg := todo[pkgPath]

				var files map[string]string
				if config.CacheFunc != nil {
					config.CacheFunc(pkg, func() {
						files = transformPackage(config, pkg)
					})
				} else {
					files = transformPackage(config, pkg)
				}

				resultChan <- transformResult{pkgPath, files}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// feed workers
	for _, pkgPath := range pkgPaths {
		workChan <- pkgPath
	}
	close(workChan)

	// collect results
	var mods []string
	for result := range resultChan {
		mods = append(mods, config.ResultFunc(todo[result.pkgPath], result.files))
	}
	return mods, nil
}

func transformPackage(config *Config, pkg *packages.Package) map[string]string {
	skipTransform := config.Skip(pkg)
	files := make(map[string]string, len(pkg.CompiledGoFiles)+len(pkg.IgnoredFiles)+len(pkg.OtherFiles)+len(pkg.EmbedFiles))

	copyFile := func(path string) {
		source, err := os.ReadFile(path)
		if err != nil {
			panic(err)
		}
		files[path] = string(source)
	}

	for i, file := range pkg.Syntax {
		path := pkg.CompiledGoFiles[i]

		var err error
		tf := newFileTransformer(file, pkg, skipTransform)
		files[path], err = tf.transform()
		if err != nil {
			panic(errors.Wrap(err, "failed to transform "+path))
		}
	}

	// other files (such as assembly files) are just copied over
	for _, path := range pkg.IgnoredFiles {
		copyFile(path)
	}
	for _, path := range pkg.OtherFiles {
		copyFile(path)
	}
	for _, path := range pkg.EmbedFiles {
		copyFile(path)
	}

	return files
}

func isStandardImportPath(path string) bool {
	i := strings.Index(path, "/")
	if i < 0 {
		i = len(path)
	}
	elem := path[:i]
	return !strings.Contains(elem, ".")
}
