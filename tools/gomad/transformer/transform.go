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
		// Simulator API packages are written as-is (no import prefixing) so they
		// resolve their own dependencies against the real source module.
		simulatorApiPackage + "/.*",
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

		// pure algorithmic library; uses inline block comments in conditions
		// that trip the AST rewriter (bitreader.go:52)
		"github.com/klauspost/compress/.*",

		// generated protobuf code compiled into the build cache produces
		// hash-named CompiledGoFiles that the writer cannot resolve
		"go.opentelemetry.io/collector/.*",

		// SQL driver packages implement stdlib database/sql/driver interfaces;
		// remapping their database/sql imports to the ext-lib version causes type
		// mismatches because the ext-lib sql.Register still expects stdlib types.
		"github.com/go-sql-driver/mysql",
		"github.com/lib/pq",
		"github.com/jackc/pgx/v5/.*",
		"modernc.org/sqlite",

		// AWS SDK uses net/http/httputil (stdlib) alongside ext-lib net/http types,
		// causing incompatible type errors.
		"github.com/aws/aws-sdk-go/.*",


		// olivere elastic uses net/http types alongside libraries
		// that use stdlib net/http (AWS signer, golang.org/x/net/http2), causing
		// incompatible type errors.
		"github.com/olivere/elastic/.*",

		// tally/v4/prometheus bridges the real-path prometheus ecosystem and net/http
		// (via promhttp.HandlerFor). Skipping AST transform keeps its net/http usage
		// consistent with stdlib, which matches promhttp's stdlib return types.
		"github.com/uber-go/tally/v4/prometheus",

		// The following packages all use stdlib net/http internally, and their
		// sub-packages may use Go's 'internal' mechanism. Skipping AST transform
		// (but still prefixing imports with gomad.local/) keeps types consistent
		// with stdlib and satisfies Go's internal-package import rule because all
		// sub-packages share the gomad.local/ prefix.
		// Note: patterns without a trailing /  match both root and sub-packages
		// because regexp.MatchString checks for a substring match.
		"golang.org/x/net",
		"golang.org/x/oauth2",

		// otelhttp and httpsnoop wrap net/http types and are used by cloud.google.com
		// packages (which are themselves skipped). Keeping them at stdlib net/http
		// prevents type mismatches between the two.
		"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp",
		"github.com/felixge/httpsnoop",

		// The elasticsearch client bridges olivere/elastic (skipped, stdlib net/http)
		// and temporal server code. Skipping it keeps net/http consistent across
		// the elastic library boundary.
		"go.temporal.io/server/common/persistence/visibility/store/elasticsearch",


		"cloud.google.com/.*",
		"github.com/GoogleCloudPlatform/.*",
		"google.golang.org/api/.*",
		"github.com/googleapis/gax-go/.*",
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
			packages.NeedDeps | packages.NeedEmbedFiles | packages.NeedModule,
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

			// Internal test packages (package foo, files ending in _test.go) share
			// PkgPath with their regular package but have a distinct ID like
			// "foo [foo.test]". Key by ID to ensure both are processed; the writer
			// normalises the path back to the base package directory.
			key := path
			if strings.Contains(pkg.ID, " [") {
				key = pkg.ID
			}

			// ignore previously added
			if _, exists := todo[key]; exists {
				continue
			}

			// ignore Go's standard library
			if isStandardImportPath(path) {
				continue
			}

			todo[key] = pkg
			collect(maps.Values(pkg.Imports))
		}
	}

	pkgs, err := packages.Load(cfg,
		"./...",
		// Always include the simulator API packages. Transformed packages gain
		// imports of these (e.g. SIMAPI/SIMLIB/ext-lib), so they must appear as
		// modules in the workspace even when the tested code doesn't import them.
		simulatorApiPackage+"/...",
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

	// Test-variant packages (ID like "foo [foo.test]") share AST objects with
	// the regular package for non-test files. Only include _test.go files here
	// so non-test files are not transformed twice (which would corrupt the AST).
	isTestVariant := strings.Contains(pkg.ID, " [")

	copyFile := func(path string) {
		source, err := os.ReadFile(path)
		if err != nil {
			panic(err)
		}
		files[path] = string(source)
	}

	for i, file := range pkg.Syntax {
		path := pkg.CompiledGoFiles[i]

		if isTestVariant && !strings.HasSuffix(path, "_test.go") {
			continue
		}

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
