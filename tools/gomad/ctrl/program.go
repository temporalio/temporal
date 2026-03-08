package sim_ctrl

import (
	"embed"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/go-cmd/cmd"
	"github.com/pingcap/failpoint/code"
	"github.com/pkg/errors"

	sim_runtime "go.temporal.io/server/tools/gomad/runtime"
	transformer "go.temporal.io/server/tools/gomad/transformer"
	"go.temporal.io/server/tools/gomad/util/verify"
)

//go:embed overrides
var staticOverrides embed.FS

const binaryName = "./simprog"

var golangVersion = strings.TrimPrefix(runtime.Version(), "go")

type (
	simProgram struct {
		workingDir  string
		sourceDir   string
		testFilter  string
		realModRoot string // absolute path of the source module root (for TEMPORAL_ROOT)
	}
	simConfig struct {
		debug          bool
		seed           int64
		remoteAddr     string
		remoteId       string
		extraTestFlags []string
	}
)

func createNewSimProgram(workingDir, sourceDir, testFilter string, resetFiles bool) *simProgram {
	workingDir, _ = filepath.Abs(workingDir)
	p := &simProgram{workingDir: workingDir, sourceDir: sourceDir, testFilter: testFilter}
	p.generate(resetFiles)
	p.compile()
	return p
}

func (p *simProgram) generate(resetFiles bool) {
	fmt.Println("[ctrl]", "building simulation")

	if resetFiles {
		fmt.Println("[ctrl]", "reset simulation first")
		err := os.RemoveAll(p.workingDir)
		if err != nil {
			panic(err)
		}
	}

	srcTestDir := p.sourceDir
	if srcTestDir == "" {
		// Legacy fallback: derive from the caller's file path.
		_, callerFile, _, _ := runtime.Caller(3)
		srcTestDir = filepath.Join(filepath.Dir(callerFile), "tests")
	}

	// generate simulation
	mods, err := transformer.Run(
		&transformer.Config{
			Dir:                    srcTestDir,
			BuildFlags:             []string{"-tags=test_dep"},
			Skip:                   transformer.TemporalDefaultSkip,
			GRPCRewritePkgPrefixes: transformer.TemporalGRPCRewritePkgPrefixes,
			NativeHTTPPkgPrefixes:  transformer.TemporalNativeHTTPPkgPrefixes,
			CacheFunc:              transformer.CreateCacheWriter(p.workingDir),
			ResultFunc:             transformer.CreateFileWriter(p.workingDir),
		},
	)
	if err != nil {
		panic(err)
	}

	// Apply static file overrides. These files are embedded in the binary and
	// override transformer-generated output for packages that need simulation-
	// aware behaviour not expressible via the normal AST transform (e.g.
	// replacing a native sync.Mutex with a cooperative SIMLIB.Mutex).
	applyStaticOverrides(p.workingDir, staticOverrides)

	// rewrite failpoints
	rewriter := code.NewRewriter(p.workingDir)
	if err = rewriter.Rewrite(); err != nil {
		panic(errors.Wrap(err, "faultpoint rewrite failed"))
	}

	// Locate the transformer output directory for the source test package.
	pkgPath := derivePkgPath(srcTestDir)
	dstTestDir := filepath.Join(p.workingDir, "gomad.local", pkgPath)
	fmt.Printf("\ncopying all test files from %v\n", dstTestDir)
	testFilePaths, err := filepath.Glob(filepath.Join(dstTestDir, "*_test.go"))
	if err != nil {
		panic(err)
	}
	if len(testFilePaths) == 0 {
		panic(fmt.Sprintf("no test files found in %v", dstTestDir))
	}

	// Inject simulator lifecycle files if the package has no TestMain.
	if !hasTestMain(testFilePaths) {
		pkgName := readPackageName(testFilePaths[0])
		writeSimulatorFiles(dstTestDir, pkgName)
		// Include the newly written files in the copy list.
		testFilePaths, err = filepath.Glob(filepath.Join(dstTestDir, "*_test.go"))
		if err != nil {
			panic(err)
		}
	}

	// Also include non-test Go files from the source test package (e.g. shared
	// helper types used by multiple test files), excluding build-ignore files.
	srcFilePaths, err := filepath.Glob(filepath.Join(dstTestDir, "*.go"))
	if err != nil {
		panic(err)
	}
	allFilePaths := make([]string, 0, len(testFilePaths)+len(srcFilePaths))
	allFilePaths = append(allFilePaths, testFilePaths...)
	for _, fp := range srcFilePaths {
		if !strings.HasSuffix(fp, "_test.go") && !isBuildIgnore(fp) {
			allFilePaths = append(allFilePaths, fp)
		}
	}

	for _, testFilePath := range allFilePaths {
		fmt.Printf("copying test file %v\n", testFilePath)

		srcFile, err := os.Open(testFilePath)
		if err != nil {
			panic(err)
		}
		defer srcFile.Close()
		dstFile, err := os.Create(filepath.Join(p.workingDir, filepath.Base(testFilePath)))
		if err != nil {
			panic(err)
		}
		defer dstFile.Close()
		_, err = io.Copy(dstFile, srcFile)
		if err != nil {
			panic(err)
		}
	}

	// create go.mod for tests
	goMod := fmt.Sprintf("module %s\n\ngo %s", "mad-sim-test", golangVersion)
	if err = os.WriteFile(filepath.Join(p.workingDir, "go.mod"), []byte(goMod), os.ModePerm); err != nil {
		panic(err)
	}

	// create go.work
	var goWork strings.Builder
	goWork.WriteString("go " + golangVersion + "\n\n")
	sort.Strings(mods)
	var prev string
	for _, mod := range mods {
		if mod == prev {
			continue
		}
		prev = mod
		goWork.WriteString(fmt.Sprintf("use %s\n", mod))
	}
	// Include the real source module so that simulator API packages (api/lang,
	// api/lib, runtime, etc.) can be resolved by their original import paths.
	// These packages are written to gomad.local/ as-is (not transformed), so
	// their internal imports still reference the real module.
	realModRoot := findModuleRoot(srcTestDir)
	p.realModRoot = realModRoot
	relModRoot, err := filepath.Rel(p.workingDir, realModRoot)
	if err != nil {
		panic(err)
	}
	goWork.WriteString(fmt.Sprintf("use %s\n", relModRoot))
	goWork.WriteString("use .")
	if err = os.WriteFile(filepath.Join(p.workingDir, "go.work"), []byte(goWork.String()), os.ModePerm); err != nil {
		panic(err)
	}
}

// derivePkgPath walks up from dir to find go.mod and computes the import path.
func derivePkgPath(dir string) string {
	abs, err := filepath.Abs(dir)
	if err != nil {
		panic(err)
	}
	cur := abs
	for {
		content, err := os.ReadFile(filepath.Join(cur, "go.mod"))
		if err == nil {
			for _, line := range strings.SplitN(string(content), "\n", 10) {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "module ") {
					moduleName := strings.TrimSpace(strings.TrimPrefix(line, "module "))
					rel, err := filepath.Rel(cur, abs)
					if err != nil {
						panic(err)
					}
					return moduleName + "/" + filepath.ToSlash(rel)
				}
			}
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			panic("no go.mod found above " + abs)
		}
		cur = parent
	}
}

// findModuleRoot returns the absolute path of the directory containing the
// go.mod file for the module that owns dir.
func findModuleRoot(dir string) string {
	abs, err := filepath.Abs(dir)
	if err != nil {
		panic(err)
	}
	cur := abs
	for {
		if _, err := os.Stat(filepath.Join(cur, "go.mod")); err == nil {
			return cur
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			panic("no go.mod found above " + abs)
		}
		cur = parent
	}
}

// isBuildIgnore reports whether the given Go source file has a //go:build ignore constraint.
func isBuildIgnore(path string) bool {
	content, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	for _, line := range strings.SplitN(string(content), "\n", 20) {
		line = strings.TrimSpace(line)
		if line == "//go:build ignore" {
			return true
		}
		if line != "" && !strings.HasPrefix(line, "//") {
			break
		}
	}
	return false
}

// hasTestMain reports whether any of the given *_test.go files contains "func TestMain".
func hasTestMain(paths []string) bool {
	fset := token.NewFileSet()
	for _, path := range paths {
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			continue
		}
		for _, decl := range f.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "TestMain" {
				return true
			}
		}
	}
	return false
}

// readPackageName returns the package name declared in the given Go source file.
func readPackageName(path string) string {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.PackageClauseOnly)
	if err != nil {
		panic(err)
	}
	return f.Name.Name
}

// writeSimulatorFiles writes _gomad_runtime_test.go and _gomad_main_test.go into dir.
func writeSimulatorFiles(dir, pkgName string) {
	runtimeFile := fmt.Sprintf("// Code generated by gomad. DO NOT EDIT.\npackage %s\n\nimport _ \"go.temporal.io/server/tools/gomad/api/runtime\"\n", pkgName)
	if err := os.WriteFile(filepath.Join(dir, "gomad_runtime_test.go"), []byte(runtimeFile), os.ModePerm); err != nil {
		panic(err)
	}

	mainFile := fmt.Sprintf(`// Code generated by gomad. DO NOT EDIT.
package %s

import (
	"testing"

	SIMLIB "go.temporal.io/server/tools/gomad/api/lib"
	SIM "go.temporal.io/server/tools/gomad/runtime"
)

func TestMain(m *testing.M) {
	SIM.Start()
	m.Run()
	SIMLIB.CloseAllListeners()
	SIM.Join()
}
`, pkgName)
	if err := os.WriteFile(filepath.Join(dir, "gomad_main_test.go"), []byte(mainFile), os.ModePerm); err != nil {
		panic(err)
	}
}

// applyStaticOverrides copies all files from the embedded "overrides" FS into
// workingDir/gomad.local/, overwriting whatever the transformer generated.
// Override files use the ".go.overlay" extension to prevent the Go toolchain
// from trying to compile them as part of the ctrl package; the extension is
// stripped back to ".go" when writing the destination file.
// This is used for packages where we need simulation-aware behaviour that
// cannot be expressed via the standard AST transform (e.g. cooperative
// mutexes in third-party C-to-Go packages).
func applyStaticOverrides(workingDir string, efs embed.FS) {
	const (
		prefix          = "overrides"
		overlaySuffix   = ".go.overlay"
		destinationSuffix = ".go"
	)
	err := fs.WalkDir(efs, prefix, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		// Strip the "overrides/" prefix to get the relative destination path
		// under gomad.local/.
		rel := strings.TrimPrefix(path, prefix+"/")
		// Rename .go.overlay → .go at the destination.
		if strings.HasSuffix(rel, overlaySuffix) {
			rel = rel[:len(rel)-len(overlaySuffix)] + destinationSuffix
		}
		dst := filepath.Join(workingDir, "gomad.local", filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(dst), os.ModePerm); err != nil {
			return err
		}
		content, err := efs.ReadFile(path)
		if err != nil {
			return err
		}
		fmt.Printf("applying static override: %s\n", dst)
		return os.WriteFile(dst, content, os.ModePerm)
	})
	if err != nil {
		panic(fmt.Errorf("applyStaticOverrides: %w", err))
	}
}

func (p *simProgram) compile() {
	compileCmd := cmd.NewCmd(
		"go", "test",
		"-c", // compile instead of execute
		"-o", binaryName,
		"-tags=test_dep",
		".",
	)
	compileCmd.Dir = p.workingDir
	compileCmd.Env = append(os.Environ(), "CGO_ENABLED=0")

	fmt.Println("[ctrl]", "compiling simulation")

	status := <-compileCmd.Start()

	logs := strings.Join(status.Stderr, "\n")
	verify.T(status.Error == nil, "⚠️failed to compile sim: %v\n\nlogs:\n%v", status.Error, logs)
	verify.T(status.Exit == 0, "⚠️failed to compile sim: exit code %v\n\nlogs:\n%v", status.Exit, logs)
}

func (p *simProgram) start(conf simConfig) (<-chan cmd.Status, <-chan string) {
	fmt.Printf("=== GOMAD seed=%d (replay: go test -tags gomad -gomad.seed=%d ./...)\n",
		conf.seed, conf.seed)

	args := []string{"-test.v", "-test.count=1"}
	args = append(args, conf.extraTestFlags...)
	if p.testFilter != "" {
		args = append(args, "-test.run="+p.testFilter, "-test.parallel=1")
	}
	testCmd := cmd.NewCmdOptions(
		cmd.Options{
			Buffered:  false,
			Streaming: true,
		},
		binaryName,
		args...,
	)
	testCmd.Dir = p.workingDir

	envVars := []string{
		fmt.Sprintf("%v=%v", sim_runtime.DebugModeEnvKey, conf.debug),
		fmt.Sprintf("%v=%v", sim_runtime.SeedEnvKey, conf.seed),
		fmt.Sprintf("%v=%v", sim_runtime.RemoteControlAddrEnvKey, conf.remoteAddr),
		fmt.Sprintf("%v=%v", sim_runtime.RemoteControlIdEnvKey, conf.remoteId),
	}
	if p.realModRoot != "" {
		// Point TEMPORAL_ROOT at the real source module root so that test
		// helpers that use runtime.Caller to locate schema and config files
		// (e.g. tests/testutils/source_root.go) resolve to the real tree
		// instead of the gomad.local/ transformed copy.
		envVars = append(envVars, "TEMPORAL_ROOT="+p.realModRoot)
	}
	testCmd.Env = append(os.Environ(), envVars...)

	fmt.Println("[ctrl]", "starting simulation with "+strings.Join(envVars, " "))

	// goroutine to forward stdout/stderr
	logsCh := make(chan string)
	go func() {
		for testCmd.Stdout != nil || testCmd.Stderr != nil {
			select {
			case line, open := <-testCmd.Stdout:
				if !open {
					testCmd.Stdout = nil
					continue
				}
				logsCh <- line
			case line, open := <-testCmd.Stderr:
				if !open {
					testCmd.Stderr = nil
					continue
				}
				logsCh <- line
			}
		}
	}()

	return testCmd.Start(), logsCh
}
