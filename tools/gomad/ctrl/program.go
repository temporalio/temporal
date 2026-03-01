package sim_ctrl

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/go-cmd/cmd"
	"github.com/pingcap/failpoint/code"
	"github.com/pkg/errors"

	sim_runtime "go.temporal.io/server/tools/gomad/runtime"
	transformer "go.temporal.io/server/tools/gomad/transformer"
	"go.temporal.io/server/tools/gomad/util/verify"
)

const binaryName = "./simprog"

var golangVersion = strings.TrimPrefix(runtime.Version(), "go")

type (
	simProgram struct {
		workingDir string
	}
	simConfig struct {
		debug      bool
		seed       int64
		remoteAddr string
		remoteId   string
	}
)

func createNewSimProgram(workingDir string, resetFiles bool) *simProgram {
	workingDir, _ = filepath.Abs(workingDir)
	p := &simProgram{workingDir: workingDir}
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

	_, callerFile, _, _ := runtime.Caller(3)
	srcTestDir := filepath.Join(filepath.Dir(callerFile), testDirName)

	// generate simulation
	mods, err := transformer.Run(
		&transformer.Config{
			Dir: srcTestDir,
			//Pattern:    srcTestDir,
			CacheFunc:  transformer.CreateCacheWriter(p.workingDir),
			ResultFunc: transformer.CreateFileWriter(p.workingDir),
		},
	)
	if err != nil {
		panic(err)
	}

	// rewrite failpoints
	rewriter := code.NewRewriter(p.workingDir)
	if err = rewriter.Rewrite(); err != nil {
		panic(errors.Wrap(err, "faultpoint rewrite failed"))
	}

	// copy test files to root for convenience
	bi, _ := debug.ReadBuildInfo()
	dstTestDir := filepath.Join(p.workingDir, "gomad.local", bi.Path, testPkgName)
	fmt.Printf("\ncopying all test files from %v\n", dstTestDir)
	testFilePaths, err := filepath.Glob(filepath.Join(dstTestDir, "*_test.go"))
	if err != nil {
		panic(err)
	}
	if len(testFilePaths) == 0 {
		panic("no tests files found! (make sure the test package is `test_tests`)")
	}
	for _, testFilePath := range testFilePaths {
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
	for _, mod := range mods {
		goWork.WriteString(fmt.Sprintf("use %s\n", mod))
	}
	goWork.WriteString("use .")
	if err = os.WriteFile(filepath.Join(p.workingDir, "go.work"), []byte(goWork.String()), os.ModePerm); err != nil {
		panic(err)
	}
}

func (p *simProgram) compile() {
	compileCmd := cmd.NewCmd(
		"go", "test",
		"-c", // compile instead of execute
		"-o", binaryName,
		".",
	)
	compileCmd.Dir = p.workingDir

	fmt.Println("[ctrl]", "compiling simulation")

	status := <-compileCmd.Start()

	logs := strings.Join(status.Stderr, "\n")
	verify.T(status.Error == nil, "⚠️failed to compile sim: %v\n\nlogs:\n%v", status.Error, logs)
	verify.T(status.Exit == 0, "⚠️failed to compile sim: exit code %v\n\nlogs:\n%v", status.Exit, logs)
}

func (p *simProgram) start(conf simConfig) (<-chan cmd.Status, <-chan string) {
	testCmd := cmd.NewCmdOptions(
		cmd.Options{
			Buffered:  false,
			Streaming: true,
		},
		binaryName,
		"-test.v",
		"-test.count=1", // disable Go cache
	)
	testCmd.Dir = p.workingDir

	envVars := []string{
		fmt.Sprintf("%v=%v", sim_runtime.DebugModeEnvKey, conf.debug),
		fmt.Sprintf("%v=%v", sim_runtime.SeedEnvKey, conf.seed),
		fmt.Sprintf("%v=%v", sim_runtime.RemoteControlAddrEnvKey, conf.remoteAddr),
		fmt.Sprintf("%v=%v", sim_runtime.RemoteControlIdEnvKey, conf.remoteId),
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
