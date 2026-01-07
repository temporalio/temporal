package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"go/ast"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Output is the JSON structure for the action output
type Output struct {
	TestPath     string        `json:"test_path"`
	File         string        `json:"file"`
	StartLine    int           `json:"start_line"`
	EndLine      int           `json:"end_line"`
	Contributors []Contributor `json:"contributors,omitempty"`
	Error        string        `json:"error,omitempty"`
}

type Contributor struct {
	Name  string `json:"name"`
	Lines int    `json:"lines"`
}

// funcLocation holds pre-computed location info for a function
type funcLocation struct {
	file      string
	startLine int
	endLine   int
}

func main() {
	// Read configuration from environment variables
	testPath := os.Getenv("INPUT_TEST_PATH")
	testPathsFile := os.Getenv("INPUT_TEST_PATHS_FILE")
	commitHash := os.Getenv("INPUT_COMMIT")
	dir := os.Getenv("INPUT_DIR")
	githubOutput := os.Getenv("GITHUB_OUTPUT")

	// Change to specified directory for package loading
	if dir != "" && dir != "." {
		if err := os.Chdir(dir); err != nil {
			fmt.Fprintf(os.Stderr, "Error changing to directory %s: %v\n", dir, err)
			os.Exit(1)
		}
	}

	// Batch mode: read test paths from file (one per line)
	if testPathsFile != "" {
		runBatchMode(testPathsFile, commitHash)
		return
	}

	// Single test mode (original behavior)
	if testPath == "" {
		fmt.Fprintln(os.Stderr, "Error: INPUT_TEST_PATH or INPUT_TEST_PATHS_FILE is required")
		os.Exit(1)
	}

	runSingleMode(testPath, commitHash, githubOutput)
}

func runSingleMode(testPath, commitHash, githubOutput string) {
	// Parse test path: TestSuiteName/TestFunctionName[/SubtestName...]
	parts := strings.Split(testPath, "/")
	if len(parts) < 2 {
		fmt.Fprintln(os.Stderr, "Error: expected format TestSuiteName/TestFunctionName")
		os.Exit(1)
	}
	testFunc := parts[1]

	// Load packages once
	funcIndex, err := buildFunctionIndex()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading packages: %v\n", err)
		os.Exit(1)
	}

	loc, ok := funcIndex[testFunc]
	if !ok {
		fmt.Fprintf(os.Stderr, "Error finding test: function %s not found\n", testFunc)
		os.Exit(1)
	}

	output := Output{
		TestPath:  testPath,
		File:      loc.file,
		StartLine: loc.startLine,
		EndLine:   loc.endLine,
	}

	if commitHash != "" {
		contributors, err := getContributors(loc.file, loc.startLine, loc.endLine, commitHash)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting contributors: %v\n", err)
			os.Exit(1)
		}
		output.Contributors = contributors
	}

	// Write output
	if githubOutput != "" {
		writeGitHubOutput(githubOutput, output)
	} else {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(output)
	}
}

func runBatchMode(testPathsFile, commitHash string) {
	// Read test paths from file
	file, err := os.Open(testPathsFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening test paths file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	var testPaths []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			testPaths = append(testPaths, line)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading test paths file: %v\n", err)
		os.Exit(1)
	}

	if len(testPaths) == 0 {
		fmt.Fprintln(os.Stderr, "Error: no test paths found in file")
		os.Exit(1)
	}

	// Load packages once for all tests
	fmt.Fprintf(os.Stderr, "Loading packages for %d tests...\n", len(testPaths))
	funcIndex, err := buildFunctionIndex()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading packages: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Indexed %d functions\n", len(funcIndex))

	// Process each test path
	var results []Output
	for _, testPath := range testPaths {
		output := processTestPath(testPath, commitHash, funcIndex)
		results = append(results, output)
	}

	// Output as JSON array
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(results)
}

func processTestPath(testPath, commitHash string, funcIndex map[string]funcLocation) Output {
	parts := strings.Split(testPath, "/")
	if len(parts) < 2 {
		return Output{
			TestPath: testPath,
			Error:    "expected format TestSuiteName/TestFunctionName",
		}
	}
	testFunc := parts[1]

	loc, ok := funcIndex[testFunc]
	if !ok {
		return Output{
			TestPath: testPath,
			Error:    fmt.Sprintf("function %s not found", testFunc),
		}
	}

	output := Output{
		TestPath:  testPath,
		File:      loc.file,
		StartLine: loc.startLine,
		EndLine:   loc.endLine,
	}

	if commitHash != "" {
		contributors, err := getContributors(loc.file, loc.startLine, loc.endLine, commitHash)
		if err != nil {
			output.Error = fmt.Sprintf("git blame failed: %v", err)
		} else {
			output.Contributors = contributors
		}
	}

	return output
}

// writeGitHubOutput writes the output to the GITHUB_OUTPUT file
func writeGitHubOutput(path string, output Output) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening GITHUB_OUTPUT: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	fmt.Fprintf(f, "file=%s\n", output.File)
	fmt.Fprintf(f, "start_line=%d\n", output.StartLine)
	fmt.Fprintf(f, "end_line=%d\n", output.EndLine)

	contributorsJSON, _ := json.Marshal(output.Contributors)
	fmt.Fprintf(f, "contributors=%s\n", contributorsJSON)

	// Also output the full result as a single JSON object
	fullJSON, _ := json.Marshal(output)
	fmt.Fprintf(f, "result=%s\n", fullJSON)
}

// buildFunctionIndex loads all packages in the current module and builds an
// index of function names to their locations. This allows efficient lookup
// of multiple functions without reloading packages.
func buildFunctionIndex() (map[string]funcLocation, error) {
	cfg := &packages.Config{
		// NeedSyntax gives us parsed AST, NeedTypes ensures accurate position info.
		// Tests: true includes _test.go files in the loaded packages.
		Mode:  packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes,
		Tests: true,
	}

	// Load all packages in the module. This is slower than gopls (which uses
	// an index) but avoids the external dependency.
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		return nil, fmt.Errorf("failed to load packages: %w", err)
	}

	// Build index of all function declarations
	index := make(map[string]funcLocation)
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				// fn.Pos() is the start of "func", fn.End() is after closing brace
				pos := pkg.Fset.Position(fn.Pos())
				endPos := pkg.Fset.Position(fn.End())
				index[fn.Name.Name] = funcLocation{
					file:      pos.Filename,
					startLine: pos.Line,
					endLine:   endPos.Line,
				}
			}
		}
	}

	return index, nil
}

// getContributors runs git blame on the specified line range and aggregates
// authors by line count, returning them sorted by most lines first.
func getContributors(filePath string, start, end int, commit string) ([]Contributor, error) {
	// git blame requires a repo-relative path
	repoRoot, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return nil, err
	}
	root := strings.TrimSpace(string(repoRoot))
	relPath, err := filepath.Rel(root, filePath)
	if err != nil {
		return nil, err
	}

	// --line-porcelain outputs machine-readable format with one block per line:
	//   <sha> <orig-line> <final-line> [<num-lines>]
	//   author <name>
	//   author-mail <email>
	//   ...
	//   	<line content>
	cmd := exec.Command("git", "-C", root, "blame", "--line-porcelain",
		"-L", fmt.Sprintf("%d,%d", start, end),
		commit, "--", relPath)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git blame failed: %w", err)
	}

	// Parse porcelain output: each blamed line has an "author <name>" line
	counts := make(map[string]int)
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "author ") {
			name := strings.TrimPrefix(line, "author ")
			counts[name]++
		}
	}

	// Convert to slice and sort by line count descending
	var result []Contributor
	for name, lines := range counts {
		result = append(result, Contributor{Name: name, Lines: lines})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Lines > result[j].Lines
	})

	return result, nil
}
