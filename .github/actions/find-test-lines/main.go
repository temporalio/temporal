package main

import (
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
	File         string        `json:"file"`
	StartLine    int           `json:"start_line"`
	EndLine      int           `json:"end_line"`
	Contributors []Contributor `json:"contributors,omitempty"`
}

type Contributor struct {
	Name  string `json:"name"`
	Lines int    `json:"lines"`
}

func main() {
	// Read configuration from environment variables
	testPath := os.Getenv("INPUT_TEST_PATH")
	commitHash := os.Getenv("INPUT_COMMIT")
	dir := os.Getenv("INPUT_DIR")
	githubOutput := os.Getenv("GITHUB_OUTPUT")

	if testPath == "" {
		fmt.Fprintln(os.Stderr, "Error: INPUT_TEST_PATH is required")
		os.Exit(1)
	}

	// Change to specified directory for package loading
	if dir != "" && dir != "." {
		if err := os.Chdir(dir); err != nil {
			fmt.Fprintf(os.Stderr, "Error changing to directory %s: %v\n", dir, err)
			os.Exit(1)
		}
	}

	// Parse test path: TestSuiteName/TestFunctionName[/SubtestName...]
	// We extract the second component as the actual function name.
	// Subtests (created via t.Run) are part of the parent function.
	parts := strings.Split(testPath, "/")
	if len(parts) < 2 {
		fmt.Fprintln(os.Stderr, "Error: expected format TestSuiteName/TestFunctionName")
		os.Exit(1)
	}
	testFunc := parts[1]

	filePath, start, end, err := findTestFunction(testFunc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error finding test: %v\n", err)
		os.Exit(1)
	}

	output := Output{
		File:      filePath,
		StartLine: start,
		EndLine:   end,
	}

	if commitHash != "" {
		contributors, err := getContributors(filePath, start, end, commitHash)
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
		// Write JSON to stdout for local testing
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(output)
	}
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

// findTestFunction loads all packages in the current module and searches for
// a function declaration matching funcName. Returns the file path and line
// bounds (start/end) of the function.
func findTestFunction(funcName string) (filePath string, start, end int, err error) {
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
		return "", 0, 0, fmt.Errorf("failed to load packages: %w", err)
	}

	// Walk the AST of each file looking for a matching function declaration.
	// pkg.Syntax contains the parsed AST for each file in the package.
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				if fn.Name.Name == funcName {
					// fn.Pos() is the start of "func", fn.End() is after closing brace
					pos := pkg.Fset.Position(fn.Pos())
					endPos := pkg.Fset.Position(fn.End())
					return pos.Filename, pos.Line, endPos.Line, nil
				}
			}
		}
	}

	return "", 0, 0, fmt.Errorf("function %s not found", funcName)
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
