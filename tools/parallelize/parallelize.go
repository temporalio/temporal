package parallelize

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func Main() error {
	if len(os.Args) < 2 {
		return errors.New("usage: parallelize <dir> [<dir>...]")
	}

	var failed bool
	for _, dir := range os.Args[1:] {
		if err := processDir(dir); err != nil {
			fmt.Fprintf(os.Stderr, "error processing %s: %v\n", dir, err)
			failed = true
		}
	}
	if failed {
		return errors.New("some files failed to process")
	}
	return nil
}

func processDir(dir string) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		return processFile(path)
	})
}

func processFile(path string) error {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}

	// Collect line numbers where we need to insert t.Parallel().
	// Each entry is the line of the opening '{' of the test function body.
	type insertion struct {
		line      int    // line number of the '{' opening the function body
		paramName string // name of the *testing.T parameter
	}
	var insertions []insertion

	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if !isTestFunc(fn) {
			continue
		}
		paramName := testingTParamName(fn)
		if paramName == "" {
			continue
		}
		if hasParallelCall(fn.Body, paramName) {
			continue
		}
		if hasNoLintComment(fn) {
			continue
		}
		bodyLine := fset.Position(fn.Body.Lbrace).Line
		insertions = append(insertions, insertion{line: bodyLine, paramName: paramName})
	}

	if len(insertions) == 0 {
		return nil
	}

	// Sort by line descending so insertions don't shift line numbers of subsequent insertions.
	sort.Slice(insertions, func(i, j int) bool {
		return insertions[i].line > insertions[j].line
	})

	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}

	src, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	lines := strings.Split(string(src), "\n")
	for _, ins := range insertions {
		// ins.line is 1-indexed, so it conveniently equals the 0-based index
		// of the line right after '{', which is where we want to insert.
		idx := ins.line
		newLine := "\t" + ins.paramName + ".Parallel()"
		lines = append(lines[:idx+1], lines[idx:]...)
		lines[idx] = newLine
	}

	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), fi.Mode()); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}

	fmt.Printf("parallelize: %s\n", path)
	return nil
}

// isTestFunc returns true for func TestXxx(t *testing.T).
func isTestFunc(fn *ast.FuncDecl) bool {
	if fn.Recv != nil {
		return false // method, not a function
	}
	if !strings.HasPrefix(fn.Name.Name, "Test") {
		return false
	}
	if fn.Body == nil {
		return false
	}
	return testingTParamName(fn) != ""
}

// testingTParamName returns the name of the *testing.T parameter, or "" if not found.
func testingTParamName(fn *ast.FuncDecl) string {
	if fn.Type.Params == nil || len(fn.Type.Params.List) == 0 {
		return ""
	}
	for _, field := range fn.Type.Params.List {
		starExpr, ok := field.Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		selExpr, ok := starExpr.X.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		pkg, ok := selExpr.X.(*ast.Ident)
		if !ok {
			continue
		}
		if pkg.Name == "testing" && selExpr.Sel.Name == "T" {
			if len(field.Names) > 0 {
				return field.Names[0].Name
			}
		}
	}
	return ""
}

// hasNoLintComment checks for //parallelize:ignore in the function's doc comment.
func hasNoLintComment(fn *ast.FuncDecl) bool {
	if fn.Doc == nil {
		return false
	}
	for _, c := range fn.Doc.List {
		if strings.Contains(c.Text, "parallelize:ignore") {
			return true
		}
	}
	return false
}

// hasParallelCall checks if the function body already contains <param>.Parallel().
func hasParallelCall(body *ast.BlockStmt, paramName string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		if ident.Name == paramName && sel.Sel.Name == "Parallel" {
			found = true
		}
		return true
	})
	return found
}
