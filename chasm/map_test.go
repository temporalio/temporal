package chasm

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Another approach would be to code generate string const.
func TestMapKeyTypesMatchConst(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to get current file path")
	srcFile := filepath.Join(filepath.Dir(currentFile), "map.go")

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, srcFile, nil, parser.AllErrors)
	require.NoError(t, err)

	var found string
	// Walk the top‐level declarations looking for:
	//   type Map[K ... , T any] map[K]T
	for _, decl := range file.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok || ts.Name.Name != "Map" {
				continue
			}
			// ts.TypeParams.List[0] is the field for K
			if ts.TypeParams != nil && len(ts.TypeParams.List) > 0 {
				field := ts.TypeParams.List[0]
				var buf strings.Builder
				// pretty‐print the AST node for the constraint
				err = printer.Fprint(&buf, fset, field.Type)
				require.NoError(t, err)
				found = buf.String()
			}
		}
	}

	require.NotEmpty(t, found, "could not locate Map[K …] in AST")
	require.Equal(t, mapKeyTypes, found)
}
