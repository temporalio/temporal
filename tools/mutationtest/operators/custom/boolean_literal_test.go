package custom

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBooleanLiteralMutatesAndRestoresUniverseConstants(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		source  string
		literal string
		want    string
	}{
		{name: "true", source: "package p\nvar value = true\n", literal: "true", want: "false"},
		{name: "false", source: "package p\nvar value = false\n", literal: "false", want: "true"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pkg, info, ident := typeCheckedIdent(t, tc.source, tc.literal)
			mutations := BooleanLiteral(pkg, info, ident)
			require.Len(t, mutations, 1)

			mutations[0].Change()
			require.Equal(t, tc.want, ident.Name)
			mutations[0].Reset()
			require.Equal(t, tc.literal, ident.Name, "reset must restore the original boolean identifier")
		})
	}
}

func TestBooleanLiteralIgnoresNonUniverseIdentifiers(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		source  string
		literal string
	}{
		{name: "unrelated", source: "package p\nvar value = 1\nvar other = value\n", literal: "value"},
		{name: "shadowed true", source: "package p\nfunc value(true bool) bool { return true }\n", literal: "true"},
		{name: "shadowed false", source: "package p\nfunc value(false bool) bool { return false }\n", literal: "false"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pkg, info, ident := typeCheckedIdent(t, tc.source, tc.literal)
			require.Empty(t, BooleanLiteral(pkg, info, ident))
		})
	}
}

func typeCheckedIdent(t *testing.T, source string, name string) (*types.Package, *types.Info, *ast.Ident) {
	t.Helper()

	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "fixture.go", source, 0)
	require.NoError(t, err)
	info := &types.Info{
		Defs: make(map[*ast.Ident]types.Object),
		Uses: make(map[*ast.Ident]types.Object),
	}
	pkg, err := (&types.Config{}).Check("example.com/p", fileSet, []*ast.File{file}, info)
	require.NoError(t, err)

	var found *ast.Ident
	ast.Inspect(file, func(node ast.Node) bool {
		ident, ok := node.(*ast.Ident)
		if ok && ident.Name == name && info.Uses[ident] != nil {
			found = ident
		}
		return true
	})
	require.NotNil(t, found)
	return pkg, info, found
}
