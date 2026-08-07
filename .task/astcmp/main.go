// astcmp emits a canonical, position-independent dump of a Go package's AST:
// every top-level declaration printed via go/printer, its doc comment, the
// union of imports, and any free-floating comments.
package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"sort"
	"strings"
)

func main() {
	dir := os.Args[1]
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, nil, parser.ParseComments)
	if err != nil {
		panic(err)
	}
	var decls []string
	imports := map[string]bool{}
	var floating []string

	for _, pkg := range pkgs {
		for _, f := range pkg.Files {
			// collect comment groups that are attached as Doc to some node
			attached := map[*ast.CommentGroup]bool{}
			ast.Inspect(f, func(n ast.Node) bool {
				switch d := n.(type) {
				case *ast.GenDecl:
					if d.Doc != nil {
						attached[d.Doc] = true
					}
				case *ast.FuncDecl:
					if d.Doc != nil {
						attached[d.Doc] = true
					}
				case *ast.Field:
					if d.Doc != nil {
						attached[d.Doc] = true
					}
					if d.Comment != nil {
						attached[d.Comment] = true
					}
				case *ast.ValueSpec:
					if d.Doc != nil {
						attached[d.Doc] = true
					}
					if d.Comment != nil {
						attached[d.Comment] = true
					}
				case *ast.TypeSpec:
					if d.Doc != nil {
						attached[d.Doc] = true
					}
					if d.Comment != nil {
						attached[d.Comment] = true
					}
				case *ast.ImportSpec:
					if d.Doc != nil {
						attached[d.Doc] = true
					}
					if d.Comment != nil {
						attached[d.Comment] = true
					}
				}
				return true
			})
			for _, cg := range f.Comments {
				if !attached[cg] {
					floating = append(floating, strings.TrimSpace(cg.Text()))
				}
			}

			for _, d := range f.Decls {
				gd, isGen := d.(*ast.GenDecl)
				if isGen && gd.Tok == token.IMPORT {
					for _, s := range gd.Specs {
						is := s.(*ast.ImportSpec)
						name := ""
						if is.Name != nil {
							name = is.Name.Name + " "
						}
						imports[name+is.Path.Value] = true
					}
					continue
				}
				var buf bytes.Buffer
				cfg := printer.Config{Mode: printer.UseSpaces | printer.TabIndent, Tabwidth: 8}
				if err := cfg.Fprint(&buf, fset, d); err != nil {
					panic(err)
				}
				doc := ""
				switch v := d.(type) {
				case *ast.FuncDecl:
					doc = v.Doc.Text()
				case *ast.GenDecl:
					doc = v.Doc.Text()
				}
				decls = append(decls, fmt.Sprintf("=== DECL %s\n--- doc:\n%s--- body:\n%s\n",
					declName(d), doc, buf.String()))
			}
		}
	}

	sort.Strings(decls)
	var imps []string
	for k := range imports {
		imps = append(imps, k)
	}
	sort.Strings(imps)
	sort.Strings(floating)

	fmt.Printf("### DECLARATIONS: %d\n", len(decls))
	for _, d := range decls {
		fmt.Print(d)
	}
	fmt.Printf("### IMPORT UNION: %d\n%s\n", len(imps), strings.Join(imps, "\n"))
	fmt.Printf("### FLOATING COMMENTS: %d\n", len(floating))
	for _, c := range floating {
		fmt.Printf("--- %s\n", c)
	}
}

func declName(d ast.Decl) string {
	switch v := d.(type) {
	case *ast.FuncDecl:
		recv := ""
		if v.Recv != nil && len(v.Recv.List) > 0 {
			var b bytes.Buffer
			printer.Fprint(&b, token.NewFileSet(), v.Recv.List[0].Type)
			recv = "(" + b.String() + ")."
		}
		return recv + v.Name.Name
	case *ast.GenDecl:
		var names []string
		for _, s := range v.Specs {
			switch sp := s.(type) {
			case *ast.TypeSpec:
				names = append(names, sp.Name.Name)
			case *ast.ValueSpec:
				for _, n := range sp.Names {
					names = append(names, n.Name)
				}
			}
		}
		return v.Tok.String() + " " + strings.Join(names, ",")
	}
	return "?"
}
