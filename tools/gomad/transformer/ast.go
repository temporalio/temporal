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
	"bytes"
	"cmp"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"go/types"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"gitlab.com/stone.code/assert"
	"golang.org/x/exp/slices"
	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/imports"
)

// tip: use https://yuroyoro.github.io/goast-viewer/ to print the Go AST

var (
	simLangPkgName = "SIMAPI"
	simLibPkgName  = "SIMLIB"
	simPkgIdent    = ast.NewIdent(simLangPkgName)
	simLibPkgIdent = ast.NewIdent(simLibPkgName)
	// NOTE: wildcards apply ONLY to functions, not var/const/type etc.
	pkgReplacements = map[string]map[string]any{ // TODO: use dedicated packages instead
		"context": {
			"*":         "signature,named",
			"AfterFunc": "CtxAfterFunc",
			// except:
			"CancelFunc": false,
			"Context":    false,
		},
		"hash/maphash": {
			"Hash":     true,
			"MakeSeed": true,
		},
		"math/rand": {
			"*": "signature",
			// except:
			"Rand":   false,
			"New":    false,
			"Source": false,
		},
		"log": {
			"Printf": true,
		},
		"net": {
			"Dial":        true,
			"Pipe":        true,
			"Listen":      true,
			"ListenTCP":   true,
			"LookupAddr":  true,
			"LookupCNAME": true,
			"LookupHost":  true,
			"LookupPort":  true,
			"LookupTXT":   true,
		},
		"os": {
			"*":    "signature",
			"File": true,
			// except:
			"Pipe":            false,
			"Getpagesize":     false,
			"Interrupt":       false,
			"IsNotExist":      false,
			"IsExist":         false,
			"IsPermission":    false,
			"IsTimeout":       false,
			"IsPathSeparator": false,
			"NewSyscallError": false,
			"Signal":          false,
			"SyscallError":    false,
			"Exit":            false,
		},
		"os/signal": {
			"Notify":        true,
			"NotifyContext": true,
			"Stop":          true,
		},
		// TODO: not supported yet
		"reflect.Value": {
			"MapKeys":     true,
			"SetMapIndex": true,
		},
		"runtime": {
			"ReadMemStats": true,
		},
		"sync": {
			"Cond":       true,
			"NewCond":    true,
			"Mutex":      true,
			"Once":       true,
			"OnceFunc":   true,
			"OnceValue":  true,
			"OnceValues": true,
			"RWMutex":    true,
			"WaitGroup":  true,
		},
		"syscall": {
			"*":        "signature",
			"Read":     "SysRead",
			"Readlink": "SysReadlink",
			"Getenv":   "SysGetenv",
			"Unsetenv": "SysUnsetenv",
			// except:
			"Pipe":        false,
			"Setrlimit":   false, // set maximum system resource consumption
			"Getrlimit":   false, // get maximum system resource consumption
			"Getpagesize": false, // get memory page size
		},
		"time": {
			"After":     true,
			"AfterFunc": true,
			"Sleep":     true,
			"Now":       true,
			"NewTicker": true,
			"NewTimer":  true,
			"Since":     true,
			"Ticker":    true,
			"Tick":      true,
			"Timer":     true,
			"Until":     true,
		},
		"google.golang.org/grpc": {
			"ChainUnaryInterceptor":      true,
			"ClientConn":                 true,
			"DialOption":                 true,
			"DialContext":                true,
			"Dial":                       true,
			"Server":                     true,
			"NewClient":                  true,
			"NewServer":                  true,
			"ServerOption":               true,
			"UnaryInvoker":               true,
			"UnaryClientInterceptor":     true,
			"WithChainUnaryInterceptor":  true,
			"Creds":                      true,
			"WithChainStreamInterceptor": true,
			"ChainStreamInterceptor":     true,
			"WithDefaultServiceConfig":   true,
			"WithConnectParams":          true,
			"WithReadBufferSize":         true,
			"WithWriteBufferSize":        true,
			"WithDisableServiceConfig":   true,
			"KeepaliveEnforcementPolicy": true,
			"SetTrailer":                 true,
			"KeepaliveParams":            true,
			"WithUserAgent":              true,
			"WithTransportCredentials":   true,
			"WithBlock":                  true,
			"WithKeepaliveParams":        true,
			"WithDefaultCallOptions":     true,
			"WithAuthority":              true,
			"StreamClientInterceptor":    true,
			"Streamer":                   true,
			"WithUnaryInterceptor":       true,
		},
		"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc": {
			"WithDialOption": true,
			"WithGRPCConn":   true,
		},
		"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc": {
			"WithDialOption": true,
			"WithGRPCConn":   true,
		},
	}
)

type (
	fileTransformer struct {
		// inputs
		pkgPath            string
		fset               *token.FileSet
		file               *ast.File
		info               *types.Info
		skipTransform      bool
		importsNamesByPath map[string]string // file's imported package paths (for fast lookup)

		// temp
		mapForRanges int

		// outputs
		failpoints          []string          // discovered failpoint names
		pkgNameByImportPath map[string]string // name of package behind import path
		replacedPkgRefs     []pkgRef          // standard library package replacements
		usedSimLangPackage  bool              // whether a simulator package is used
		usedSimLibPackage   bool              // whether a simulator lib package is used
		extraImports        map[string]string // TODO: collapse other 2 into here
	}
	pkgRef struct {
		pkg, ident string
		isType     bool
		typeArgs   []ast.Expr
	}
)

func newFileTransformer(file *ast.File, pkg *packages.Package, skipTransform bool) *fileTransformer {
	importsNamesByPath := make(map[string]string, len(file.Imports))
	for _, i := range file.Imports {
		// Technically, a package can be imported with multiple names,
		// but since they all point to the same package, it doesn't matter.
		var name string
		if i.Name != nil {
			name = i.Name.Name
		}
		path, _ := strconv.Unquote(i.Path.Value)
		importsNamesByPath[path] = name
	}

	return &fileTransformer{
		pkgPath:             pkg.PkgPath,
		fset:                pkg.Fset,
		file:                file,
		info:                pkg.TypesInfo,
		skipTransform:       skipTransform,
		importsNamesByPath:  importsNamesByPath,
		pkgNameByImportPath: make(map[string]string),
		extraImports:        make(map[string]string),
	}
}

func (tf *fileTransformer) transform() (res string, retErr error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("stacktrace from panic: \n" + string(debug.Stack()))
			retErr = errors.Errorf("%v", err)
		}
	}()

	// transform
	if !tf.skipTransform {
		tf.transformAST()
	}

	tf.transformImports(tf.skipTransform)

	// render and format source code
	var buf bytes.Buffer
	if err := format.Node(&buf, tf.fset, tf.file); err != nil {
		panic(errors.Wrap(err, "error parsing transformed source for formatting"))
	}
	source, err := format.Source(buf.Bytes())
	if err != nil {
		panic(errors.Wrap(err, "error reading transformed source for formatting"))
	}

	options := &imports.Options{
		TabWidth:   8,
		TabIndent:  true,
		Comments:   true,
		Fragment:   true,
		FormatOnly: true, // too slow otherwise
	}
	source, err = imports.Process("", source, options)
	if err != nil {
		panic(errors.Wrap(err, "error formatting transformed source"))
	}

	return string(source), nil
}

// rewrite external import to point to transformed package instead
func (tf *fileTransformer) transformImports(skipTransform bool) {
	for _, imp := range tf.file.Imports {
		path := strings.Trim(imp.Path.Value, "\"")
		switch {
		case !skipTransform && (strings.HasPrefix(path, "net/http") ||
			strings.HasPrefix(path, "database/sql")):
			imp.Path.Value = fmt.Sprintf("\"%v/%v/%v\"", simPkgPrefix, simulatorApiPackage+"/ext-lib", path)
		case isStandardImportPath(path):
			// ignore
		default:
			imp.Path.Value = fmt.Sprintf("\"%v/%v\"", simPkgPrefix, path)
		}
	}
}

func (tf *fileTransformer) transformAST() {
	for _, commentGroup := range tf.file.Comments {
		var newList []*ast.Comment
		for _, comment := range commentGroup.List {
			// TODO: remove once `prometheus/client_golang` works with generics
			if strings.Contains(comment.Text, "+build go1.17") {
				comment.Text = "//"
			} else if strings.Contains(comment.Text, "go:build go1.17") {
				comment.Text = "//"
			} else if strings.Contains(comment.Text, "+build go1.9") {
				// TODO: remove once `go.opencensus.io/tag` works with generics
				comment.Text = "//"
			} else if strings.Contains(comment.Text, "go:build go1.9") {
				comment.Text = "//"
			} else if strings.HasPrefix(comment.Text, "/*") {
				// TODO: needed for SQLITE but breaks other packages
				if tf.file.Name.Name == "sqlite3" || tf.file.Name.Name == "fixtures" {
					// ignore block comment as it messes with our replacements
					continue
				}
			}
			newList = append(newList, comment)
		}
		commentGroup.List = newList
	}

	astutil.Apply(tf.file, nil, func(c *astutil.Cursor) bool {
		switch node := c.Node().(type) {
		// expressions
		case *ast.UnaryExpr:
			tf.transformUnaryExpr(c, node)

		// declarations / specs
		case *ast.Field:
			node.Type = tf.transformExpr(node.Type)
		case *ast.GenDecl:
			tf.transformDecl(node)
		case *ast.TypeSpec:
			tf.transformTypeSpec(node)

		// statements
		case *ast.ExprStmt:
			node.X = tf.transformExpr(node.X)
		case *ast.GoStmt:
			tf.transformGoroutineCall(c, node)
		case *ast.DeferStmt:
			node.Call = tf.transformExpr(node.Call).(*ast.CallExpr)
		case *ast.ReturnStmt:
			node.Results = tf.transformExprs(node.Results)
		case *ast.IfStmt:
			node.Cond = tf.transformExpr(node.Cond)
		case *ast.SendStmt:
			tf.transformChannelSend(c)
		case *ast.CompositeLit:
			tf.transformExpr(node.Type)
			tf.transformExprs(node.Elts)
			if lit, isMapLiteral := node.Type.(*ast.MapType); isMapLiteral {
				tf.transformMapLiteral(c, node, lit, false)
			}
		case *ast.AssignStmt:
			tf.transformMapAssignment(c, node)
			node.Rhs = tf.transformExprs(node.Rhs)
		case *ast.RangeStmt:
			tf.transformRange(c, node)
		case *ast.SelectStmt:
			tf.transformSelect(c, node)
		case *ast.ForStmt:
			node.Cond = tf.transformExpr(node.Cond)
		case *ast.FuncDecl:
			tf.transformFunc(node)
		}

		return true
	})

	// add simulator package import, when needed
	if tf.usedSimLangPackage {
		astutil.AddNamedImport(tf.fset, tf.file, simLangPkgName, simulatorLangPackage)
	}
	if tf.usedSimLibPackage || len(tf.failpoints) > 0 {
		astutil.AddNamedImport(tf.fset, tf.file, simLibPkgName, simulatorLibPackage)
	}
	for alias, path := range tf.extraImports {
		astutil.AddNamedImport(tf.fset, tf.file, alias, path)
	}

	// register failpoints
	for _, fpname := range tf.failpoints {
		tf.file.Decls = append(tf.file.Decls, &ast.GenDecl{
			Tok: token.VAR,
			Specs: []ast.Spec{
				&ast.ValueSpec{
					Names: []*ast.Ident{{Name: "_"}},
					Values: []ast.Expr{&ast.CallExpr{
						Fun:  tf.newSimLibExpr("RegisterFailpoint"),
						Args: []ast.Expr{&ast.BasicLit{Value: fpname}}},
					},
				},
			},
		})
	}

	// add replaced imports (to avoid "unused import" compilation errors)
	dedup := map[string]struct{}{}
	slices.SortFunc(tf.replacedPkgRefs, func(a, b pkgRef) int {
		res := cmp.Compare(a.pkg, b.pkg)
		if res == 0 {
			res = cmp.Compare(a.ident, b.ident)
		}
		return res
	})
	for _, ref := range tf.replacedPkgRefs {
		if len(ref.typeArgs) == 0 { // ignore `typeArgs`, no need to bother!
			dedupKey := fmt.Sprintf("%v.%v", ref.pkg, ref.ident)
			if _, ok := dedup[dedupKey]; ok {
				continue
			}
			dedup[dedupKey] = struct{}{}
		}

		tok := token.VAR
		if ref.isType {
			tok = token.TYPE
		}

		var valExpr ast.Expr = &ast.SelectorExpr{
			X:   ast.NewIdent(ref.pkg),
			Sel: ast.NewIdent(ref.ident),
		}
		if len(ref.typeArgs) > 0 {
			valExpr = &ast.IndexListExpr{
				X:       valExpr,
				Indices: ref.typeArgs,
			}
		}

		tf.file.Decls = append(tf.file.Decls,
			&ast.GenDecl{
				Tok: tok,
				Specs: []ast.Spec{
					&ast.ValueSpec{
						Names:  []*ast.Ident{{Name: "_"}},
						Values: []ast.Expr{valExpr},
					},
				},
			})
	}
}

func (tf *fileTransformer) transformFunc(node *ast.FuncDecl) {
	if node.Body == nil || len(node.Body.List) == 0 {
		return
	}
	stmts := []ast.Stmt{
		&ast.ExprStmt{
			X: &ast.CallExpr{
				Fun:  tf.newSimExpr("FuncStart"),
				Args: []ast.Expr{},
			},
		},
	}
	stmts = append(stmts, node.Body.List...)
	node.Body.List = stmts
	node.Type = tf.transformExpr(node.Type).(*ast.FuncType)
}

func (tf *fileTransformer) transformTypeSpec(node *ast.TypeSpec) {
	node.Type = tf.transformExpr(node.Type)
	node.TypeParams = tf.transformFieldList(node.TypeParams)
}

func (tf *fileTransformer) transformSelect(c *astutil.Cursor, selectStmt *ast.SelectStmt) {
	var switchCases []ast.Stmt
	var initArgs []ast.Expr
	var hasDefaultCase bool
	var defaultCaseBody []ast.Stmt

	// NOTE: default case can be at the top
loop:
	for _, stmt := range selectStmt.Body.List {
		clause := stmt.(*ast.CommClause)
		if clause.Comm == nil {
			defaultCaseBody = clause.Body
			hasDefaultCase = true
			continue loop
		}

		// switch case
		switchCase := &ast.CaseClause{
			List: []ast.Expr{&ast.BasicLit{Value: fmt.Sprintf("%v", len(switchCases))}},
			Body: clause.Body,
		}

		// switch case body
		var channelCallExpr *ast.CallExpr
		switch commStmt := clause.Comm.(type) {
		case *ast.AssignStmt:
			var stmts []ast.Stmt
			channelCallExpr = commStmt.Rhs[0].(*ast.CallExpr)

			// rcv value assignment
			if identExpr, ok := commStmt.Lhs[0].(*ast.Ident); ok && identExpr.Name != "_" {
				chanType := tf.info.TypeOf(commStmt.Lhs[0])
				typeExpr := tf.typeToAstExpr(chanType)

				// if it's a new variable, define it
				if commStmt.Tok == token.DEFINE {
					stmts = append(stmts,
						&ast.DeclStmt{
							Decl: &ast.GenDecl{
								Tok: token.VAR,
								Specs: []ast.Spec{
									&ast.ValueSpec{
										Names: []*ast.Ident{identExpr},
										Type:  typeExpr,
									},
								},
							},
						},
					)
				}

				// NOTE: this used to be a type assertion, but there was an edge case where the type assertion fails
				// to compile when there is a variable with the same name as the type
				stmts = append(stmts,
					&ast.ExprStmt{
						X: &ast.CallExpr{
							Fun: &ast.SelectorExpr{
								X:   ast.NewIdent("selector"),
								Sel: ast.NewIdent("Assign"),
							},
							Args: []ast.Expr{&ast.UnaryExpr{
								Op: token.AND,
								X:  identExpr,
							}},
						},
					})
			}

			// rcv ok assignment
			if len(commStmt.Lhs) > 1 {
				stmts = append(stmts, &ast.AssignStmt{
					Lhs: []ast.Expr{commStmt.Lhs[1].(*ast.Ident)},
					Tok: commStmt.Tok,
					Rhs: []ast.Expr{
						&ast.SelectorExpr{
							X:   ast.NewIdent("selector"),
							Sel: ast.NewIdent("Ok"),
						},
					},
				})
			}

			switchCase.Body = append(
				stmts,
				switchCase.Body...,
			)
		case *ast.ExprStmt:
			channelCallExpr = commStmt.X.(*ast.CallExpr)
		}

		// Selector args
		if channelCallExpr != nil {
			selExpr := channelCallExpr.Fun.(*ast.SelectorExpr)
			assert.Assert(selExpr.X == simPkgIdent, "expected `gomad` package")

			// channel operation (rcv or snd)
			var chanOp int
			if selExpr.Sel.Name == "ChanSend" {
				chanOp = 1
			} else {
				chanOp = 0
				selExpr.Sel.Name = "ChanRcvOk" // simplifies generated code
			}
			initArgs = append(initArgs, &ast.BasicLit{Value: fmt.Sprintf("%d", chanOp)})

			// channel expr
			var channelExpr = channelCallExpr.Args[0]
			if chanOp == 1 { // `snd`
				initArgs = append(initArgs,
					&ast.CallExpr{Fun: tf.newSimExpr("SndChan"), Args: []ast.Expr{channelExpr}})
			} else {
				initArgs = append(initArgs,
					&ast.CallExpr{Fun: tf.newSimExpr("RcvChan"), Args: []ast.Expr{channelExpr}})
			}

			// channel send func var
			if chanOp == 1 { // `snd`
				initArgs = append(initArgs, &ast.FuncLit{
					Type: &ast.FuncType{
						Results: &ast.FieldList{
							List: []*ast.Field{{Type: ast.NewIdent("any")}},
						},
					},
					Body: &ast.BlockStmt{
						List: []ast.Stmt{
							&ast.ReturnStmt{
								Results: channelCallExpr.Args[1:],
							},
						},
					},
				})
			} else {
				initArgs = append(initArgs, ast.NewIdent("nil"))
			}
		}

		switchCases = append(switchCases, switchCase)
	}

	// default switch case
	if hasDefaultCase {
		initArgs = append(initArgs, ast.NewIdent("nil"))
	} else {
		defaultCaseBody = []ast.Stmt{
			&ast.ExprStmt{
				X: &ast.CallExpr{
					Fun:  ast.NewIdent("panic"),
					Args: []ast.Expr{&ast.BasicLit{Value: "\"unreachable\"", Kind: token.STRING}},
				},
			},
		}
	}
	switchCases = append(switchCases, &ast.CaseClause{List: nil, Body: defaultCaseBody})

	c.Replace(&ast.SwitchStmt{
		Init: &ast.AssignStmt{
			Lhs: []ast.Expr{ast.NewIdent("selector")},
			Rhs: []ast.Expr{
				&ast.CallExpr{
					Fun:  tf.newSimExpr("Select"),
					Args: initArgs,
				},
			},
			Tok: token.DEFINE,
		},
		Tag: &ast.SelectorExpr{
			X:   ast.NewIdent("selector"),
			Sel: ast.NewIdent("Case"),
		},
		Body: &ast.BlockStmt{
			List: switchCases,
		},
	})
}

func (tf *fileTransformer) transformDecl(node *ast.GenDecl) {
	if node.Tok != token.VAR {
		return
	}

	newSpecs := make([]ast.Spec, len(node.Specs))
	for i, spec := range node.Specs {
		newSpecs[i] = spec
		if valSpec, ok := spec.(*ast.ValueSpec); ok {
			valSpec.Type = tf.transformExpr(valSpec.Type)
			for j, val := range valSpec.Values {
				valExpr := tf.transformExpr(val)

				// wrap function calls
				//if _, ok := valExpr.(*ast.CallExpr); ok {
				//	var results []*ast.Field
				//	valType := tf.info.TypeOf(val)
				//	if tupleType, ok := valType.(*types.Tuple); ok {
				//		for k := 0; k < tupleType.Len(); k += 1 {
				//			results = append(results, &ast.Field{
				//				Type: tf.typeToAstExpr(tupleType.At(k).Type()),
				//			})
				//		}
				//	} else {
				//		results = append(results, &ast.Field{
				//			Type: tf.typeToAstExpr(valType),
				//		})
				//	}
				//
				//	valExpr = &ast.CallExpr{
				//		Fun: tf.newSimExpr(fmt.Sprintf("VarInit%d", len(results))),
				//		Args: []ast.Expr{
				//			&ast.FuncLit{
				//				Type: &ast.FuncType{
				//					Params:  &ast.FieldList{},
				//					Results: &ast.FieldList{List: results},
				//				},
				//				Body: &ast.BlockStmt{
				//					List: []ast.Stmt{
				//						&ast.ReturnStmt{Results: []ast.Expr{valExpr}},
				//					},
				//				},
				//			},
				//		},
				//	}
				//}

				valSpec.Values[j] = valExpr
			}
		}
	}
}

func (tf *fileTransformer) transformRange(c *astutil.Cursor, node *ast.RangeStmt) {
	rangeExprType := tf.info.TypeOf(node.X)
	if rangeExprType == nil {
		panic(fmt.Sprintf("unknown type in for-range: %T", node.X))
	}

	// HACK: the range object was already re-written!
	//	switch rangeObj := callExpr.Fun.(type) {
	//	//case *ast.SelectorExpr:
	//	//	if rangeObj.X.(*ast.Ident).Name == simLangPkgName && rangeObj.Sel.Name == "RandPerm" {
	//	//		return
	//	//	}
	//	//case *ast.IndexListExpr:
	//	//	if rangeObj.X.(*ast.Ident).Name == simLangPkgName && rangeObj. == "MapInit" {
	//	//	return
	//	//}
	//}

	keyIdent := node.Key

	underlyingType := rangeExprType.Underlying()
	switch underlyingType.(type) {
	case *types.Chan:
		if keyIdent == nil {
			keyIdent = ast.NewIdent("_")
		}
		c.Replace(&ast.ForStmt{
			Body: &ast.BlockStmt{
				List: append([]ast.Stmt{
					&ast.AssignStmt{
						Lhs: []ast.Expr{
							keyIdent,
							ast.NewIdent("ok"),
						},
						Rhs: []ast.Expr{
							&ast.CallExpr{
								Fun: tf.newSimExpr("ChanRcvOk"),
								Args: []ast.Expr{
									node.X,
								},
							},
						},
						Tok: token.DEFINE,
					},
					&ast.IfStmt{
						Cond: &ast.UnaryExpr{
							Op: token.NOT,
							X:  ast.NewIdent("ok"),
						},
						Body: &ast.BlockStmt{
							List: []ast.Stmt{
								&ast.BranchStmt{
									Tok: token.BREAK,
								},
							},
						},
					},
				}, node.Body.List...),
			},
		})
	case *types.Map:

		// find out if key and/or value of the map are used
		valIdent := node.Value
		if nilOrWildcardIdent(keyIdent) && nilOrWildcardIdent(valIdent) {
			valIdent = ast.NewIdent("_")
			keyIdent = ast.NewIdent("_")
		} else if nilOrWildcardIdent(keyIdent) {
			keyIdent = ast.NewIdent("SIMRangeKey")
		}

		// identify assignment operator
		assignOp := node.Tok
		if nilOrWildcardIdent(keyIdent) && nilOrWildcardIdent(valIdent) {
			assignOp = token.ASSIGN
		}

		var mapIdent = node.X
		if !nilOrWildcardIdent(valIdent) {
			_, isCallExpr := node.X.(*ast.CallExpr)
			_, isLiteralExpr := node.X.(*ast.CompositeLit)
			hasNameCollision := identName(keyIdent) == identName(node.X)
			if isCallExpr || isLiteralExpr || hasNameCollision {
				// introduce temporary variable
				ident := fmt.Sprintf("SIMRangeSrc%v", tf.mapForRanges)
				mapIdent = ast.NewIdent(ident)
				c.InsertBefore(&ast.AssignStmt{
					Lhs: []ast.Expr{mapIdent},
					Rhs: []ast.Expr{node.X},
					Tok: token.DEFINE,
				})
				tf.mapForRanges += 1
			}

			// prepend map value assignment to for-range's body
			node.Body = &ast.BlockStmt{
				List: []ast.Stmt{
					&ast.AssignStmt{
						Lhs: []ast.Expr{valIdent},
						Rhs: []ast.Expr{&ast.IndexExpr{X: mapIdent, Index: keyIdent}},
						Tok: assignOp,
					},
					&ast.BlockStmt{List: node.Body.List},
				},
			}
		}

		c.Replace(&ast.RangeStmt{
			Key:   ast.NewIdent("_"),
			Value: keyIdent, // the map key identifier becomes the range's value identifier
			Body:  node.Body,
			X: &ast.CallExpr{
				Fun:  tf.newSimExpr("MapKeys"),
				Args: []ast.Expr{mapIdent},
			},
			Tok: assignOp,
		})
	}
}

func (tf *fileTransformer) transformChannelSend(c *astutil.Cursor) {
	send, ok := c.Node().(*ast.SendStmt)
	if !ok {
		return
	}

	c.Replace(&ast.ExprStmt{
		X: &ast.CallExpr{
			Fun: tf.newSimExpr("ChanSend"),
			Args: []ast.Expr{
				send.Chan,
				send.Value,
			},
		},
	})
}

func (tf *fileTransformer) transformUnaryExpr(c *astutil.Cursor, node *ast.UnaryExpr) {
	switch node.Op {
	case token.ARROW:
		simFunc := "ChanRcv"
		if assignStmt, ok := c.Parent().(*ast.AssignStmt); ok {
			if len(assignStmt.Lhs) == 2 {
				simFunc = "ChanRcvOk"
			}
		}
		c.Replace(&ast.CallExpr{
			Fun:  tf.newSimExpr(simFunc),
			Args: []ast.Expr{node.X},
		})
		return

	case token.AND:
		if callExpr, ok := node.X.(*ast.CallExpr); ok {
			if idxListExpr, ok := callExpr.Fun.(*ast.IndexListExpr); ok {
				if selExpr, ok := idxListExpr.X.(*ast.SelectorExpr); ok {
					if selExpr.Sel.Name == "MapInit" {
						selExpr.Sel.Name = "MapInitPtr"
						c.Replace(callExpr)
					}
				}
			}
		}
	}

	node.X = tf.transformExpr(node.X)
	return
}

func (tf *fileTransformer) transformGoroutineCall(c *astutil.Cursor, node *ast.GoStmt) {
	c.Replace(&ast.ExprStmt{
		X: &ast.CallExpr{
			Fun: tf.newSimExpr("Go"),
			Args: []ast.Expr{
				&ast.FuncLit{
					Type: &ast.FuncType{},
					Body: &ast.BlockStmt{
						List: []ast.Stmt{
							&ast.ExprStmt{
								X: node.Call,
							},
						},
					},
				},
			},
		},
	})
}

func (tf *fileTransformer) transformExpr(node ast.Expr) ast.Expr {
	if node == nil {
		return node
	}

	switch t := node.(type) {
	case *ast.StarExpr:
		return &ast.StarExpr{
			X: tf.transformExpr(t.X),
		}
	case *ast.BinaryExpr:
		return &ast.BinaryExpr{
			Op: t.Op,
			X:  tf.transformExpr(t.X),
			Y:  tf.transformExpr(t.Y),
		}
	case *ast.CallExpr:
		switch fn := t.Fun.(type) {
		case *ast.Ident:
			if fn.Obj == nil {
				switch fn.Name {
				case "close":
					return &ast.CallExpr{
						Fun:  tf.newSimExpr("ChanClose"),
						Args: tf.transformExprs(t.Args),
					}
				default:
					// fallthrough
				}
			}
		case *ast.SelectorExpr:
			var isPtr bool
			typeOf := tf.info.TypeOf(fn.X)
			if ptr, ok := typeOf.(*types.Pointer); ok {
				typeOf = ptr.Elem()
				isPtr = true
			}

			if _, ok := typeOf.(*types.Named); ok {
				switch typeOf.String() {
				//case "net/http.Client":
				//	fn.Sel.Name = "Client_" + fn.Sel.Name
				//	if isPtr {
				//		t.Args = append(
				//			[]ast.Expr{fn.X},
				//			t.Args...)
				//	} else {
				//		t.Args = append(
				//			[]ast.Expr{&ast.UnaryExpr{Op: token.AND, X: fn.X}},
				//			t.Args...)
				//	}
				//	fn.X = ast.NewIdent(simLibPkgName)
				//	tf.usedSimLibPackage = true
				case "net.Dialer":
					fn.Sel.Name = "Dialer_" + fn.Sel.Name
					if isPtr {
						t.Args = append(
							[]ast.Expr{fn.X},
							t.Args...)
					} else {
						t.Args = append(
							[]ast.Expr{&ast.UnaryExpr{Op: token.AND, X: fn.X}},
							t.Args...)
					}
					fn.X = ast.NewIdent(simLibPkgName)
					tf.usedSimLibPackage = true
				case "reflect.Value":
					if fn.Sel.Name == "SetMapIndex" || fn.Sel.Name == "MapKeys" {
						t.Args = append([]ast.Expr{fn.X}, t.Args...)
						fn.X = ast.NewIdent(simLibPkgName)
						tf.usedSimLibPackage = true
					}
				}
			}

			switch x := fn.X.(type) {
			case *ast.Ident:
				if x.Name == "failpoint" {
					switch fn.Sel.Name {
					case "Inject":
						tf.failpoints = append(tf.failpoints, t.Args[0].(*ast.BasicLit).Value)
					case "InjectContext":
						tf.failpoints = append(tf.failpoints, t.Args[1].(*ast.BasicLit).Value)
					}
				}
			}
		}
		return &ast.CallExpr{
			Fun:      tf.transformExpr(t.Fun),
			Args:     tf.transformExprs(t.Args),
			Ellipsis: t.Ellipsis,
		}
	case *ast.ArrayType:
		return &ast.ArrayType{
			Elt: tf.transformExpr(t.Elt),
			Len: t.Len,
		}
	case *ast.KeyValueExpr:
		return &ast.KeyValueExpr{
			Key:   t.Key,
			Value: tf.transformExpr(t.Value),
		}
	case *ast.FuncType:
		return &ast.FuncType{
			TypeParams: tf.transformFieldList(t.TypeParams),
			Params:     tf.transformFieldList(t.Params),
			Results:    tf.transformFieldList(t.Results),
		}
	case *ast.CompositeLit:
		return &ast.CompositeLit{
			Type:       tf.transformExpr(t.Type),
			Elts:       tf.transformExprs(t.Elts),
			Incomplete: t.Incomplete,
		}
	case *ast.TypeAssertExpr:
		return &ast.TypeAssertExpr{
			X:    t.X,
			Type: tf.transformExpr(t.Type),
		}
	case *ast.SelectorExpr:
		return tf.transformSelExpr(t, nil)
	case *ast.StructType:
		return &ast.StructType{
			Fields:     tf.transformFieldList(t.Fields),
			Incomplete: t.Incomplete,
		}
	case *ast.IndexExpr:
		return &ast.IndexExpr{
			X:     tf.transformExpr(t.X),
			Index: tf.transformExpr(t.Index),
		}
	case *ast.Ellipsis:
		return &ast.Ellipsis{
			Elt: tf.transformExpr(t.Elt),
		}
	case *ast.ParenExpr:
		return &ast.ParenExpr{
			X: tf.transformExpr(t.X),
		}
	case *ast.InterfaceType:
		return &ast.InterfaceType{
			Methods:    tf.transformFieldList(t.Methods),
			Incomplete: t.Incomplete,
		}
	case *ast.FuncLit:
		return &ast.FuncLit{
			Type: tf.transformExpr(t.Type).(*ast.FuncType),
			Body: t.Body,
		}
	case *ast.IndexListExpr:
		var x ast.Expr
		switch tt := t.X.(type) {
		case *ast.SelectorExpr:
			x = tf.transformSelExpr(tt, t.Indices)
		default:
			x = tf.transformExpr(t.X)
		}
		return &ast.IndexListExpr{X: x, Indices: tf.transformExprs(t.Indices)}
	case *ast.MapType:
		return &ast.MapType{
			Key:   tf.transformExpr(t.Key),
			Value: tf.transformExpr(t.Value),
		}
	case *ast.ChanType:
		// HACK
		// cannot cast channel messages to alias; we need to resolve the alias here
		if ident, ok := t.Value.(*ast.Ident); ok {
			if obj := ident.Obj; obj != nil && obj.Kind == ast.Typ {
				if ts, ok := obj.Decl.(*ast.TypeSpec); ok {
					if _, ok := ts.Type.(*ast.FuncType); ok {
						t.Value = ts.Type
					}
				}
			}
		}
	case *ast.Ident,
		*ast.BasicLit,
		*ast.UnaryExpr,
		*ast.SliceExpr:
		// ignored
	default:
		panic(fmt.Sprintf("unsupported expr node type %T", t))
	}
	return node
}

func (tf *fileTransformer) transformSelExpr(t *ast.SelectorExpr, typeArgs []ast.Expr) ast.Expr {
	base, ok := t.X.(*ast.Ident)
	if !ok {
		return &ast.SelectorExpr{
			X:   tf.transformExpr(t.X),
			Sel: t.Sel,
		}
	}

	pkgType, ok := tf.info.Uses[base].(*types.PkgName)
	if !ok {
		return t
	}

	pkgPath := pkgType.Imported().Path()
	pkgName := pkgType.Name()
	tf.pkgNameByImportPath[pkgPath] = pkgName

	replacements, ok := pkgReplacements[pkgPath]
	if !ok {
		return t
	}

	// find reference replacement
	identifier := t.Sel.Name
	alias, defined := replacements[identifier]
	if defined {
		if alias == false {
			return t // explicitly disabled
		} else if s, ok := alias.(string); ok {
			identifier = s
		}
	} else {
		if included, wildcard := replacements["*"]; wildcard {
			includedStr := included.(string)
			typeOf := tf.info.TypeOf(t.Sel)
			switch typeOf.(type) {
			case *types.Signature:
				if !strings.Contains(includedStr, "signature") {
					return t
				}
			case *types.Named:
				if !strings.Contains(includedStr, "named") {
					return t
				}
			default:
				return t
			}
		} else {
			return t // no wildcard -> ignore!
		}
	}

	// record original reference
	// TODO: can we omit this entirely?
	ref := pkgRef{pkg: pkgName, ident: t.Sel.Name, typeArgs: typeArgs}
	refType := tf.info.TypeOf(t)
	switch refType.Underlying().(type) {
	case *types.Interface, *types.Struct:
		ref.isType = true
	}
	// TODO
	if pkgName == "context" && (t.Sel.Name == "DeadlineExceeded" || t.Sel.Name == "Canceled") {
		ref.isType = false
	}

	// TODO: fix special case
	var res ast.Expr
	if pkgPath == "google.golang.org/grpc" {
		if !strings.HasPrefix(tf.pkgPath, "go.temporal") &&
			!strings.HasPrefix(tf.pkgPath, "github.com/grpc-ecosystem/go-grpc-middleware/retry") &&
			!strings.HasPrefix(tf.pkgPath, "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc") {
			// other libraries rely on these types
			return t
		}
		// TODO: how do you check if sth is a type alias?
		if identifier == "UnaryClientInterceptor" || identifier == "UnaryInvoker" {
			ref.isType = true
		}
		if identifier == "StreamClientInterceptor" || identifier == "Streamer" {
			ref.isType = true
		}
		res = &ast.SelectorExpr{
			X:   ast.NewIdent("fakegrpc"),
			Sel: ast.NewIdent(identifier),
		}
		tf.extraImports["fakegrpc"] = "go.temporal.io/server/tools/gomad/api/ext-lib/fakegprc"
	} else if pkgPath == "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc" {
		res = &ast.SelectorExpr{
			X:   ast.NewIdent("fakeotlptrace"),
			Sel: ast.NewIdent(identifier),
		}
		tf.extraImports["fakeotlptrace"] = "go.temporal.io/server/tools/gomad/api/ext-lib/fakegprc/otlptrace"
	} else if pkgPath == "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc" {
		res = &ast.SelectorExpr{
			X:   ast.NewIdent("fakeotlpmetric"),
			Sel: ast.NewIdent(identifier),
		}
		tf.extraImports["fakeotlpmetric"] = "go.temporal.io/server/tools/gomad/api/ext-lib/fakegprc/otlpmetric"
	} else {
		if pkgName == "sync" {
			if len(typeArgs) == 0 {
				if t.Sel.Name == "OnceValue" {
					ref.typeArgs = []ast.Expr{ast.NewIdent("any")}
				} else if t.Sel.Name == "OnceValues" {
					ref.typeArgs = []ast.Expr{ast.NewIdent("any"), ast.NewIdent("any")}
				}
			}
		}
		res = tf.newSimLibExpr(identifier)
	}

	tf.replacedPkgRefs = append(tf.replacedPkgRefs, ref)
	return res
}

func (tf *fileTransformer) transformMapLiteral(c *astutil.Cursor, node *ast.CompositeLit, litType *ast.MapType, pointer bool) {
	var annotateWithType func(expr ast.Expr, typeExpr ast.Expr) ast.Expr
	annotateWithType = func(expr ast.Expr, typeExpr ast.Expr) ast.Expr {
		switch n := expr.(type) {
		case *ast.CompositeLit:
			switch t := typeExpr.(type) {
			case *ast.InterfaceType:
				// ignore
			case *ast.StarExpr:
				expr = &ast.UnaryExpr{X: n, Op: token.AND}
				n.Type = t.X
			default:
				n.Type = typeExpr
			}
		default:
			expr = &ast.CallExpr{
				Fun:  typeExpr,
				Args: []ast.Expr{expr},
			}
		}
		return expr
	}

	var args []ast.Expr
	for _, expr := range node.Elts {
		keyValExpr := expr.(*ast.KeyValueExpr)
		args = append(args,
			annotateWithType(keyValExpr.Key, litType.Key),
			annotateWithType(keyValExpr.Value, litType.Value))
	}

	funcName := "MapInit"
	if pointer {
		funcName = "MapInitPtr"
	}

	newNode := &ast.CallExpr{
		Fun: &ast.IndexListExpr{
			X:       tf.newSimExpr(funcName),
			Indices: []ast.Expr{litType.Key, litType.Value},
		},
		Args: args,
	}

	c.Replace(newNode)
	tf.info.Types[newNode] = tf.info.Types[node]
}

func (tf *fileTransformer) transformMapAssignment(c *astutil.Cursor, assign *ast.AssignStmt) {
	for _, lhs := range assign.Lhs {
		if indexExpr, ok := lhs.(*ast.IndexExpr); ok {
			mapKeyExpr := indexExpr.Index
			if _, ok := c.Parent().(*ast.IfStmt); ok {
				continue
			}

			c.InsertAfter(&ast.ExprStmt{
				X: &ast.CallExpr{
					Fun:  tf.newSimExpr("MapKey"),
					Args: []ast.Expr{mapKeyExpr},
				},
			})

			//mapKeyExpr := indexExpr.Index
			//mapKeyType := tf.typeToAstExpr(tf.info.TypeOf(mapKeyExpr))
			//
			//// TODO: remove these terrible hacks
			//if ident, ok := indexExpr.X.(*ast.SelectorExpr); ok {
			//	if ident.Sel.Name == "float64" {
			//		mapKeyType = ast.NewIdent("observablID[float64]")
			//	} else if ident.Sel.Name == "int64" {
			//		mapKeyType = ast.NewIdent("observablID[int64]")
			//	} else if ident.Sel.Name == "rateLimiters" {
			//		return
			//	} else if ident.Sel.Name == "byNum" {
			//		return
			//	}
			//}
			//if ident, ok := indexExpr.X.(*ast.IndexExpr); ok {
			//	if ident, ok := ident.X.(*ast.SelectorExpr); ok {
			//		if ident.Sel.Name == "extensionsByMessage" {
			//			return
			//		}
			//	}
			//}
			//if ident, ok := indexExpr.X.(*ast.Ident); ok {
			//	if ident.Name == "Number" {
			//		return
			//	}
			//}
			//
			//// TODO: something like this?
			////switch x := mapKeyExpr.(type) {
			////case *ast.CompositeLit:
			////	mapKeyType =
			////default:
			////	mapKeyType = tf.typeToAstExpr(tf.info.TypeOf(mapKeyExpr))
			////}
			//
			//indexExpr.Index = &ast.CallExpr{
			//	Fun: &ast.IndexExpr{
			//		X:     tf.newSimExpr("MapKey"),
			//		Index: mapKeyType,
			//	},
			//	Args: tf.transformExprs([]ast.Expr{mapKeyExpr}),
			//}
		}
	}
}

func (tf *fileTransformer) transformExprs(nodes []ast.Expr) []ast.Expr {
	res := make([]ast.Expr, len(nodes))
	for i, node := range nodes {
		res[i] = tf.transformExpr(node)
	}
	return res
}

func (tf *fileTransformer) transformFieldList(fields *ast.FieldList) *ast.FieldList {
	if fields == nil || fields.List == nil {
		return fields
	}
	res := &ast.FieldList{List: make([]*ast.Field, len(fields.List))}
	for i, f := range fields.List {
		res.List[i] = &ast.Field{
			Names: f.Names,
			Type:  tf.transformExpr(f.Type),
			Tag:   f.Tag,
		}
	}
	return res
}

func (tf *fileTransformer) newSimExpr(funcName string) *ast.SelectorExpr {
	tf.usedSimLangPackage = true
	return &ast.SelectorExpr{
		X:   simPkgIdent,
		Sel: ast.NewIdent(funcName),
	}
}

func (tf *fileTransformer) newSimLibExpr(funcName string) *ast.SelectorExpr {
	tf.usedSimLibPackage = true
	return &ast.SelectorExpr{
		X:   simLibPkgIdent,
		Sel: ast.NewIdent(funcName),
	}
}

func (tf *fileTransformer) typeToAstExpr(t types.Type) ast.Expr {
	switch t := t.(type) {
	case *types.Chan:
		return &ast.ChanType{Dir: 3, Value: tf.typeToAstExpr(t.Elem())}
	case *types.Basic:
		return ast.NewIdent(t.String())
	case *types.Signature:
		var typeParamList *ast.FieldList
		typeParams := make([]*ast.Field, t.TypeParams().Len())
		for i := 0; i < t.TypeParams().Len(); i++ {
			typeParam := t.TypeParams().At(i)
			field := &ast.Field{
				Names: []*ast.Ident{ast.NewIdent(typeParam.Obj().Name())},
				Type:  tf.typeToAstExpr(typeParam.Obj().Type()),
			}
			typeParams[i] = field
		}
		if len(typeParams) > 0 {
			typeParamList = &ast.FieldList{List: typeParams}
		}

		params := make([]*ast.Field, t.Params().Len())
		for i := 0; i < t.Params().Len(); i++ {
			param := t.Params().At(i)
			field := &ast.Field{
				Names: []*ast.Ident{ast.NewIdent(param.Name())},
				Type:  tf.typeToAstExpr(param.Type()),
			}
			params[i] = field
		}

		var resultList *ast.FieldList
		results := make([]*ast.Field, t.Results().Len())
		for i := 0; i < t.Results().Len(); i++ {
			result := t.Results().At(i)
			field := &ast.Field{
				Names: []*ast.Ident{ast.NewIdent(result.Name())},
				Type:  tf.typeToAstExpr(result.Type()),
			}
			results[i] = field
		}
		if len(results) > 0 {
			resultList = &ast.FieldList{List: results}
		}

		return &ast.FuncType{
			TypeParams: typeParamList,
			Params:     &ast.FieldList{List: params}, // never nil
			Results:    resultList,
		}
	case *types.Struct:
		fields := make([]*ast.Field, t.NumFields())
		for i := 0; i < t.NumFields(); i++ {
			field := &ast.Field{
				Names: []*ast.Ident{ast.NewIdent(t.Field(i).Name())},
				Type:  tf.typeToAstExpr(t.Field(i).Type()),
			}
			fields[i] = field
		}
		return &ast.StructType{Fields: &ast.FieldList{List: fields}}
	case *types.Named:
		objName := ast.NewIdent(t.Obj().Name())

		// add type parameters, if defined
		withTypeParams := func(expr ast.Expr) ast.Expr {
			if t.TypeParams() == nil {
				return expr
			}

			indices := make([]ast.Expr, t.TypeParams().Len())
			for i := 0; i < t.TypeParams().Len(); i++ {
				typeParam := t.TypeParams().At(i)
				indices[i] = ast.NewIdent(typeParam.Obj().Name())
			}

			return &ast.IndexListExpr{
				X:       expr,
				Indices: indices,
			}
		}

		// no package?
		pkg := t.Obj().Pkg()
		if pkg == nil {
			return withTypeParams(objName)
		}

		var pkgIdentifier = pkg.Name()
		pkgName, imported := tf.importsNamesByPath[pkg.Path()]
		if !imported {
			return withTypeParams(objName) // prevents self-referential import
		}
		if pkgName != "" {
			pkgIdentifier = pkgName // use custom import identifier, when provided
		}

		return withTypeParams(&ast.SelectorExpr{
			X:   ast.NewIdent(pkgIdentifier),
			Sel: objName,
		})
	case *types.Slice:
		return &ast.ArrayType{Elt: tf.typeToAstExpr(t.Elem())}
	case *types.Array:
		return &ast.ArrayType{Elt: tf.typeToAstExpr(t.Elem())}
	case *types.Pointer:
		return &ast.StarExpr{X: tf.typeToAstExpr(t.Elem())}
	case *types.Interface:
		if t.NumMethods() == 0 {
			return &ast.InterfaceType{Methods: &ast.FieldList{List: []*ast.Field{}}}
		}
		panic(fmt.Sprintf("unsupported interface type: %v", t))
	case *types.Map:
		return &ast.MapType{Key: tf.typeToAstExpr(t.Key()), Value: tf.typeToAstExpr(t.Elem())}
	case *types.TypeParam:
		return ast.NewIdent(t.Obj().Name())
	default:
		panic(fmt.Sprintf("unsupported var type: %v", t))
	}
}

func nilOrWildcardIdent(expr ast.Expr) bool {
	return expr == nil || expr.(*ast.Ident).Name == "_"
}

func identName(expr ast.Expr) string {
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}
