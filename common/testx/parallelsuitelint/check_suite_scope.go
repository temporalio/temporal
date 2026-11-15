package parallelsuitelint

import (
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/cfg"
)

type suiteScopeChecker struct {
	pass         *analysis.Pass
	aliases      map[types.Object]types.Object
	writes       map[types.Object]int
	reported     map[token.Pos]bool
	awaitParents map[types.Object]bool
}

func (c *suiteScopeChecker) alias(lhs ast.Expr, rhs []ast.Expr, i int) {
	id, ok := lhs.(*ast.Ident)
	if !ok {
		return
	}
	obj := c.pass.TypesInfo.ObjectOf(id)
	c.writes[obj]++
	if i >= len(rhs) {
		return
	}
	if source, ok := ast.Unparen(rhs[i]).(*ast.Ident); ok {
		c.aliases[obj] = c.pass.TypesInfo.ObjectOf(source)
	}
}

func (c *suiteScopeChecker) root(expr ast.Expr) types.Object {
	id, ok := ast.Unparen(expr).(*ast.Ident)
	if !ok {
		return nil
	}
	obj := c.pass.TypesInfo.ObjectOf(id)
	seen := make(map[types.Object]bool)
	for c.writes[obj] == 1 && c.aliases[obj] != nil && !seen[obj] {
		seen[obj] = true
		obj = c.aliases[obj]
	}
	if c.writes[obj] > 1 {
		return nil
	}
	return obj
}

func (c *suiteScopeChecker) body(body *ast.BlockStmt, initial map[types.Object]bool) {
	graph := cfg.New(body, func(*ast.CallExpr) bool { return true })
	states := make(map[*cfg.Block]map[types.Object]bool)
	states[graph.Blocks[0]] = clone(initial)
	queue := []*cfg.Block{graph.Blocks[0]}
	for len(queue) > 0 {
		block := queue[0]
		queue = queue[1:]
		state := clone(states[block])
		for _, node := range block.Nodes {
			c.node(node, state)
		}
		for _, next := range block.Succs {
			previous, visited := states[next]
			if !visited {
				previous = make(map[types.Object]bool)
				states[next] = previous
			}
			changed := !visited
			for obj := range state {
				if !previous[obj] {
					previous[obj] = true
					changed = true
				}
			}
			if changed {
				queue = append(queue, next)
			}
		}
	}
}

func (c *suiteScopeChecker) node(node ast.Node, state map[types.Object]bool) {
	ast.Inspect(node, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				c.reset(lhs, state)
			}
		case *ast.ValueSpec:
			for _, name := range n.Names {
				c.reset(name, state)
			}
		default:
		}
		switch n.(type) {
		case *ast.FuncLit, *ast.DeferStmt, *ast.GoStmt:
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		c.call(call, state)
		return false
	})
}

func (c *suiteScopeChecker) reset(expr ast.Expr, state map[types.Object]bool) {
	id, ok := expr.(*ast.Ident)
	if !ok {
		return
	}
	obj := c.pass.TypesInfo.ObjectOf(id)
	if c.writes[obj] == 1 && c.aliases[obj] == nil {
		delete(state, obj)
	}
}

func (c *suiteScopeChecker) call(call *ast.CallExpr, state map[types.Object]bool) {
	c.node(call.Fun, state)
	for _, arg := range call.Args {
		c.node(arg, state)
	}
	sel, method := selectedFunction(c.pass, call)
	if method == nil || method.Pkg() == nil {
		return
	}
	selection := c.pass.TypesInfo.Selections[sel]
	if selection == nil || selection.Kind() != types.MethodVal {
		return
	}
	obj := c.root(sel.X)
	if obj == nil {
		return
	}
	path := method.Pkg().Path()
	if path == suitePath && !c.awaitParents[obj] && (method.Name() == "Run" || method.Name() == "RunSequential") {
		state[obj] = true
		for _, arg := range call.Args {
			if fn, ok := ast.Unparen(arg).(*ast.FuncLit); ok {
				c.body(fn.Body, clone(state))
			}
		}
		return
	}
	if path == suitePath && !state[obj] && (method.Name() == "Await" || method.Name() == "Awaitf") {
		c.awaitCallbacks(call, obj, state)
		return
	}
	if !state[obj] {
		return
	}
	guarded := suiteMethod(selection)
	if guarded && !c.reported[call.Pos()] {
		c.reported[call.Pos()] = true
		if c.awaitParents[obj] {
			c.pass.Reportf(call.Pos(), "use the callback suite inside Await; the parent uses a different assertion target and context")
		} else {
			c.pass.Reportf(call.Pos(), "parallelsuite receiver is sealed by Run; use the child callback suite")
		}
	}
}

func (c *suiteScopeChecker) awaitCallbacks(call *ast.CallExpr, parent types.Object, state map[types.Object]bool) {
	for _, arg := range call.Args {
		callback, ok := ast.Unparen(arg).(*ast.FuncLit)
		if !ok {
			continue
		}
		childState := clone(state)
		childState[parent] = true
		previous := c.awaitParents[parent]
		c.awaitParents[parent] = true
		c.body(callback.Body, childState)
		c.awaitParents[parent] = previous
	}
}

func (c *suiteScopeChecker) recordWrites(n ast.Node) bool {
	switch n := n.(type) {
	case *ast.AssignStmt:
		for i, lhs := range n.Lhs {
			c.alias(lhs, n.Rhs, i)
		}
	case *ast.ValueSpec:
		for i, lhs := range n.Names {
			c.alias(lhs, n.Values, i)
		}
	case *ast.IncDecStmt:
		if id, ok := n.X.(*ast.Ident); ok {
			c.writes[c.pass.TypesInfo.ObjectOf(id)]++
		}
	case *ast.RangeStmt:
		for _, expr := range []ast.Expr{n.Key, n.Value} {
			if id, ok := expr.(*ast.Ident); ok {
				c.writes[c.pass.TypesInfo.ObjectOf(id)] += 2
			}
		}
	default:
	}
	return true
}

// checkSuiteScope keeps suite API calls on the active subtest or await attempt.
// A parent may start further sibling subtests after Run.
func checkSuiteScope(pass *analysis.Pass) {
	c := &suiteScopeChecker{
		pass:         pass,
		aliases:      make(map[types.Object]types.Object),
		writes:       make(map[types.Object]int),
		reported:     make(map[token.Pos]bool),
		awaitParents: make(map[types.Object]bool),
	}
	for _, file := range pass.Files {
		ast.Inspect(file, c.recordWrites)
	}
	for _, file := range pass.Files {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
				c.body(fn.Body, make(map[types.Object]bool))
			}
		}
	}
}

func clone(state map[types.Object]bool) map[types.Object]bool {
	result := make(map[types.Object]bool)
	for obj := range state {
		result[obj] = true
	}
	return result
}
