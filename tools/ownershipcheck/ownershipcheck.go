// Package ownershipcheck is a static analyzer that flags a borrowed (possibly
// shared) reference value — a map, slice, pointer, or other kind selected by
// -value-kinds — being embedded by reference into a "sink" type that escapes the
// function: a value later serialized or read outside the lock that protected it.
// Such a value can be mutated by its retained alias during that read. Maps are
// always treated strictly; slices are reported when visible writes may mutate the
// backing array in place or when the checker cannot prove replace-only writes.
//
// The core is domain-agnostic and has no defaults: what counts as a sink, which
// reference kinds are tracked, and which calls sanitize are configuration (-sink,
// -value-kinds, -sanitizers). The protobuf profile lives in the lint-ownership
// Makefile target (a sink is a type with a ProtoReflect method, marshaled by gRPC
// outside the lock; tracked kinds are map,slice).
//
// The model is a polar ownership flow (inspired by algebraic subtyping): a value
// either provides ownership it is safe to embed ("owned") or it is "borrowed"
// (read off the shared receiver). A sink requires "owned"; a borrowed value
// reaching one is a finding, and the flow path from the source is the diagnostic.
//
// Each function has one polar signature (a signature fact), composed at call
// sites: per result, where its ownership comes from (own); per parameter, whether
// a borrowed argument there escapes into a sink. The //ownership:result,
// //ownership:param, and //ownership:ignore directives override inference and
// suppress findings.
package ownershipcheck

import (
	"go/ast"
	"go/token"
	"go/types"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/tools/go/analysis"
)

const Doc = `flag borrowed reference values embedded by reference into a sink that escapes

A reference value (-value-kinds: map, slice, pointer, ...) read out of shared state
(a field, or an accessor/getter that returns an internal field by reference) and
embedded into a sink type that escapes the function (e.g. a protobuf message
marshaled by gRPC outside the lock) can be mutated by its retained alias during that
read. Maps are strict; slices are strict unless visible writes prove replace-only
publication. Clone it (maps.Clone or slices.Clone) first.`

// How the file is organized. `run` drives two phases over each package, both built
// on one generic flow-sensitive walker (flow[T]):
//
//	ownership algebra  the lattice (own) and the exported per-function fact (signature)
//	configuration      -sink / -sanitizers flags (no defaults; proto profile is external)
//	checker / run      entry point and the two-phase driver
//	inference          PHASE 1: infer each function's signature, bottom-up to a fixpoint
//	abstract domain      └─ classifies expressions to `own` for inference (flow[own])
//	generic flow walker  the shared engine: a forward, branch-joining walk over a body
//	reporting          PHASE 2: classify expressions to `binding` and flag at sinks (flow[binding])
//	sinks                └─ the embed / field-assign / call-arg checks + diagnostics
//	escape detection   which values/protos escape the function (gate the sinks)
//	directives         //ownership:result | param | ignore parsing
//	helpers            isSink / isSanitizer / typeName and small AST/type utilities
//
// The two phases differ only in the lattice element they flow (own vs binding) and
// the work done per statement (collect signature vs emit diagnostics); the control
// flow, escape detection, and directives are shared.

// ---- ownership algebra ------------------------------------------------------

// ownKind is the polarity of a value's ownership.
type ownKind uint8

const (
	owned    ownKind = iota // freshly produced, cloned, a parameter, or a global
	borrowed                // read off the shared receiver (always borrowed)
	viaInput                // ownership equals input Idx's (receiver = -1, params 0..)
)

// own is an ownership expression in terms of a function's inputs. It is the
// element of a function's polar signature; at a call site it is resolved against
// the actual receiver/argument expressions.
//
// For viaInput, Idxs is the SET of inputs the value may alias (ownership is the OR
// over them): a helper returning one of several inputs (e.g. min(a, b)) records all
// of them, so a call site stays owned when its actual arguments are owned — rather
// than collapsing to borrowed and reporting a false positive. -1 = receiver, 0.. =
// parameter index; Idxs is kept sorted and deduplicated.
type own struct {
	Kind ownKind
	Idxs []int
}

func viaInputOf(i int) own { return own{Kind: viaInput, Idxs: []int{i}} }

// ownEqual compares two ownership elements (own is not == comparable: Idxs is a slice).
func ownEqual(a, b own) bool {
	if a.Kind != b.Kind {
		return false
	}
	if a.Kind != viaInput {
		return true
	}
	return intsEqual(a.Idxs, b.Idxs)
}

// combine merges the ownership of two return paths, conservatively: borrowed wins;
// owned is the identity; two viaInput sets union (the value may alias any of them).
func combine(a, b own) own {
	switch {
	case a.Kind == borrowed || b.Kind == borrowed:
		return own{Kind: borrowed}
	case a.Kind == owned:
		return b // owned ∨ viaInput → viaInput (conditional on the input)
	case b.Kind == owned:
		return a
	default:
		return own{Kind: viaInput, Idxs: unionInts(a.Idxs, b.Idxs)} // viaInput(x) ∨ viaInput(y)
	}
}

// signature is the polar ObjectFact of a function: where each result's ownership
// comes from, whether each slice result has fresh backing storage, and which
// parameters leak (a borrowed argument flows into a proto that escapes). Absence
// means "all results owned, no fresh slice results, no leaks".
type signature struct {
	Results []own
	Fresh   []bool
	Leaks   []bool
}

func (*signature) AFact() {}

func (s *signature) String() string {
	var parts []string
	nb := 0
	for _, r := range s.Results {
		if r.Kind != owned {
			nb++
		}
	}
	switch {
	case nb == 1:
		parts = append(parts, "borrowed result")
	case nb > 1:
		parts = append(parts, "borrowed results")
	}
	if anyBool(s.Leaks) {
		parts = append(parts, "embeds param")
	}
	if anyBool(s.Fresh) {
		parts = append(parts, "fresh result")
	}
	return strings.Join(parts, ", ")
}

func (s *signature) trivial() bool {
	for _, r := range s.Results {
		if r.Kind != owned {
			return false
		}
	}
	return !anyBool(s.Leaks) && !anyBool(s.Fresh)
}

// canonical returns the minimal equivalent signature (type simplification): own
// is normalized, and trailing trivial entries are dropped — absence defaults to
// owned / no-leak, so behaviorally-equal functions get identical facts.
func (s *signature) canonical() *signature {
	r := append([]own(nil), s.Results...)
	for i := range r {
		if r[i].Kind != viaInput {
			r[i] = own{Kind: r[i].Kind}
		} else {
			r[i] = own{Kind: viaInput, Idxs: unionInts(r[i].Idxs, nil)} // sort + dedup
		}
	}
	for len(r) > 0 && r[len(r)-1].Kind == owned {
		r = r[:len(r)-1]
	}
	l := append([]bool(nil), s.Leaks...)
	for len(l) > 0 && !l[len(l)-1] {
		l = l[:len(l)-1]
	}
	f := append([]bool(nil), s.Fresh...)
	for len(f) > 0 && !f[len(f)-1] {
		f = f[:len(f)-1]
	}
	return &signature{Results: r, Leaks: l, Fresh: f}
}

// ---- configuration ----------------------------------------------------------
//
// In taint-analysis terms the config is the classic triad: sources are structural
// (the shared receiver); sinks are types matched by -sink (a marker method or a
// qualified interface); sanitizers are calls that yield an owned value (-sanitizers).
// -value-kinds selects which reference kinds are tracked as the embedded hazard
// (e.g. map,slice — collections iterated during marshal; or pointer for an aliased
// sub-message). There are NO defaults — the checker has no built-in domain
// knowledge; the protobuf profile is supplied externally (centralized in the
// lint-ownership Makefile target). With nothing set, nothing is reported.
// -escape-funcs names opaque callees that retain or marshal an argument where
// inference cannot see (a gRPC client send, a cache/queue that stores the value):
// passing a borrowed value to the listed parameter is then flagged at the call site.
var (
	sinkFlag        string
	sanitizerFlag   string
	valueKindsFlag  string
	escapeFuncsFlag string
)

// validValueKinds enumerates the reference kinds -value-kinds may select (the
// underlying type kinds whose values alias shared mutable state).
var validValueKinds = map[string]bool{
	"map": true, "slice": true, "pointer": true,
	"interface": true, "chan": true, "func": true,
}

// ifaceRef identifies an interface by import path and name (e.g. encoding/json,
// Marshaler), parsed from a package-qualified -sink entry.
type ifaceRef struct{ path, name string }

// parseSinks splits -sink entries into bare marker-method names and qualified
// interface references. A dotted entry (pkg/path.Name) is an interface (precise:
// matched by types.Implements); a bare entry is a marker method name.
func parseSinks(csv string) (methods map[string]bool, ifaces []ifaceRef) {
	methods = map[string]bool{}
	for _, e := range strings.Split(csv, ",") {
		if e = strings.TrimSpace(e); e == "" {
			continue
		}
		if i := strings.LastIndex(e, "."); i >= 0 {
			ifaces = append(ifaces, ifaceRef{path: e[:i], name: e[i+1:]})
		} else {
			methods[e] = true
		}
	}
	return methods, ifaces
}

func parseSanitizers() map[string]map[string]bool {
	m := map[string]map[string]bool{}
	for _, entry := range strings.Split(sanitizerFlag, ",") {
		entry = strings.TrimSpace(entry)
		pkg, fn, ok := strings.Cut(entry, ".")
		if !ok || pkg == "" || fn == "" {
			continue
		}
		if m[pkg] == nil {
			m[pkg] = map[string]bool{}
		}
		m[pkg][fn] = true
	}
	return m
}

// parseValueKinds splits -value-kinds into the set of tracked reference kinds,
// ignoring blanks and unrecognized entries.
func parseValueKinds() map[string]bool {
	m := map[string]bool{}
	for _, e := range strings.Split(valueKindsFlag, ",") {
		if e = strings.TrimSpace(e); validValueKinds[e] {
			m[e] = true
		}
	}
	return m
}

// parseEscapeFuncs splits -escape-funcs into a lookup keyed by funcKey form
// (pkgpath.Func or pkgpath.Type.Method) -> set of escaping argument indices. Each
// entry is "<key>#<argN>" (argN defaults to 0 if omitted); -1 denotes the receiver.
func parseEscapeFuncs() map[string]map[int]bool {
	m := map[string]map[int]bool{}
	for _, e := range strings.Split(escapeFuncsFlag, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		key, idx := e, 0
		if h := strings.LastIndex(e, "#"); h >= 0 {
			key = strings.TrimSpace(e[:h])
			if n, err := strconv.Atoi(strings.TrimSpace(e[h+1:])); err == nil {
				idx = n
			}
		}
		if key == "" {
			continue
		}
		if m[key] == nil {
			m[key] = map[int]bool{}
		}
		m[key][idx] = true
	}
	return m
}

var Analyzer = &analysis.Analyzer{
	Name:      "ownershipcheck",
	Doc:       Doc,
	Run:       run,
	FactTypes: []analysis.Fact{(*signature)(nil)},
}

func init() {
	Analyzer.Flags.StringVar(&sinkFlag, "sink", "",
		"REQUIRED. comma-separated sinks: a bare marker-method name (e.g. ProtoReflect) or a qualified interface (e.g. encoding/json.Marshaler); a value of such a type is a sink")
	Analyzer.Flags.StringVar(&sanitizerFlag, "sanitizers", "",
		"comma-separated pkg.Func calls that yield an owned value from a borrowed one (e.g. clone/copy helpers)")
	Analyzer.Flags.StringVar(&valueKindsFlag, "value-kinds", "",
		"comma-separated reference kinds tracked as the embedded hazard: map, slice, pointer, interface, chan, func (e.g. map,slice)")
	Analyzer.Flags.StringVar(&escapeFuncsFlag, "escape-funcs", "",
		"comma-separated opaque callees that retain/marshal an argument, as pkgpath.Func#argN or pkgpath.Type.Method#argN (e.g. go.temporal.io/x/rpc.Client.Send#0)")
}

// ---- checker & run: entry point and the two-phase driver ------------------

type checker struct {
	pass        *analysis.Pass
	sigs        map[*types.Func]*signature // signatures inferred for funcs in this package
	sinkCache   map[types.Type]bool        // memoized isSink results
	sinkMeths   map[string]bool            // marker methods identifying a sink type
	sinkIfaces  []*types.Interface         // interfaces a sink type must implement
	sanitizers  map[string]map[string]bool // pkg -> func -> yields an owned value
	valueKinds  map[string]bool            // reference kinds tracked as the embedded hazard
	escapeFuncs map[string]map[int]bool    // funcKey -> escaping arg indices (opaque callees)
	leafCache   map[types.Type]bool        // memoized reachesLeaf results
	sliceWrites map[*types.Var]sliceWrite  // field -> whether visible writes can mutate the backing array
}

func run(pass *analysis.Pass) (any, error) {
	meths, ifaceRefs := parseSinks(sinkFlag)
	c := &checker{
		pass:        pass,
		sigs:        map[*types.Func]*signature{},
		sinkCache:   map[types.Type]bool{},
		sinkMeths:   meths,
		sanitizers:  parseSanitizers(),
		valueKinds:  parseValueKinds(),
		escapeFuncs: parseEscapeFuncs(),
		leafCache:   map[types.Type]bool{},
		sliceWrites: map[*types.Var]sliceWrite{},
	}
	for _, ref := range ifaceRefs {
		if iface := c.resolveInterface(ref); iface != nil {
			c.sinkIfaces = append(c.sinkIfaces, iface)
		}
	}
	c.infer() // compute & export per-function polar signatures
	c.collectSliceWrites()
	c.report() // flag borrowed values reaching owned-requiring sinks
	return nil, nil
}

// ---- slice mutability -------------------------------------------------------

type sliceWrite uint8

const (
	sliceReplaceOnly sliceWrite = iota + 1
	sliceUnknown
	sliceMutates
)

func (c *checker) collectSliceWrites() {
	for _, file := range c.pass.Files {
		for _, d := range file.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			c.collectFuncSliceWrites(fd.Body)
		}
	}
}

func (c *checker) collectFuncSliceWrites(body *ast.BlockStmt) {
	aliases := map[*types.Var]*types.Var{}
	fresh := map[*types.Var]bool{}
	var cases func(*ast.BlockStmt)
	var walk func([]ast.Stmt)
	cases = func(body *ast.BlockStmt) {
		for _, cc := range body.List {
			cl, ok := cc.(*ast.CaseClause)
			if !ok {
				continue
			}
			c.collectExprSliceWrites(cl.List, aliases)
			walk(cl.Body)
		}
	}
	walk = func(stmts []ast.Stmt) {
		for _, stmt := range stmts {
			switch s := stmt.(type) {
			case *ast.AssignStmt:
				c.collectExprSliceWrites(s.Rhs, aliases)
				c.collectAssignSliceWrites(s, aliases, fresh)
			case *ast.ExprStmt:
				c.collectExprSliceWrites([]ast.Expr{s.X}, aliases)
			case *ast.ReturnStmt:
				c.collectExprSliceWrites(s.Results, aliases)
			case *ast.SendStmt:
				c.collectExprSliceWrites([]ast.Expr{s.Value}, aliases)
			case *ast.GoStmt:
				c.collectExprSliceWrites([]ast.Expr{s.Call}, aliases)
			case *ast.DeferStmt:
				c.collectExprSliceWrites([]ast.Expr{s.Call}, aliases)
			case *ast.DeclStmt:
				c.collectDeclSliceWrites(s, aliases, fresh)
			case *ast.BlockStmt:
				walk(s.List)
			case *ast.LabeledStmt:
				walk([]ast.Stmt{s.Stmt})
			case *ast.IfStmt:
				if s.Init != nil {
					walk([]ast.Stmt{s.Init})
				}
				c.collectExprSliceWrites([]ast.Expr{s.Cond}, aliases)
				walk(s.Body.List)
				if s.Else != nil {
					walk([]ast.Stmt{s.Else})
				}
			case *ast.ForStmt:
				if s.Init != nil {
					walk([]ast.Stmt{s.Init})
				}
				if s.Cond != nil {
					c.collectExprSliceWrites([]ast.Expr{s.Cond}, aliases)
				}
				walk(s.Body.List)
				if s.Post != nil {
					walk([]ast.Stmt{s.Post})
				}
			case *ast.RangeStmt:
				c.collectExprSliceWrites([]ast.Expr{s.X}, aliases)
				walk(s.Body.List)
			case *ast.SwitchStmt:
				if s.Init != nil {
					walk([]ast.Stmt{s.Init})
				}
				if s.Tag != nil {
					c.collectExprSliceWrites([]ast.Expr{s.Tag}, aliases)
				}
				cases(s.Body)
			case *ast.TypeSwitchStmt:
				if s.Init != nil {
					walk([]ast.Stmt{s.Init})
				}
				cases(s.Body)
			case *ast.SelectStmt:
				for _, cc := range s.Body.List {
					if comm, ok := cc.(*ast.CommClause); ok {
						walk(comm.Body)
					}
				}
			}
		}
	}
	walk(body.List)
}

func (c *checker) collectDeclSliceWrites(ds *ast.DeclStmt, aliases map[*types.Var]*types.Var, fresh map[*types.Var]bool) {
	gd, ok := ds.Decl.(*ast.GenDecl)
	if !ok {
		return
	}
	for _, spec := range gd.Specs {
		vs, ok := spec.(*ast.ValueSpec)
		if !ok {
			continue
		}
		c.collectExprSliceWrites(vs.Values, aliases)
		if len(vs.Values) == 1 && len(vs.Names) > 1 {
			if call, ok := unparen(vs.Values[0]).(*ast.CallExpr); ok {
				for i, name := range vs.Names {
					c.bindSliceLocalCallResult(name, call, i, aliases, fresh)
				}
				continue
			}
		}
		if len(vs.Names) != len(vs.Values) {
			continue
		}
		for i, name := range vs.Names {
			c.bindSliceLocal(name, vs.Values[i], aliases, fresh)
		}
	}
}

func (c *checker) collectAssignSliceWrites(as *ast.AssignStmt, aliases map[*types.Var]*types.Var, fresh map[*types.Var]bool) {
	if len(as.Rhs) == 1 && len(as.Lhs) > 1 {
		if call, ok := unparen(as.Rhs[0]).(*ast.CallExpr); ok {
			for i, lhs := range as.Lhs {
				c.bindSliceLocalCallResult(lhs, call, i, aliases, fresh)
			}
			return
		}
	}
	if len(as.Lhs) != len(as.Rhs) {
		return
	}
	for i, lhs := range as.Lhs {
		rhs := as.Rhs[i]
		if src := c.sliceSource(lhs, aliases); src != nil && isIndexOrSliceMutation(lhs) {
			c.markSliceWrite(src, sliceMutates)
			continue
		}
		if src := c.sliceSource(lhs, aliases); src != nil {
			if c.isFreshSlice(rhs, fresh) {
				c.markSliceWrite(src, sliceReplaceOnly)
			} else {
				c.markSliceWrite(src, sliceUnknown)
			}
			continue
		}
		c.bindSliceLocal(lhs, rhs, aliases, fresh)
	}
}

func (c *checker) bindSliceLocalCallResult(
	lhs ast.Expr,
	call *ast.CallExpr,
	i int,
	aliases map[*types.Var]*types.Var,
	fresh map[*types.Var]bool,
) {
	id, ok := unparen(lhs).(*ast.Ident)
	if !ok {
		return
	}
	v, ok := c.pass.TypesInfo.ObjectOf(id).(*types.Var)
	if !ok {
		return
	}
	delete(aliases, v)
	if c.callResultFresh(call, i) {
		fresh[v] = true
	} else {
		delete(fresh, v)
	}
}

func (c *checker) bindSliceLocal(lhs ast.Expr, rhs ast.Expr, aliases map[*types.Var]*types.Var, fresh map[*types.Var]bool) {
	id, ok := lhs.(*ast.Ident)
	if !ok {
		return
	}
	v, ok := c.pass.TypesInfo.ObjectOf(id).(*types.Var)
	if !ok {
		return
	}
	if src := c.sliceSource(rhs, aliases); src != nil {
		aliases[v] = src
		delete(fresh, v)
		return
	}
	delete(aliases, v)
	if c.isFreshSlice(rhs, fresh) {
		fresh[v] = true
	} else {
		delete(fresh, v)
	}
}

func (c *checker) collectExprSliceWrites(exprs []ast.Expr, aliases map[*types.Var]*types.Var) {
	for _, expr := range exprs {
		if expr == nil {
			continue
		}
		ast.Inspect(expr, func(n ast.Node) bool {
			if _, ok := n.(*ast.FuncLit); ok {
				return false
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			c.collectCallSliceWrites(call, aliases)
			return true
		})
	}
}

func (c *checker) collectCallSliceWrites(call *ast.CallExpr, aliases map[*types.Var]*types.Var) {
	switch {
	case c.isBuiltinCall(call, "append"):
		if len(call.Args) > 0 {
			if src := c.sliceSource(call.Args[0], aliases); src != nil {
				c.markSliceWrite(src, sliceMutates)
			}
		}
		return
	case c.isBuiltinCall(call, "copy"):
		if len(call.Args) > 0 {
			if src := c.sliceSource(call.Args[0], aliases); src != nil {
				c.markSliceWrite(src, sliceMutates)
			}
		}
		return
	case c.isBuiltinCall(call, "len") || c.isBuiltinCall(call, "cap"):
		return
	}
	if c.isKnownSliceMutator(call) {
		if len(call.Args) > 0 {
			if src := c.sliceSource(call.Args[0], aliases); src != nil {
				c.markSliceWrite(src, sliceMutates)
			}
		}
		return
	}
	if c.calleeFunc(call) == nil {
		return
	}
	for _, arg := range call.Args {
		if src := c.sliceSource(arg, aliases); src != nil {
			c.markSliceWrite(src, sliceUnknown)
		}
	}
}

func (c *checker) markSliceWrite(field *types.Var, w sliceWrite) {
	switch c.sliceWrites[field] {
	case sliceMutates:
		return
	case sliceUnknown:
		if w != sliceMutates {
			return
		}
	}
	c.sliceWrites[field] = w
}

func (c *checker) sliceSource(expr ast.Expr, aliases map[*types.Var]*types.Var) *types.Var {
	switch e := unparen(expr).(type) {
	case *ast.Ident:
		v, _ := c.pass.TypesInfo.ObjectOf(e).(*types.Var)
		if v == nil {
			return nil
		}
		return aliases[v]
	case *ast.SelectorExpr:
		if sel := c.pass.TypesInfo.Selections[e]; sel != nil {
			if v, ok := sel.Obj().(*types.Var); ok && kindOf(v.Type()) == "slice" && !isByteSlice(v.Type()) {
				return v
			}
		}
	case *ast.SliceExpr:
		return c.sliceSource(e.X, aliases)
	case *ast.CallExpr:
		return c.getterSliceSource(e)
	case *ast.TypeAssertExpr:
		return c.sliceSource(e.X, aliases)
	case *ast.StarExpr:
		return c.sliceSource(e.X, aliases)
	}
	return nil
}

func (c *checker) getterSliceSource(call *ast.CallExpr) *types.Var {
	sel, ok := unparen(call.Fun).(*ast.SelectorExpr)
	if !ok || len(call.Args) != 0 || !strings.HasPrefix(sel.Sel.Name, "Get") {
		return nil
	}
	fn, _ := c.pass.TypesInfo.ObjectOf(sel.Sel).(*types.Func)
	if fn == nil {
		return nil
	}
	sig, _ := fn.Type().(*types.Signature)
	if sig == nil || sig.Results().Len() == 0 {
		return nil
	}
	fieldName := strings.TrimPrefix(sel.Sel.Name, "Get")
	if fieldName == "" {
		return nil
	}
	return fieldByName(receiverBase(sig), fieldName)
}

func receiverBase(sig *types.Signature) types.Type {
	if sig == nil || sig.Recv() == nil {
		return nil
	}
	t := sig.Recv().Type()
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	if n, ok := t.(*types.Named); ok {
		return n.Underlying()
	}
	return t.Underlying()
}

func fieldByName(t types.Type, name string) *types.Var {
	st, ok := t.(*types.Struct)
	if !ok {
		return nil
	}
	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		if f.Name() == name && kindOf(f.Type()) == "slice" && !isByteSlice(f.Type()) {
			return f
		}
	}
	return nil
}

func (c *checker) isFreshSlice(expr ast.Expr, fresh map[*types.Var]bool) bool {
	switch e := unparen(expr).(type) {
	case *ast.Ident:
		if e.Name == "nil" {
			return true
		}
		v, _ := c.pass.TypesInfo.ObjectOf(e).(*types.Var)
		return v != nil && fresh[v]
	case *ast.CallExpr:
		if c.isBuiltinCall(e, "make") && kindOf(c.pass.TypesInfo.TypeOf(e)) == "slice" {
			return true
		}
		if c.isBuiltinCall(e, "append") && len(e.Args) > 0 {
			return c.isFreshSlice(e.Args[0], fresh)
		}
		if c.isSanitizer(e) {
			return true
		}
		return c.callResultFresh(e, 0)
	case *ast.CompositeLit:
		return kindOf(c.pass.TypesInfo.TypeOf(e)) == "slice"
	}
	return false
}

func (c *checker) callResultFresh(call *ast.CallExpr, i int) bool {
	callee := c.calleeFunc(call)
	if callee == nil {
		return false
	}
	sig := c.signatureOf(callee)
	return sig != nil && i < len(sig.Fresh) && sig.Fresh[i]
}

func (c *checker) isKnownSliceMutator(call *ast.CallExpr) bool {
	fn := c.calleeFunc(call)
	if fn == nil || fn.Pkg() == nil {
		return false
	}
	switch fn.Pkg().Path() {
	case "sort":
		return fn.Name() == "Slice" || fn.Name() == "SliceStable"
	case "slices":
		switch fn.Name() {
		case "Sort", "SortFunc", "SortStableFunc", "Reverse":
			return true
		}
	}
	return false
}

func calledName(call *ast.CallExpr) string {
	switch fun := unparen(call.Fun).(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	}
	return ""
}

func (c *checker) isBuiltinCall(call *ast.CallExpr, name string) bool {
	id, ok := unparen(call.Fun).(*ast.Ident)
	if !ok {
		return false
	}
	builtin, ok := c.pass.TypesInfo.ObjectOf(id).(*types.Builtin)
	return ok && builtin.Name() == name
}

func isIndexOrSliceMutation(expr ast.Expr) bool {
	switch unparen(expr).(type) {
	case *ast.IndexExpr, *ast.SliceExpr:
		return true
	}
	return false
}

// resolveInterface finds the interface type named by ref in this package or its
// import closure; nil if not reachable (then no value of it exists here anyway).
func (c *checker) resolveInterface(ref ifaceRef) *types.Interface {
	pkg := c.findPackage(ref.path)
	if pkg == nil {
		return nil
	}
	tn, ok := pkg.Scope().Lookup(ref.name).(*types.TypeName)
	if !ok {
		return nil
	}
	iface, _ := tn.Type().Underlying().(*types.Interface)
	return iface
}

func (c *checker) findPackage(path string) *types.Package {
	seen := map[*types.Package]bool{}
	var walk func(*types.Package) *types.Package
	walk = func(p *types.Package) *types.Package {
		if p == nil || seen[p] {
			return nil
		}
		seen[p] = true
		if p.Path() == path {
			return p
		}
		for _, imp := range p.Imports() {
			if r := walk(imp); r != nil {
				return r
			}
		}
		return nil
	}
	return walk(c.pass.Pkg)
}

// ---- inference: per-function signatures -------------------------------------

func (c *checker) infer() {
	type decl struct {
		fn   *types.Func
		body *ast.FuncDecl
	}
	var decls []decl
	for _, file := range c.pass.Files {
		for _, d := range file.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			if fn, ok := c.pass.TypesInfo.Defs[fd.Name].(*types.Func); ok {
				decls = append(decls, decl{fn, fd})
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for _, d := range decls {
			sig := c.inferSignature(d.fn, d.body).canonical()
			if prev := c.sigs[d.fn]; prev == nil || !sigEqual(prev, sig) {
				c.sigs[d.fn] = sig
				changed = true
			}
		}
	}
	for fn, sig := range c.sigs {
		if !sig.trivial() {
			c.pass.ExportObjectFact(fn, sig)
		}
	}
}

func (c *checker) inferSignature(fn *types.Func, fd *ast.FuncDecl) *signature {
	sig, ok := fn.Type().(*types.Signature)
	if !ok {
		return &signature{}
	}
	nr, np := sig.Results().Len(), sig.Params().Len()
	out := &signature{Results: make([]own, nr), Leaks: make([]bool, np)}

	ic := c.newInferCtx(fd)
	esc := c.escapingVars(fd)
	escLits := c.escapingLits(fd, esc)

	f := &flow[own]{
		c:        c,
		owned:    own{Kind: owned},
		merge:    combine,
		classify: ic.classify,
		callRes:  ic.callResult,
	}
	// Results: combine the env-resolved return expressions (flow-sensitive).
	// //ownership:result annotations override specific results afterward.
	f.onReturn = func(results []ast.Expr, env map[*types.Var]own) {
		switch {
		case len(results) == nr:
			for i, r := range results {
				out.Results[i] = combine(out.Results[i], ic.classify(r, env))
			}
		case len(results) == 1 && nr > 1:
			if call, ok := unparen(results[0]).(*ast.CallExpr); ok {
				for i := range out.Results {
					out.Results[i] = combine(out.Results[i], ic.callResult(call, i, env))
				}
			}
		}
	}
	// Leaks: a parameter embedded into an escaping proto (directly, via an escaping
	// field assignment, or passed to another function's leaking parameter).
	f.onExprs = func(exprs []ast.Expr, env map[*types.Var]own) {
		for _, expr := range exprs {
			if expr == nil {
				continue
			}
			ast.Inspect(expr, func(n ast.Node) bool {
				if _, ok := n.(*ast.FuncLit); ok {
					return false
				}
				switch x := n.(type) {
				case *ast.CompositeLit:
					if !escLits[x] {
						return true
					}
					if t := c.pass.TypesInfo.TypeOf(x); t == nil || !c.isSink(t) {
						return true
					}
					for _, elt := range x.Elts {
						if kv, ok := elt.(*ast.KeyValueExpr); ok {
							if vt := c.pass.TypesInfo.TypeOf(kv.Value); c.tracked(vt) {
								for _, i := range paramIdxs(ic.classify(kv.Value, env)) {
									out.Leaks[i] = true
								}
							}
						}
					}
				case *ast.CallExpr:
					if callee := c.calleeFunc(x); callee != nil {
						if cs := c.signatureOf(callee); cs != nil {
							for j, arg := range x.Args {
								if j < len(cs.Leaks) && cs.Leaks[j] {
									for _, i := range paramIdxs(ic.classify(arg, env)) {
										out.Leaks[i] = true
									}
								}
							}
						}
					}
				}
				return true
			})
		}
	}
	f.onAssign = func(as *ast.AssignStmt, env map[*types.Var]own) {
		if len(as.Lhs) != len(as.Rhs) {
			return
		}
		for k, lhs := range as.Lhs {
			sel, ok := lhs.(*ast.SelectorExpr)
			if !ok {
				continue
			}
			if ft := c.pass.TypesInfo.TypeOf(sel); !c.tracked(ft) {
				continue
			}
			if h := c.pass.TypesInfo.TypeOf(sel.X); h == nil || !c.isSink(h) {
				continue
			}
			if v := c.rootVar(sel.X); v == nil || !esc[v] {
				continue
			}
			for _, i := range paramIdxs(ic.classify(as.Rhs[k], env)) {
				out.Leaks[i] = true
			}
		}
	}
	f.run(fd.Body)
	// //ownership:result annotations override inferred result ownership.
	for i, k := range c.resultAnnotations(fd, nr) {
		if i < len(out.Results) {
			out.Results[i] = own{Kind: k}
		}
	}
	// //ownership:escapes annotations force a parameter to leak (for opaque
	// callees inference can't see through, e.g. an RPC sender that marshals it).
	for i := range c.paramEscapes(fd) {
		if i < len(out.Leaks) {
			out.Leaks[i] = true
		}
	}
	out.Fresh = c.inferFreshResults(fn, fd, nr)
	return out
}

type freshValue struct {
	fresh   bool
	origins map[freshOrigin]struct{}
}

type freshOrigin struct {
	pos    token.Pos
	result int
}

func freshAt(pos token.Pos) freshValue {
	return freshResultAt(pos, -1)
}

func freshResultAt(pos token.Pos, result int) freshValue {
	origin := freshOrigin{pos: pos, result: result}
	return freshValue{fresh: true, origins: map[freshOrigin]struct{}{origin: {}}}
}

func mergeFresh(a, b freshValue) freshValue {
	if !a.fresh || !b.fresh {
		return freshValue{}
	}
	origins := make(map[freshOrigin]struct{}, len(a.origins)+len(b.origins))
	for pos := range a.origins {
		origins[pos] = struct{}{}
	}
	for pos := range b.origins {
		origins[pos] = struct{}{}
	}
	return freshValue{fresh: true, origins: origins}
}

type freshCtx struct {
	c *checker
}

type freshResults struct {
	values  []bool
	origins []map[freshOrigin]struct{}
	seen    []bool
}

func newFreshResults(n int) *freshResults {
	return &freshResults{
		values:  make([]bool, n),
		origins: make([]map[freshOrigin]struct{}, n),
		seen:    make([]bool, n),
	}
}

func (c *checker) inferFreshResults(fn *types.Func, fd *ast.FuncDecl, nr int) []bool {
	if nr == 0 {
		return nil
	}
	sig, ok := fn.Type().(*types.Signature)
	if !ok {
		return make([]bool, nr)
	}
	fc := freshCtx{c: c}
	results := newFreshResults(nr)
	named := namedResultVars(c.pass, fd, nr)
	f := &flow[freshValue]{
		c:        c,
		owned:    freshValue{},
		merge:    mergeFresh,
		classify: fc.classify,
		callRes:  fc.callResult,
	}
	f.onExprs = func(exprs []ast.Expr, env map[*types.Var]freshValue) {
		fc.invalidateCalls(exprs, env)
	}
	f.onAssign = func(as *ast.AssignStmt, env map[*types.Var]freshValue) {
		fc.invalidateRetainedAssignments(as, env)
	}
	f.onSend = func(expr ast.Expr, env map[*types.Var]freshValue) {
		fc.invalidateAliases(fc.classify(expr, env), env)
	}
	f.onReturn = func(exprs []ast.Expr, env map[*types.Var]freshValue) {
		results.record(fc.returnValues(exprs, env, named, nr))
	}
	f.run(fd.Body)
	return results.finish(sig)
}

func (fc freshCtx) returnValues(
	exprs []ast.Expr,
	env map[*types.Var]freshValue,
	named []*types.Var,
	nr int,
) []freshValue {
	values := make([]freshValue, nr)
	switch {
	case len(exprs) == nr:
		for i, expr := range exprs {
			values[i] = fc.classify(expr, env)
		}
	case len(exprs) == 1 && nr > 1:
		if call, ok := unparen(exprs[0]).(*ast.CallExpr); ok {
			for i := range values {
				values[i] = fc.callResult(call, i, env)
			}
		}
	case len(exprs) == 0:
		for i, v := range named {
			values[i] = env[v]
		}
	default:
	}
	return values
}

func (r *freshResults) record(values []freshValue) {
	for i, value := range values {
		if r.origins[i] == nil {
			r.origins[i] = map[freshOrigin]struct{}{}
		}
		for origin := range value.origins {
			r.origins[i][origin] = struct{}{}
		}
		if !r.seen[i] {
			r.values[i] = value.fresh
			r.seen[i] = true
		} else {
			r.values[i] = r.values[i] && value.fresh
		}
	}
}

func (r *freshResults) finish(sig *types.Signature) []bool {
	for i := range r.values {
		r.values[i] = kindOf(sig.Results().At(i).Type()) == "slice" && r.seen[i] && r.values[i]
	}
	for i := range r.values {
		for j := i + 1; j < len(r.values); j++ {
			if originsOverlap(r.origins[i], r.origins[j]) {
				r.values[i] = false
				r.values[j] = false
			}
		}
	}
	return r.values
}

func originsOverlap(a, b map[freshOrigin]struct{}) bool {
	for origin := range a {
		if _, ok := b[origin]; ok {
			return true
		}
	}
	return false
}

func namedResultVars(pass *analysis.Pass, fd *ast.FuncDecl, nr int) []*types.Var {
	results := make([]*types.Var, nr)
	if fd.Type.Results == nil {
		return results
	}
	i := 0
	for _, field := range fd.Type.Results.List {
		for _, name := range field.Names {
			if i < len(results) {
				if v, ok := pass.TypesInfo.Defs[name].(*types.Var); ok {
					results[i] = v
				}
			}
			i++
		}
		if len(field.Names) == 0 {
			i++
		}
	}
	return results
}

func (fc freshCtx) classify(expr ast.Expr, env map[*types.Var]freshValue) freshValue {
	switch e := unparen(expr).(type) {
	case *ast.Ident:
		if e.Name == "nil" {
			return freshValue{fresh: true}
		}
		if v, ok := fc.c.pass.TypesInfo.ObjectOf(e).(*types.Var); ok {
			return env[v]
		}
		return freshValue{}
	case *ast.CompositeLit:
		if kindOf(fc.c.pass.TypesInfo.TypeOf(e)) == "slice" {
			return freshAt(e.Pos())
		}
		value := freshValue{fresh: true}
		for _, elt := range e.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				elt = kv.Value
			}
			candidate := fc.classify(elt, env)
			if candidate.fresh {
				value = mergeFresh(value, candidate)
			}
		}
		return value
	case *ast.SliceExpr:
		return fc.classify(e.X, env)
	case *ast.UnaryExpr:
		return fc.classify(e.X, env)
	case *ast.CallExpr:
		return fc.callResult(e, 0, env)
	}
	return freshValue{}
}

func (fc freshCtx) callResult(call *ast.CallExpr, i int, env map[*types.Var]freshValue) freshValue {
	switch {
	case fc.c.isBuiltinCall(call, "make"):
		if i == 0 && kindOf(fc.c.pass.TypesInfo.TypeOf(call)) == "slice" {
			return freshAt(call.Pos())
		}
	case fc.c.isBuiltinCall(call, "append"):
		if i == 0 && len(call.Args) > 0 {
			base := fc.classify(call.Args[0], env)
			if base.fresh && len(base.origins) == 0 {
				return freshAt(call.Pos())
			}
			return base
		}
	default:
	}
	if i == 0 && fc.c.isSanitizer(call) {
		return freshAt(call.Pos())
	}
	callee := fc.c.calleeFunc(call)
	if callee == nil {
		return freshValue{}
	}
	sig := fc.c.signatureOf(callee)
	if sig != nil && i < len(sig.Fresh) && sig.Fresh[i] {
		return freshResultAt(call.Pos(), i)
	}
	return freshValue{}
}

func (fc freshCtx) invalidateCalls(exprs []ast.Expr, env map[*types.Var]freshValue) {
	for _, expr := range exprs {
		ast.Inspect(expr, func(n ast.Node) bool {
			return fc.invalidateCallNode(n, env)
		})
	}
}

func (fc freshCtx) invalidateCallNode(n ast.Node, env map[*types.Var]freshValue) bool {
	if lit, ok := n.(*ast.FuncLit); ok {
		fc.invalidateClosure(lit, env)
		return false
	}
	call, ok := n.(*ast.CallExpr)
	if !ok || fc.isFreshnessSafeCall(call) {
		return true
	}
	for _, arg := range call.Args {
		fc.invalidateAliases(fc.classify(arg, env), env)
	}
	if sel, ok := unparen(call.Fun).(*ast.SelectorExpr); ok {
		fc.invalidateAliases(fc.classify(sel.X, env), env)
	}
	return true
}

func (fc freshCtx) invalidateClosure(lit *ast.FuncLit, env map[*types.Var]freshValue) {
	ast.Inspect(lit.Body, func(n ast.Node) bool {
		id, ok := n.(*ast.Ident)
		if !ok {
			return true
		}
		if v, ok := fc.c.pass.TypesInfo.ObjectOf(id).(*types.Var); ok {
			fc.invalidateAliases(env[v], env)
		}
		return true
	})
}

func (fc freshCtx) isFreshnessSafeCall(call *ast.CallExpr) bool {
	for _, name := range []string{"append", "cap", "copy", "len", "make"} {
		if fc.c.isBuiltinCall(call, name) {
			return true
		}
	}
	return fc.c.isSanitizer(call)
}

func (fc freshCtx) invalidateRetainedAssignments(as *ast.AssignStmt, env map[*types.Var]freshValue) {
	if len(as.Lhs) != len(as.Rhs) {
		return
	}
	for i, lhs := range as.Lhs {
		id, ok := unparen(lhs).(*ast.Ident)
		if ok {
			v, varOK := fc.c.pass.TypesInfo.ObjectOf(id).(*types.Var)
			if varOK && (v.Pkg() == nil || v.Parent() != v.Pkg().Scope()) {
				continue
			}
		}
		fc.invalidateAliases(fc.classify(as.Rhs[i], env), env)
	}
}

func (freshCtx) invalidateAliases(value freshValue, env map[*types.Var]freshValue) {
	if !value.fresh || len(value.origins) == 0 {
		return
	}
	for v, candidate := range env {
		for origin := range value.origins {
			if _, ok := candidate.origins[origin]; ok {
				env[v] = freshValue{}
				break
			}
		}
	}
}

// paramIdxs returns the parameter indices an ownership value may root to (the
// non-receiver members of a viaInput set). A borrowed argument at any of them leaks.
func paramIdxs(o own) []int {
	if o.Kind != viaInput {
		return nil
	}
	var out []int
	for _, i := range o.Idxs {
		if i >= 0 {
			out = append(out, i)
		}
	}
	return out
}

func (c *checker) signatureOf(fn *types.Func) *signature {
	fn = fn.Origin()
	if s, ok := c.sigs[fn]; ok {
		return s
	}
	var s signature
	if c.pass.ImportObjectFact(fn, &s) {
		return &s
	}
	// Opaque callee with no inferred/imported fact: fall back to -escape-funcs config.
	return c.configuredEscape(fn)
}

// configuredEscape synthesizes a signature for a callee listed in -escape-funcs,
// marking the configured argument indices as leaking, or nil if not configured.
func (c *checker) configuredEscape(fn *types.Func) *signature {
	args := c.escapeFuncs[funcKey(fn)]
	if args == nil {
		return nil
	}
	np := 0
	if sig, ok := fn.Type().(*types.Signature); ok {
		np = sig.Params().Len()
	}
	leaks := make([]bool, np)
	for i := range args {
		if i >= 0 && i < np {
			leaks[i] = true
		}
	}
	return &signature{Leaks: leaks}
}

// funcKey is the -escape-funcs lookup key for a function: pkgpath.Type.Method for a
// method, pkgpath.Func for a free function (matching the configured-entry form).
func funcKey(fn *types.Func) string {
	pkg := ""
	if fn.Pkg() != nil {
		pkg = fn.Pkg().Path()
	}
	if sig, ok := fn.Type().(*types.Signature); ok {
		if recv := sig.Recv(); recv != nil {
			t := recv.Type()
			if p, ok := t.(*types.Pointer); ok {
				t = p.Elem()
			}
			if named, ok := t.(*types.Named); ok {
				return pkg + "." + named.Obj().Name() + "." + fn.Name()
			}
		}
	}
	return pkg + "." + fn.Name()
}

// ---- abstract domain: ownership in terms of a function's inputs -------------

// inferCtx classifies expressions into own (ownership in terms of the function's
// inputs) for signature inference; locals come from the flow env.
type inferCtx struct {
	c       *checker
	recv    *types.Var
	paramIx map[*types.Var]int
}

func (c *checker) newInferCtx(fd *ast.FuncDecl) *inferCtx {
	ic := &inferCtx{c: c, paramIx: map[*types.Var]int{}}
	if fd.Recv != nil {
		for _, f := range fd.Recv.List {
			for _, n := range f.Names {
				if v, ok := c.pass.TypesInfo.Defs[n].(*types.Var); ok {
					ic.recv = v
				}
			}
		}
	}
	pos := 0
	if fd.Type.Params != nil {
		for _, f := range fd.Type.Params.List {
			if len(f.Names) == 0 {
				pos++
				continue
			}
			for _, n := range f.Names {
				if v, ok := c.pass.TypesInfo.Defs[n].(*types.Var); ok {
					ic.paramIx[v] = pos
				}
				pos++
			}
		}
	}
	return ic
}

func (ic *inferCtx) classify(expr ast.Expr, env map[*types.Var]own) own {
	switch e := unparen(expr).(type) {
	case *ast.Ident:
		v, ok := ic.c.pass.TypesInfo.ObjectOf(e).(*types.Var)
		if !ok {
			return own{Kind: owned}
		}
		if v == ic.recv {
			return viaInputOf(-1)
		}
		if i, ok := ic.paramIx[v]; ok {
			return viaInputOf(i)
		}
		if o, ok := env[v]; ok {
			return o
		}
		return own{Kind: owned} // global / unresolved local: optimistic
	case *ast.SelectorExpr:
		if _, isSel := ic.c.pass.TypesInfo.Selections[e]; isSel {
			return ic.classify(e.X, env)
		}
		return own{Kind: owned}
	case *ast.IndexExpr:
		return ic.classify(e.X, env)
	case *ast.SliceExpr:
		return ic.classify(e.X, env)
	case *ast.StarExpr:
		return ic.classify(e.X, env)
	case *ast.TypeAssertExpr:
		return ic.classify(e.X, env)
	case *ast.CallExpr:
		return ic.callResult(e, 0, env)
	}
	return own{Kind: owned}
}

func (ic *inferCtx) callResult(call *ast.CallExpr, i int, env map[*types.Var]own) own {
	if ic.c.isSanitizer(call) {
		return own{Kind: owned}
	}
	callee := ic.c.calleeFunc(call)
	if callee == nil {
		return own{Kind: owned}
	}
	cs := ic.c.signatureOf(callee)
	if cs == nil || i >= len(cs.Results) {
		return own{Kind: owned}
	}
	r := cs.Results[i]
	switch r.Kind {
	case owned, borrowed:
		return r
	case viaInput:
		// Resolve every member of the input set against the call's actuals and
		// combine: owned iff all members resolve to owned (the OR semantics).
		res := own{Kind: owned}
		for _, idx := range r.Idxs {
			res = combine(res, ic.classify(callArg(call, idx), env))
		}
		return res
	}
	return own{Kind: owned}
}

// callArg returns the actual expression an input index refers to at a call site:
// the receiver expression for -1, the positional argument otherwise (nil if absent,
// which classify treats as owned).
func callArg(call *ast.CallExpr, idx int) ast.Expr {
	if idx == -1 {
		if sel, ok := unparen(call.Fun).(*ast.SelectorExpr); ok {
			return sel.X
		}
		return nil
	}
	if idx < len(call.Args) {
		return call.Args[idx]
	}
	return nil
}

// ---- generic flow walker ----------------------------------------------------

// flow is a forward, flow-sensitive walk over a function body, parameterized by
// the ownership lattice element T (binding for reporting, own for inference). It
// threads a per-variable environment through control flow, copying it into
// branches and merging at join points (conservatively, via merge), so a value's
// ownership reflects the path that reaches each use.
type flow[T any] struct {
	c        *checker
	owned    T
	merge    func(a, b T) T
	classify func(ast.Expr, map[*types.Var]T) T
	callRes  func(*ast.CallExpr, int, map[*types.Var]T) T
	onExprs  func([]ast.Expr, map[*types.Var]T)      // per-statement expression hook (sinks)
	onAssign func(*ast.AssignStmt, map[*types.Var]T) // field-assignment hook
	onReturn func([]ast.Expr, map[*types.Var]T)      // return hook (result inference)
	onSend   func(ast.Expr, map[*types.Var]T)
}

func cloneEnv[T any](e map[*types.Var]T) map[*types.Var]T {
	out := make(map[*types.Var]T, len(e))
	for k, v := range e {
		out[k] = v
	}
	return out
}

func (f *flow[T]) run(body *ast.BlockStmt) {
	if body != nil {
		f.walk(body.List, map[*types.Var]T{})
	}
}

func (f *flow[T]) walk(stmts []ast.Stmt, env map[*types.Var]T) {
	for _, s := range stmts {
		f.step(s, env)
	}
}

func (f *flow[T]) exprs(es []ast.Expr, env map[*types.Var]T) {
	if f.onExprs != nil {
		f.onExprs(es, env)
	}
}

func (f *flow[T]) step(stmt ast.Stmt, env map[*types.Var]T) {
	switch s := stmt.(type) {
	case *ast.AssignStmt:
		f.exprs(s.Rhs, env)
		if f.onAssign != nil {
			f.onAssign(s, env)
		}
		f.applyAssign(s, env)
	case *ast.ExprStmt:
		f.exprs([]ast.Expr{s.X}, env)
	case *ast.ReturnStmt:
		f.exprs(s.Results, env)
		if f.onReturn != nil {
			f.onReturn(s.Results, env)
		}
	case *ast.SendStmt:
		f.exprs([]ast.Expr{s.Value}, env)
		if f.onSend != nil {
			f.onSend(s.Value, env)
		}
	case *ast.GoStmt:
		f.exprs([]ast.Expr{s.Call}, env)
	case *ast.DeferStmt:
		f.exprs([]ast.Expr{s.Call}, env)
	case *ast.DeclStmt:
		if gd, ok := s.Decl.(*ast.GenDecl); ok {
			for _, spec := range gd.Specs {
				if vs, ok := spec.(*ast.ValueSpec); ok {
					f.exprs(vs.Values, env)
					if len(vs.Values) == 1 && len(vs.Names) > 1 {
						if call, ok := unparen(vs.Values[0]).(*ast.CallExpr); ok {
							for i, name := range vs.Names {
								f.bind(env, name, f.callRes(call, i, env))
							}
							continue
						}
					}
					if len(vs.Names) == len(vs.Values) {
						for i, name := range vs.Names {
							f.bind(env, name, f.classify(vs.Values[i], env))
						}
					}
				}
			}
		}
	case *ast.BlockStmt:
		f.walk(s.List, env)
	case *ast.LabeledStmt:
		f.step(s.Stmt, env)
	case *ast.IfStmt:
		if s.Init != nil {
			f.step(s.Init, env)
		}
		f.exprs([]ast.Expr{s.Cond}, env)
		thenEnv := cloneEnv(env)
		f.walk(s.Body.List, thenEnv)
		branches := []map[*types.Var]T{thenEnv}
		if s.Else != nil {
			elseEnv := cloneEnv(env)
			f.step(s.Else, elseEnv)
			branches = append(branches, elseEnv)
		}
		f.join(env, branches, s.Else != nil)
	case *ast.ForStmt:
		if s.Init != nil {
			f.step(s.Init, env)
		}
		if s.Cond != nil {
			f.exprs([]ast.Expr{s.Cond}, env)
		}
		body := cloneEnv(env)
		f.walk(s.Body.List, body)
		f.join(env, []map[*types.Var]T{body}, false)
	case *ast.RangeStmt:
		f.exprs([]ast.Expr{s.X}, env)
		body := cloneEnv(env)
		f.walk(s.Body.List, body)
		f.join(env, []map[*types.Var]T{body}, false)
	case *ast.SwitchStmt:
		if s.Init != nil {
			f.step(s.Init, env)
		}
		if s.Tag != nil {
			f.exprs([]ast.Expr{s.Tag}, env)
		}
		f.cases(s.Body, env)
	case *ast.TypeSwitchStmt:
		if s.Init != nil {
			f.step(s.Init, env)
		}
		f.cases(s.Body, env)
	case *ast.SelectStmt:
		var branches []map[*types.Var]T
		for _, cc := range s.Body.List {
			if comm, ok := cc.(*ast.CommClause); ok {
				ce := cloneEnv(env)
				if comm.Comm != nil {
					f.step(comm.Comm, ce)
				}
				f.walk(comm.Body, ce)
				branches = append(branches, ce)
			}
		}
		f.join(env, branches, len(branches) > 0) // select runs exactly one clause
	}
	// Other statement kinds (branch, empty, inc/dec, ...) carry no ownership.
}

func (f *flow[T]) cases(body *ast.BlockStmt, env map[*types.Var]T) {
	var branches []map[*types.Var]T
	hasDefault := false
	for _, cc := range body.List {
		cl, ok := cc.(*ast.CaseClause)
		if !ok {
			continue
		}
		if cl.List == nil {
			hasDefault = true
		}
		f.exprs(cl.List, env)
		ce := cloneEnv(env)
		f.walk(cl.Body, ce)
		branches = append(branches, ce)
	}
	f.join(env, branches, hasDefault)
}

// join replaces env with the merge of the branch environments; unless the
// branches are exhaustive, the fall-through (pre-branch) env is also merged in.
func (f *flow[T]) join(env map[*types.Var]T, branches []map[*types.Var]T, exhaustive bool) {
	if len(branches) == 0 {
		return
	}
	all := branches
	if !exhaustive {
		all = append([]map[*types.Var]T{cloneEnv(env)}, branches...)
	}
	merged := cloneEnv(all[0])
	for _, e := range all[1:] {
		for k, vb := range e {
			if va, ok := merged[k]; ok {
				merged[k] = f.merge(va, vb)
			} else {
				merged[k] = vb
			}
		}
	}
	for k := range env {
		delete(env, k)
	}
	for k, v := range merged {
		env[k] = v
	}
}

func (f *flow[T]) bind(env map[*types.Var]T, lhs ast.Expr, v T) {
	if id, ok := lhs.(*ast.Ident); ok {
		if vv, ok := f.c.pass.TypesInfo.ObjectOf(id).(*types.Var); ok {
			env[vv] = v
		}
	}
}

// applyAssign updates env for an assignment (RHS read with the pre-assignment env).
func (f *flow[T]) applyAssign(as *ast.AssignStmt, env map[*types.Var]T) {
	if len(as.Rhs) == 1 {
		if call, ok := unparen(as.Rhs[0]).(*ast.CallExpr); ok {
			vs := make([]T, len(as.Lhs))
			for i := range as.Lhs {
				vs[i] = f.callRes(call, i, env)
			}
			for i, lhs := range as.Lhs {
				f.bind(env, lhs, vs[i])
			}
			return
		}
		if len(as.Lhs) == 2 {
			switch r := unparen(as.Rhs[0]).(type) {
			case *ast.TypeAssertExpr:
				f.bind(env, as.Lhs[0], f.classify(r.X, env))
				f.bind(env, as.Lhs[1], f.owned)
				return
			case *ast.IndexExpr:
				f.bind(env, as.Lhs[0], f.classify(r.X, env))
				f.bind(env, as.Lhs[1], f.owned)
				return
			}
		}
	}
	if len(as.Lhs) == len(as.Rhs) {
		vs := make([]T, len(as.Rhs))
		for i := range as.Rhs {
			vs[i] = f.classify(as.Rhs[i], env)
		}
		for i, lhs := range as.Lhs {
			f.bind(env, lhs, vs[i])
		}
	}
}

// ---- reporting: flow-sensitive ownership with provenance --------------------

// binding is the resolved ownership of a value at a concrete site, with the flow
// path from its borrowed source (for the diagnostic).
type binding struct {
	kind ownKind // owned or borrowed (never viaInput — resolved)
	path []analysis.RelatedInformation
}

func ownedBinding() binding { return binding{kind: owned} }

func (b binding) hop(pos token.Pos, msg string) binding {
	if b.kind != borrowed {
		return b
	}
	return binding{kind: borrowed, path: append(append([]analysis.RelatedInformation{}, b.path...),
		analysis.RelatedInformation{Pos: pos, Message: msg})}
}

// ownEnv maps a local variable to its ownership at the current program point.
// Flow-sensitive: branches get copies, joined conservatively (borrowed wins) at
// merge points, so a value cloned on only one path is still borrowed afterward.
// ownEnv aliases the generic flow environment for the binding (report) domain, so
// the flowCtx methods below plug directly into flow[binding].
type ownEnv = map[*types.Var]binding

func mergeBinding(a, b binding) binding {
	if a.kind == borrowed {
		return a
	}
	return b // owned ∨ x → x; the borrowed one (with its path) wins
}

// flowCtx is the per-function reporting domain: it classifies expressions to
// binding and emits diagnostics; the generic flow[binding] walker drives it.
type flowCtx struct {
	c       *checker
	recv    *types.Var
	esc     map[*types.Var]bool
	escLits map[*ast.CompositeLit]bool
	ignore  map[int]bool
}

func (c *checker) report() {
	for _, file := range c.pass.Files {
		if f := c.pass.Fset.File(file.Pos()); f != nil && strings.HasSuffix(f.Name(), "_test.go") {
			continue
		}
		ignore := c.ignoreLines(file)
		for _, d := range file.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil || !fd.Name.IsExported() {
				continue
			}
			fc := &flowCtx{c: c, ignore: ignore}
			if fd.Recv != nil {
				for _, f := range fd.Recv.List {
					for _, n := range f.Names {
						if v, ok := c.pass.TypesInfo.Defs[n].(*types.Var); ok {
							fc.recv = v
						}
					}
				}
			}
			fc.esc = c.escapingVars(fd)
			fc.escLits = c.escapingLits(fd, fc.esc)
			f := &flow[binding]{
				c:        c,
				owned:    ownedBinding(),
				merge:    mergeBinding,
				classify: fc.classify,
				callRes:  fc.callResult,
				onExprs:  fc.checkExprs,
				onAssign: fc.checkFieldAssign,
			}
			f.run(fd.Body)
		}
	}
}

func (fc *flowCtx) classify(expr ast.Expr, env ownEnv) binding {
	switch e := unparen(expr).(type) {
	case *ast.Ident:
		v, ok := fc.c.pass.TypesInfo.ObjectOf(e).(*types.Var)
		if !ok {
			return ownedBinding()
		}
		if b, ok := env[v]; ok {
			return b
		}
		if v == fc.recv {
			return binding{kind: borrowed, path: []analysis.RelatedInformation{
				{Pos: e.Pos(), Message: "borrowed from the receiver"}}}
		}
		return ownedBinding() // parameter / global / unresolved local
	case *ast.SelectorExpr:
		if _, isSel := fc.c.pass.TypesInfo.Selections[e]; isSel {
			return fc.classify(e.X, env).hop(e.Sel.Pos(), "field "+e.Sel.Name)
		}
		return ownedBinding()
	case *ast.IndexExpr:
		return fc.classify(e.X, env)
	case *ast.SliceExpr:
		return fc.classify(e.X, env)
	case *ast.StarExpr:
		return fc.classify(e.X, env)
	case *ast.TypeAssertExpr:
		return fc.classify(e.X, env)
	case *ast.CallExpr:
		return fc.callResult(e, 0, env)
	}
	return ownedBinding()
}

func (fc *flowCtx) callResult(call *ast.CallExpr, i int, env ownEnv) binding {
	if fc.c.isSanitizer(call) {
		return ownedBinding()
	}
	callee := fc.c.calleeFunc(call)
	if callee == nil {
		return ownedBinding()
	}
	cs := fc.c.signatureOf(callee)
	if cs == nil || i >= len(cs.Results) {
		return ownedBinding()
	}
	r := cs.Results[i]
	switch r.Kind {
	case owned:
		return ownedBinding()
	case borrowed:
		return binding{kind: borrowed, path: []analysis.RelatedInformation{
			{Pos: call.Pos(), Message: "borrowed result of " + callee.Name()}}}
	case viaInput:
		// Resolve every member of the input set; the result is borrowed if any
		// member is, carrying that member's flow path.
		out := ownedBinding()
		for _, idx := range r.Idxs {
			arg := callArg(call, idx)
			if arg == nil {
				continue
			}
			out = mergeBinding(out, fc.classify(arg, env).hop(call.Pos(), "returned by "+callee.Name()))
		}
		return out
	}
	return ownedBinding()
}

// ---- sinks: classify the embedded value and flag if borrowed ---------------

func (fc *flowCtx) flag(pos token.Pos, msg string, path []analysis.RelatedInformation) {
	line := fc.c.pass.Fset.Position(pos).Line
	if fc.ignore[line] || fc.ignore[line-1] {
		return
	}
	fc.c.pass.Report(analysis.Diagnostic{Pos: pos, Message: msg, Related: path})
}

// checkExprs runs the embed and call-arg sinks over the given expressions (not
// descending into nested statement bodies or closures), classified with env.
func (fc *flowCtx) checkExprs(exprs []ast.Expr, env ownEnv) {
	for _, expr := range exprs {
		if expr == nil {
			continue
		}
		ast.Inspect(expr, func(n ast.Node) bool {
			if _, ok := n.(*ast.FuncLit); ok {
				return false
			}
			switch x := n.(type) {
			case *ast.CompositeLit:
				if fc.escLits[x] {
					fc.checkLiteralFields(x, env)
				}
			case *ast.CallExpr:
				fc.checkCallArgsLeak(x, env)
			}
			return true
		})
	}
}

func (fc *flowCtx) checkLiteralFields(cl *ast.CompositeLit, env ownEnv) {
	t := fc.c.pass.TypesInfo.TypeOf(cl)
	if t == nil || !fc.c.isSink(t) {
		return
	}
	for _, elt := range cl.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		vt := fc.c.pass.TypesInfo.TypeOf(kv.Value)
		if !fc.c.tracked(vt) {
			continue
		}
		if b, ok := fc.reportableBorrowed(vt, kv.Value, env); ok {
			fc.flag(kv.Value.Pos(),
				"borrowed "+kindOf(vt)+" embedded into "+typeName(t)+" field "+fieldName(kv.Key)+
					" without clone; clone it (e.g. maps.Clone) before embedding to avoid a "+
					"concurrent-access crash while it is serialized", b.path)
		}
	}
}

func (fc *flowCtx) checkFieldAssign(as *ast.AssignStmt, env ownEnv) {
	if len(as.Lhs) != len(as.Rhs) {
		return
	}
	for i, lhs := range as.Lhs {
		sel, ok := lhs.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		ft := fc.c.pass.TypesInfo.TypeOf(sel)
		if !fc.c.tracked(ft) {
			continue
		}
		if h := fc.c.pass.TypesInfo.TypeOf(sel.X); h == nil || !fc.c.isSink(h) {
			continue
		}
		if v := fc.c.rootVar(sel.X); v == nil || !fc.esc[v] {
			continue
		}
		rhs := as.Rhs[i]
		if _, isLit := unparen(rhs).(*ast.CompositeLit); isLit {
			continue
		}
		if b, ok := fc.reportableBorrowed(ft, rhs, env); ok {
			fc.flag(rhs.Pos(),
				"borrowed "+kindOf(ft)+" assigned to "+typeName(fc.c.pass.TypesInfo.TypeOf(sel.X))+" field "+sel.Sel.Name+
					" without clone; clone it (e.g. maps.Clone) before assigning to avoid a "+
					"concurrent-access crash while it is serialized", b.path)
		}
	}
}

func (fc *flowCtx) reportableBorrowed(t types.Type, expr ast.Expr, env ownEnv) (binding, bool) {
	b := fc.classify(expr, env)
	if b.kind != borrowed {
		return b, false
	}
	if kindOf(t) != "slice" {
		return b, true
	}
	src := fc.c.sliceSource(expr, nil)
	if src == nil {
		return b, true
	}
	return b, fc.c.sliceWrites[src] != sliceReplaceOnly
}

func (fc *flowCtx) checkCallArgsLeak(call *ast.CallExpr, env ownEnv) {
	callee := fc.c.calleeFunc(call)
	if callee == nil {
		return
	}
	cs := fc.c.signatureOf(callee)
	if cs == nil {
		return
	}
	for j, arg := range call.Args {
		if j >= len(cs.Leaks) || !cs.Leaks[j] {
			continue
		}
		at := fc.c.pass.TypesInfo.TypeOf(arg)
		if !fc.c.tracked(at) {
			continue
		}
		if b := fc.classify(arg, env); b.kind == borrowed {
			fc.flag(arg.Pos(),
				"borrowed "+kindOf(at)+" passed to "+callee.Name()+
					", which embeds it without clone; clone it (e.g. maps.Clone) first", b.path)
		}
	}
}

// ---- escape detection: which values/protos leave the function -------------

// escapingVars returns local variables whose value reaches a return (directly or
// via an alias chain), plus proto-pointer parameters (output protos the caller
// owns and may marshal).
func (c *checker) escapingVars(fd *ast.FuncDecl) map[*types.Var]bool {
	esc := map[*types.Var]bool{}
	if fd.Type.Params != nil {
		for _, f := range fd.Type.Params.List {
			for _, n := range f.Names {
				if v, ok := c.pass.TypesInfo.Defs[n].(*types.Var); ok {
					if ptr, ok := v.Type().(*types.Pointer); ok && c.isSink(ptr.Elem()) {
						esc[v] = true
					}
				}
			}
		}
	}
	if fd.Type.Results != nil {
		for _, f := range fd.Type.Results.List {
			for _, n := range f.Names {
				if v, ok := c.pass.TypesInfo.Defs[n].(*types.Var); ok {
					esc[v] = true // named results always escape (they are returned)
				}
			}
		}
	}
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		if ret, ok := n.(*ast.ReturnStmt); ok {
			for _, r := range ret.Results {
				if v := c.identVar(returnedExpr(r)); v != nil {
					esc[v] = true
				}
			}
		}
		return true
	})
	for changed := true; changed; {
		changed = false
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if _, ok := n.(*ast.FuncLit); ok {
				return false
			}
			as, ok := n.(*ast.AssignStmt)
			if !ok || len(as.Lhs) != len(as.Rhs) {
				return true
			}
			for i, lhs := range as.Lhs {
				if lv := c.identVar(lhs); lv != nil && esc[lv] {
					if rv := c.identVar(returnedExpr(as.Rhs[i])); rv != nil && !esc[rv] {
						esc[rv] = true
						changed = true
					}
				}
			}
			return true
		})
	}
	return esc
}

// escapingLits returns composite literals that escape: directly returned (or
// nested within one), or assigned to an escaping variable or a field of one.
func (c *checker) escapingLits(fd *ast.FuncDecl, esc map[*types.Var]bool) map[*ast.CompositeLit]bool {
	lits := map[*ast.CompositeLit]bool{}
	var collect func(ast.Expr)
	collect = func(e ast.Expr) {
		switch x := unparen(e).(type) {
		case *ast.UnaryExpr:
			collect(x.X)
		case *ast.CompositeLit:
			lits[x] = true
			for _, elt := range x.Elts {
				v := elt
				if kv, ok := elt.(*ast.KeyValueExpr); ok {
					v = kv.Value
				}
				collect(v)
			}
		}
	}
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		switch node := n.(type) {
		case *ast.ReturnStmt:
			for _, r := range node.Results {
				collect(r)
			}
		case *ast.AssignStmt:
			if len(node.Lhs) != len(node.Rhs) {
				return true
			}
			for i, lhs := range node.Lhs {
				if v := c.rootVar(lhs); v != nil && esc[v] {
					collect(node.Rhs[i])
				}
			}
		case *ast.CallExpr:
			// a literal passed to a parameter that escapes the callee escapes too
			if callee := c.calleeFunc(node); callee != nil {
				if cs := c.signatureOf(callee); cs != nil {
					for j, arg := range node.Args {
						if j < len(cs.Leaks) && cs.Leaks[j] {
							collect(arg)
						}
					}
				}
			}
		}
		return true
	})
	return lits
}

// ---- directives: //ownership:result | param | ignore ----------------------

var (
	reResultNamed = regexp.MustCompile(`//ownership:result\s+(\w+)\s+(owned|borrowed)\b`)
	reResultBare  = regexp.MustCompile(`//ownership:result\s+(owned|borrowed)\b`)
	reIgnore      = regexp.MustCompile(`//ownership:ignore\s+\S`)
	// Named, in the func doc — Go does not reliably attach comments to individual
	// parameters, so the contract is stated by parameter name.
	reParamEscapes = regexp.MustCompile(`//ownership:param\s+(\w+)\s+escapes\b`)
)

// paramEscapes returns the parameter indices declared //ownership:param <name>
// escapes in the function's doc comment. Such a parameter's data is retained or
// marshaled by the callee, so passing a borrowed value to it leaks.
func (c *checker) paramEscapes(fd *ast.FuncDecl) map[int]bool {
	esc := map[int]bool{}
	if fd.Doc == nil || fd.Type.Params == nil {
		return esc
	}
	name2idx := map[string]int{}
	pos := 0
	for _, field := range fd.Type.Params.List {
		if len(field.Names) == 0 {
			pos++
			continue
		}
		for _, n := range field.Names {
			name2idx[n.Name] = pos
			pos++
		}
	}
	for _, com := range fd.Doc.List {
		if m := reParamEscapes.FindStringSubmatch(com.Text); m != nil {
			if i, ok := name2idx[m[1]]; ok {
				esc[i] = true
			}
		}
	}
	return esc
}

// resultAnnotations parses //ownership:result directives in a function's doc into
// per-result ownership overrides. The bare form (//ownership:result owned|borrowed)
// applies to all nr results; the named form (//ownership:result <name> ...) targets
// a single named result.
func (c *checker) resultAnnotations(fd *ast.FuncDecl, nr int) map[int]ownKind {
	out := map[int]ownKind{}
	if fd.Doc == nil {
		return out
	}
	name2idx := map[string]int{}
	if fd.Type.Results != nil {
		pos := 0
		for _, field := range fd.Type.Results.List {
			if len(field.Names) == 0 {
				pos++
				continue
			}
			for _, n := range field.Names {
				name2idx[n.Name] = pos
				pos++
			}
		}
	}
	kindOfWord := func(w string) ownKind {
		if w == "owned" {
			return owned
		}
		return borrowed
	}
	for _, com := range fd.Doc.List {
		if m := reResultNamed.FindStringSubmatch(com.Text); m != nil {
			if i, ok := name2idx[m[1]]; ok {
				out[i] = kindOfWord(m[2])
			}
			continue
		}
		if m := reResultBare.FindStringSubmatch(com.Text); m != nil {
			for i := 0; i < nr; i++ {
				out[i] = kindOfWord(m[1])
			}
		}
	}
	return out
}

func (c *checker) ignoreLines(file *ast.File) map[int]bool {
	lines := map[int]bool{}
	for _, cg := range file.Comments {
		for _, com := range cg.List {
			if reIgnore.MatchString(com.Text) {
				lines[c.pass.Fset.Position(com.Pos()).Line] = true
			}
		}
	}
	return lines
}

// ---- helpers: sink/sanitizer predicates and small AST/type utilities ------

func (c *checker) calleeFunc(call *ast.CallExpr) *types.Func {
	switch fun := unparen(call.Fun).(type) {
	case *ast.Ident:
		fn, _ := c.pass.TypesInfo.ObjectOf(fun).(*types.Func)
		return fn
	case *ast.SelectorExpr:
		fn, _ := c.pass.TypesInfo.ObjectOf(fun.Sel).(*types.Func)
		return fn
	}
	return nil
}

// isSanitizer reports whether a call yields an owned value (a configured
// pkg.Func, e.g. a clone/copy helper).
func (c *checker) isSanitizer(call *ast.CallExpr) bool {
	sel, ok := unparen(call.Fun).(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return c.sanitizers[pkg.Name][sel.Sel.Name]
}

// typeName returns the (dereferenced) named type's short name, for diagnostics.
func typeName(t types.Type) string {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	if n, ok := t.(*types.Named); ok {
		return n.Obj().Name()
	}
	return "value"
}

// isSink reports whether t is a sink type — one a borrowed value must not be
// embedded into (it gets serialized / read outside locks). Recognized by a
// configured marker method (default: ProtoReflect, i.e. a protobuf message).
func (c *checker) isSink(t types.Type) bool {
	if v, ok := c.sinkCache[t]; ok {
		return v
	}
	v := c.hasMarker(t)
	c.sinkCache[t] = v
	return v
}

func (c *checker) hasMarker(t types.Type) bool {
	for _, typ := range []types.Type{t, types.NewPointer(t)} {
		ms := types.NewMethodSet(typ)
		for i := 0; i < ms.Len(); i++ {
			if c.sinkMeths[ms.At(i).Obj().Name()] {
				return true
			}
		}
		for _, iface := range c.sinkIfaces {
			if types.Implements(typ, iface) {
				return true
			}
		}
	}
	return false
}

// tracked reports whether t is a hazard the embed checks should flag: a reference
// kind selected by -value-kinds that can actually be mutated concurrently.
//
//   - map: always a hazard (iterated and mutated in place during marshal).
//   - slice: a hazard unless it is a []byte (a blob/token, not a mutable collection).
//   - conduit kinds (pointer, interface, chan, func): a hazard only if the value
//     transitively REACHES a map/slice leaf — embedding a borrowed pointer to a
//     scalar-only message (timestamp, duration, ...) can never produce the crash.
func (c *checker) tracked(t types.Type) bool {
	if t == nil {
		return false
	}
	k := kindOf(t)
	if !c.valueKinds[k] {
		return false
	}
	switch k {
	case "map":
		return true
	case "slice":
		return !isByteSlice(t)
	default:
		return c.reachesLeaf(t)
	}
}

// reachesLeaf reports whether t transitively contains a hazard leaf — a map or a
// non-byte slice, the kinds that actually crash on concurrent access. A conduit
// (pointer/interface) only matters if it reaches such a leaf. An interface field is
// treated conservatively as reaching one (its dynamic type is unknown). Results are
// memoized; recursion is cycle-guarded for self-referential message types.
func (c *checker) reachesLeaf(t types.Type) bool {
	if v, ok := c.leafCache[t]; ok {
		return v
	}
	c.leafCache[t] = false // break cycles: assume no leaf until proven otherwise
	v := reaches(t, map[types.Type]bool{})
	c.leafCache[t] = v
	return v
}

func reaches(t types.Type, seen map[types.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	switch u := t.Underlying().(type) {
	case *types.Map:
		return true
	case *types.Slice:
		return !isByteSlice(t)
	case *types.Array:
		return reaches(u.Elem(), seen)
	case *types.Pointer:
		return reaches(u.Elem(), seen)
	case *types.Interface:
		return true // unknown dynamic type: conservatively a possible leaf
	case *types.Struct:
		// Only the exported (marshaled) fields matter: generated proto structs embed
		// unexported plumbing (protoimpl.MessageState, sizeCache, unknownFields) that
		// transitively reaches maps and would make every message look like a hazard.
		for i := 0; i < u.NumFields(); i++ {
			if u.Field(i).Exported() && reaches(u.Field(i).Type(), seen) {
				return true
			}
		}
	}
	return false
}

// isByteSlice reports whether t is a []byte (or a named type over one): a slice
// whose element is the byte/uint8 basic type.
func isByteSlice(t types.Type) bool {
	s, ok := t.Underlying().(*types.Slice)
	if !ok {
		return false
	}
	b, ok := s.Elem().Underlying().(*types.Basic)
	return ok && b.Kind() == types.Uint8
}

// kindOf names t's underlying reference kind (for diagnostics and -value-kinds
// matching), or "" if it is not a reference type.
func kindOf(t types.Type) string {
	switch t.Underlying().(type) {
	case *types.Map:
		return "map"
	case *types.Slice:
		return "slice"
	case *types.Pointer:
		return "pointer"
	case *types.Interface:
		return "interface"
	case *types.Chan:
		return "chan"
	case *types.Signature:
		return "func"
	}
	return ""
}

func fieldName(key ast.Expr) string {
	if id, ok := key.(*ast.Ident); ok {
		return id.Name
	}
	return "?"
}

func returnedExpr(e ast.Expr) ast.Expr {
	e = unparen(e)
	if u, ok := e.(*ast.UnaryExpr); ok {
		return unparen(u.X)
	}
	return e
}

func (c *checker) rootVar(e ast.Expr) *types.Var {
	for {
		switch x := unparen(e).(type) {
		case *ast.SelectorExpr:
			e = x.X
		case *ast.IndexExpr:
			e = x.X
		case *ast.StarExpr:
			e = x.X
		default:
			return c.identVar(unparen(e))
		}
	}
}

func (c *checker) identVar(e ast.Expr) *types.Var {
	id, ok := e.(*ast.Ident)
	if !ok {
		return nil
	}
	v, _ := c.pass.TypesInfo.ObjectOf(id).(*types.Var)
	return v
}

func unparen(e ast.Expr) ast.Expr {
	for {
		p, ok := e.(*ast.ParenExpr)
		if !ok {
			return e
		}
		e = p.X
	}
}

func anyBool(b []bool) bool {
	for _, v := range b {
		if v {
			return true
		}
	}
	return false
}

func sigEqual(a, b *signature) bool {
	if len(a.Results) != len(b.Results) || len(a.Leaks) != len(b.Leaks) || len(a.Fresh) != len(b.Fresh) {
		return false
	}
	for i := range a.Results {
		if !ownEqual(a.Results[i], b.Results[i]) {
			return false
		}
	}
	for i := range a.Leaks {
		if a.Leaks[i] != b.Leaks[i] {
			return false
		}
	}
	for i := range a.Fresh {
		if a.Fresh[i] != b.Fresh[i] {
			return false
		}
	}
	return true
}

// unionInts returns the sorted, deduplicated union of two int slices (the merge of
// two viaInput sets; also used to canonicalize a single set when b is nil).
func unionInts(a, b []int) []int {
	seen := map[int]bool{}
	var out []int
	for _, s := range [][]int{a, b} {
		for _, v := range s {
			if !seen[v] {
				seen[v] = true
				out = append(out, v)
			}
		}
	}
	sort.Ints(out)
	return out
}

func intsEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
