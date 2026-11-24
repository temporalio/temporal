package parallelsuitelint

import (
	"go/token"
	"go/types"
)

// Export data can omit declarations from packages that are imported only transitively.
func matchesAwaitSignature(fn *types.Func, awaitType *types.Named) bool {
	switch fn.Name() {
	case "Rcv", "Snd":
		return matchesAwaitChannelSignature(fn)
	case "Require", "Requiref", "RequireTrue", "RequireTruef":
		return matchesAwaitPollingSignature(fn, awaitType)
	default:
		return false
	}
}

func matchesAwaitChannelSignature(fn *types.Func) bool {
	sig := fn.Signature()
	params, results := sig.Params(), sig.Results()
	if sig.TypeParams().Len() != 1 || sig.Variadic() || params.Len() < 2 || !isNamedType(params.At(0).Type(), "testing", "TB") {
		return false
	}
	element := sig.TypeParams().At(0)
	if !types.Identical(element.Constraint(), types.Universe.Lookup("any").Type()) {
		return false
	}
	if fn.Name() == "Rcv" {
		return params.Len() == 2 && results.Len() == 1 &&
			types.Identical(params.At(1).Type(), types.NewChan(types.RecvOnly, element)) &&
			types.Identical(results.At(0).Type(), element)
	}
	return params.Len() == 3 && results.Len() == 0 &&
		types.Identical(params.At(1).Type(), types.NewChan(types.SendOnly, element)) &&
		types.Identical(params.At(2).Type(), element)
}

func matchesAwaitPollingSignature(fn *types.Func, awaitType *types.Named) bool {
	sig := fn.Signature()
	name := fn.Name()
	hasContext := name == "Require" || name == "Requiref"
	formatted := name == "Requiref" || name == "RequireTruef"
	handleIndex, arity := 0, 4
	if hasContext {
		handleIndex++
		arity++
	}
	if formatted {
		arity += 2
	}
	params := sig.Params()
	if params.Len() != arity || sig.Results().Len() != 0 || sig.TypeParams().Len() != 0 || sig.Variadic() != formatted {
		return false
	}
	if hasContext && !isNamedType(params.At(0).Type(), "context", "Context") {
		return false
	}
	if !isNamedType(params.At(handleIndex).Type(), "testing", "TB") ||
		!isNamedType(params.At(handleIndex+2).Type(), "time", "Duration") ||
		!isNamedType(params.At(handleIndex+3).Type(), "time", "Duration") {
		return false
	}
	callbackParams := types.NewTuple()
	callbackResults := types.NewTuple(types.NewVar(token.NoPos, nil, "", types.Typ[types.Bool]))
	if hasContext {
		callbackParams = types.NewTuple(types.NewVar(token.NoPos, nil, "", types.NewPointer(awaitType)))
		callbackResults = types.NewTuple()
	}
	callback := types.NewSignatureType(nil, nil, nil, callbackParams, callbackResults, false)
	if !types.Identical(params.At(handleIndex+1).Type(), callback) {
		return false
	}
	return !formatted || types.Identical(params.At(arity-2).Type(), types.Typ[types.String]) &&
		types.Identical(params.At(arity-1).Type(), types.NewSlice(types.Universe.Lookup("any").Type()))
}

func isNamedType(t types.Type, path, name string) bool {
	named, ok := types.Unalias(t).(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Pkg() != nil && obj.Pkg().Path() == path && obj.Name() == name
}
