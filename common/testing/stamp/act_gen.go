package stamp

import (
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"

	"pgregory.net/rapid"
)

// TODO: use Go fuzzer instead of rapid? what's the shrinking like?
// checkout https://pkg.go.dev/github.com/AdaLogics/go-fuzz-headers#section-readme

var (
	_           genContextProvider = &genContext{}
	globalGenID atomic.Int32
)

// TODO: distinguish between discrete and continuous generators?

type (
	// Gen is a generator for a type T.
	Gen[T any] struct {
		id        genID
		generator *rapid.Generator[T]
		choices   []T
		name      string
		static    bool // true if a static/fixed value is always returned (see GenJust)
	}
	genID int

	Generator[T any] interface {
		// Next generates a value of type T based on the generator context.
		Next(GenContext) T
	}

	GenContext interface {
		genContext() *genContext
		AllowRandom() *genContext
	}
	genContext struct {
		baseSeed     int
		allowRandom  bool
		pickChoiceFn func(string, int) int
		// seedPerGen is a map of generator pointers to their seed values.
		// Whenever a generator is used, its seed is incremented.
		// This ensures that their output is deterministic, but novel for every call.
		seedPerGen map[genID]int
	}
	genContextProvider interface {
		genContext() *genContext
	}

	ListGen[T any] []Generator[T]

	// genDefault is an interface that allows a type to provide a default generator.
	genDefault[T any] interface {
		DefaultGen() Gen[T]
	}
)

func (g Gen[T]) String() string {
	var details []string
	if g.name != "" {
		details = append(details, fmt.Sprintf("name: %s", g.name))
	}
	if len(g.choices) > 0 {
		details = append(details, fmt.Sprintf("choices: %d", len(g.choices)))
	}
	res := fmt.Sprintf("Gen[%s]", reflect.TypeFor[T]())
	if len(details) > 0 {
		res = res + fmt.Sprintf("(%s)", strings.Join(details, ", "))
	}
	return res
}

func newGenContext(seed int) *genContext {
	return &genContext{
		baseSeed:   seed,
		seedPerGen: make(map[genID]int),
	}
}

func GenJust[T any](val T) Gen[T] {
	return Gen[T]{
		id:        genID(globalGenID.Add(1)),
		generator: rapid.Just(val),
		static:    true,
	}
}

func GenInt[T ~int](min, max T) Gen[T] {
	return newGenImpl("Int", func(t *rapid.T) T {
		return T(rapid.IntRange(int(min), int(max)).Draw(t, ""))
	})
}

func GenBool[T ~bool]() Gen[T] {
	return newGenImpl("Bool", func(t *rapid.T) T {
		return T(rapid.Bool().Draw(t, ""))
	})
}

func GenName[T ~string]() Gen[T] {
	return newGenImpl("Name", func(t *rapid.T) T {
		return T(fmt.Sprintf("%v-%v",
			rapid.SampledFrom(adjectives).Draw(t, ""),
			rapid.SampledFrom(names).Draw(t, "")))
	})
}

func GenChoice[T any](name string, choices ...T) Gen[T] {
	if name == "" {
		panic("name must not be empty")
	}
	if len(choices) == 0 {
		panic("choices must not be empty")
	}
	return Gen[T]{
		id:        genID(globalGenID.Add(1)),
		generator: rapid.SampledFrom(choices),
		choices:   choices,
		name:      fmt.Sprintf("Choice(%s)", name),
	}
}

func GenList[T any](items ...Generator[T]) []Generator[T] {
	return items
}

func (g Gen[T]) Next(gcp genContextProvider) T {
	var zero T
	return g.NextOrDefault(gcp, zero)
}

func (g Gen[T]) Name() string {
	return g.name
}

func (g Gen[T]) ID() genID {
	return g.id
}

func (g Gen[T]) AsJust(gcp genContextProvider) Gen[T] {
	return GenJust(g.Next(gcp))
}

func (g Gen[T]) NextOrDefault(gcp genContextProvider, defaultVal T) T {
	ctx := gcp.genContext()

	// Use generator context pick a choice, if defined.
	// This is useful in scenario macros to select all choices.
	if len(g.choices) > 0 && ctx.pickChoiceFn != nil {
		if g.id == 0 {
			panic("generator ID is not set")
		}
		return g.choices[ctx.pickChoiceFn(g.String(), len(g.choices))]
	}

	// Use generator, if defined.
	if g.generator != nil {
		if g.id == 0 {
			panic("generator ID is not set")
		}
		// TODO
		//if !ctx.allowRandom && !g.static {
		//	panic(fmt.Sprintf("generator %q is random, which is not allowed here", g.name))
		//}
		if _, ok := ctx.seedPerGen[g.id]; !ok {
			ctx.seedPerGen[g.id] = ctx.baseSeed
		}
		ctx.seedPerGen[g.id]++
		return g.generator.Example(ctx.seedPerGen[g.id])
	}

	// Use default generator of T, if defined.
	if dg, ok := any(defaultVal).(genDefault[T]); ok {
		return dg.DefaultGen().Next(ctx)
	}

	// Otherwise, return the default value of T.
	return defaultVal
}

func (g *genContext) genContext() *genContext {
	return g
}

// AllowRandom returns a new generator that allows generating random values.
// NOTE: it must return a (shallow) copy of the context!
func (g genContext) AllowRandom() *genContext {
	g.allowRandom = true
	return &g
}

func newGenImpl[T any](name string, fn func(t *rapid.T) T) Gen[T] {
	return Gen[T]{
		id:        genID(globalGenID.Add(1)),
		name:      name,
		generator: rapid.Custom(fn),
	}
}
