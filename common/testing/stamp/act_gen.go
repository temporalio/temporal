package stamp

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
)

// TODO: use Go fuzzer instead of rapid? what's the shrinking like?
// checkout https://pkg.go.dev/github.com/AdaLogics/go-fuzz-headers#section-readme

var (
	_ genContextProvider = (*genContext)(nil)
)

// TODO: distinguish between discrete and continuous generators?

type (
	// Gen is a generator for a type T.
	Gen[T any] struct {
		generator generatorFunc[T]
		choices   []T
		name      string
		static    bool // true if a static/fixed value is always returned (see GenJust)
	}

	// generatorFunc is our internal generator function type
	generatorFunc[T any] func(rng *rand.Rand) T

	Arbitrary[T any] interface {
		// Next returns a value of type T based on the generator context.
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
		// seedLookup is a map of type names to their seed values.
		// Whenever a generator is used, its seed is incremented.
		// This ensures that their output is deterministic, but novel for every call.
		seedLookup map[string]int
	}

	genContextProvider interface {
		genContext() *genContext
	}

	ListGen[T any] = []Arbitrary[T]

	// genDefault is an interface that allows a type to provide a default generator.
	genDefault[T any] interface {
		DefaultGen() Gen[T]
	}
)

func newGenImpl[T any](name string, fn generatorFunc[T]) Gen[T] {
	return Gen[T]{
		name:      name,
		generator: fn,
	}
}

func newGenContext(seed int) *genContext {
	return &genContext{
		baseSeed:   seed,
		seedLookup: make(map[string]int),
	}
}

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

// GenJust creates a generator that always returns the same value.
func GenJust[T any](val T) Gen[T] {
	return Gen[T]{
		generator: func(rng *rand.Rand) T {
			return val
		},
		static: true,
	}
}

// GenInt creates a generator for integer types within the specified range [min, max].
func GenInt[T ~int](min, max T) Gen[T] {
	if min > max {
		panic(fmt.Sprintf("GenInt: min (%v) cannot be greater than max (%v)", min, max))
	}
	return newGenImpl("Int", func(rng *rand.Rand) T {
		if min == max {
			return min
		}
		return T(rng.Intn(int(max-min+1)) + int(min))
	})
}

// GenBool creates a generator for boolean types.
func GenBool[T ~bool]() Gen[T] {
	return newGenImpl("Bool", func(rng *rand.Rand) T {
		return T(rng.Intn(2) == 1)
	})
}

// GenName creates a generator for string types using adjective-name combinations.
func GenName[T ~string]() Gen[T] {
	return newGenImpl("Name", func(rng *rand.Rand) T {
		if len(adjectives) == 0 || len(names) == 0 {
			return T("unknown-name") // fallback if arrays are empty
		}
		adj := adjectives[rng.Intn(len(adjectives))]
		name := names[rng.Intn(len(names))]
		return T(fmt.Sprintf("%s-%s", adj, name))
	})
}

// GenEnum creates a generator that randomly selects from the provided choices.
func GenEnum[T any](name string, choices ...T) Gen[T] {
	if name == "" {
		panic("GenEnum: name must not be empty")
	}
	if len(choices) == 0 {
		panic("GenEnum: choices must not be empty")
	}
	return Gen[T]{
		generator: func(rng *rand.Rand) T {
			return choices[rng.Intn(len(choices))]
		},
		choices: choices,
		name:    fmt.Sprintf("Choice(%s)", name),
	}
}

// GenList creates a list of generators.
func GenList[T any](items ...Arbitrary[T]) []Arbitrary[T] {
	return items
}

// Next generates a value using the zero value as default.
func (g Gen[T]) Next(gcp genContextProvider) T {
	var zero T
	return g.NextOrDefault(gcp, zero)
}

// Name returns the generator's name.
func (g Gen[T]) Name() string {
	return g.name
}

// AsJust converts this generator to a static generator with a fixed value.
func (g Gen[T]) AsJust(gcp genContextProvider) Gen[T] {
	return GenJust(g.Next(gcp))
}

// NextOrDefault generates a value or returns the default if generation fails.
func (g Gen[T]) NextOrDefault(gcp genContextProvider, defaultVal T) T {
	if gcp == nil {
		panic("GenContext provider cannot be nil")
	}

	ctx := gcp.genContext()
	if ctx == nil {
		panic("GenContext cannot be nil")
	}

	// Use generator context to pick a choice, if defined.
	if len(g.choices) > 0 && ctx.pickChoiceFn != nil {
		choiceIdx := ctx.pickChoiceFn(g.String(), len(g.choices))
		if choiceIdx < 0 || choiceIdx >= len(g.choices) {
			panic(fmt.Sprintf("pickChoiceFn returned invalid index %d for %d choices", choiceIdx, len(g.choices)))
		}
		return g.choices[choiceIdx]
	}

	// Use generator, if defined.
	if g.generator != nil {
		// Check if random generation is allowed for non-static generators
		if !ctx.allowRandom && !g.static {
			panic(fmt.Sprintf("generator %q is random, which is not allowed in this context", g.name))
		}

		// Use a more specific seed key that includes the generator name
		seedKey := fmt.Sprintf("%s_%T", g.name, defaultVal)
		if _, ok := ctx.seedLookup[seedKey]; !ok {
			ctx.seedLookup[seedKey] = ctx.baseSeed
		}
		ctx.seedLookup[seedKey]++

		// Create a deterministic random number generator with the incremented seed
		rng := rand.New(rand.NewSource(int64(ctx.seedLookup[seedKey])))
		return g.generator(rng)
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

// AllowRandom returns a new generator context that allows generating random values.
// NOTE: it returns a shallow copy of the context to avoid mutating the original.
func (g genContext) AllowRandom() *genContext {
	newCtx := g // shallow copy
	newCtx.allowRandom = true
	return &newCtx
}

// Seed returns the base seed of the context.
func (g *genContext) Seed() int {
	return g.baseSeed
}

// IsRandomAllowed returns whether random generation is allowed in this context.
func (g *genContext) IsRandomAllowed() bool {
	return g.allowRandom
}

// SetPickChoiceFn sets a custom function for picking choices in deterministic scenarios.
func (g *genContext) SetPickChoiceFn(fn func(string, int) int) {
	g.pickChoiceFn = fn
}
