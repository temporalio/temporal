package stamp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var _ genDefault[typeWithDefaultGen] = typeWithDefaultGen(0)

type typeWithDefaultGen int

func (t typeWithDefaultGen) DefaultGen() Gen[typeWithDefaultGen] {
	return GenInt[typeWithDefaultGen](1, 10)
}

func TestGenNext(t *testing.T) {
	t.Run("pick random value deterministically", func(t *testing.T) {
		intGen := GenInt(1, 10)

		genCtx1 := newGenContext(0).AllowRandom()
		require.Equal(t, 5, intGen.Next(genCtx1))
		require.Equal(t, 1, intGen.Next(genCtx1))
		require.Equal(t, 2, intGen.Next(genCtx1))

		genCtx2 := newGenContext(0).AllowRandom()
		require.Equal(t, 5, intGen.Next(genCtx2))
		require.Equal(t, 1, intGen.Next(genCtx2))
		require.Equal(t, 2, intGen.Next(genCtx2))
	})

	t.Run("forbid random value by default", func(t *testing.T) {
		genCtx := newGenContext(0)

		// random generator is not allowed
		randGen := GenInt(1, 10)
		require.Panics(t, func() {
			randGen.Next(genCtx)
		})

		// non-random generator is allowed
		nonRandGen := GenJust("hello")
		nonRandGen.Next(genCtx)

		// explicitly allow random generator
		genCtx = genCtx.AllowRandom()
		randGen.Next(genCtx)
	})

	t.Run("convert to GenJust cache", func(t *testing.T) {
		genCtx := newGenContext(0).AllowRandom()
		intGen := GenInt(1, 10).AsJust(genCtx)

		genCtx = newGenContext(0)
		require.Equal(t, 5, intGen.Next(genCtx))
		require.Equal(t, 5, intGen.Next(genCtx))
		require.Equal(t, 5, intGen.Next(genCtx))
	})

	t.Run("empty generator", func(t *testing.T) {
		t.Run("returns zero value", func(t *testing.T) {
			var emptyGen Gen[int]
			genCtx := newGenContext(0)
			require.Equal(t, 0, emptyGen.Next(genCtx))
		})

		t.Run("returns custom default value", func(t *testing.T) {
			var emptyGen Gen[int]
			genCtx := newGenContext(0)
			require.Equal(t, 100, emptyGen.NextOrDefault(genCtx, 100))
		})

		t.Run("returns value from type's default generator", func(t *testing.T) {
			var emptyGen Gen[typeWithDefaultGen]
			genCtx := newGenContext(0).AllowRandom()
			require.Equal(t, typeWithDefaultGen(5), emptyGen.Next(genCtx))

			// still restrict randomness
			require.Panics(t, func() {
				emptyGen.Next(newGenContext(0))
			})
		})
	})

	t.Run("custom choice picker", func(t *testing.T) {
		choiceGen := GenEnum("letters", "a", "b", "c")

		genCtx := newGenContext(0)
		genCtx.pickChoiceFn = func(id string, count int) int {
			require.Equal(t, "Gen[string](1,Choice(letters))", id)
			require.Equal(t, count, 3)
			return 1 // always pick the second choice
		}

		require.Equal(t, "b", choiceGen.Next(genCtx))
		require.Equal(t, "b", choiceGen.Next(genCtx))
		require.Equal(t, "b", choiceGen.Next(genCtx))
	})
}

func TestGenJust(t *testing.T) {
	justGen := GenJust("hello")
	genCtx := newGenContext(0)
	require.Equal(t, "hello", justGen.Next(genCtx))
	require.Equal(t, "hello", justGen.Next(genCtx))
}
