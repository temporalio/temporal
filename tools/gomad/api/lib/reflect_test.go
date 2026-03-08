package lib_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	SIMLIB "go.temporal.io/server/tools/gomad/api/lib"
	SIM "go.temporal.io/server/tools/gomad/runtime"
	"go.temporal.io/server/tools/gomad/runtime/testutil"
)

func TestValueOf(t *testing.T) {
	t.Run("MapKeys", func(t *testing.T) {
		fn := func() []string {
			m := SIMLANG.MapInit[string, int]("A", 0, "B", 1, "C", 2)
			SIM.Join()

			var keys []string
			for _, k := range SIMLIB.MapKeys(reflect.ValueOf(m)) {
				keys = append(keys, k.String())
			}

			return keys
		}

		for i := 0; i < testutil.TestRuns; i++ {
			var keys1, keys2 []string
			testutil.SingleRun(func(seed int64) { keys1 = fn() }, int64(i))
			testutil.SingleRun(func(seed int64) { keys2 = fn() }, int64(i))
			require.Equal(t, keys1, keys2)
		}
	})

	t.Run("SetMapIndex", func(t *testing.T) {
		fn := func() []string {
			mv := reflect.MakeMap(reflect.TypeOf(SIMLANG.MapInit[string, int]()))
			SIMLIB.SetMapIndex(mv, reflect.ValueOf("A"), reflect.ValueOf(0))
			SIMLIB.SetMapIndex(mv, reflect.ValueOf("B"), reflect.ValueOf(1))
			SIMLIB.SetMapIndex(mv, reflect.ValueOf("C"), reflect.ValueOf(2))

			m := mv.Interface().(map[string]int)
			require.Equal(t, map[string]int{
				"A": 0,
				"B": 1,
				"C": 2,
			}, m)

			return SIMLANG.MapKeys(m)
		}

		for i := 0; i < testutil.TestRuns; i++ {
			var keys1, keys2 []string
			testutil.SingleRun(func(seed int64) { keys1 = fn() }, int64(i))
			testutil.SingleRun(func(seed int64) { keys2 = fn() }, int64(i))
			require.Equal(t, keys1, keys2)
		}
	})
}
