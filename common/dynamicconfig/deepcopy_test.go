package dynamicconfig

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeepCopy_IntSlice(t *testing.T) {
	a := []int{1, 2, 3}
	b := deepCopyForMapstructure(a)
	a[1]++
	assert.NotEqual(t, a[1], b[1])
}

func TestDeepCopy_StringSlice(t *testing.T) {
	a := []string{"one", "two", "three"}
	b := deepCopyForMapstructure(a)
	a[1] = "four"
	assert.NotEqual(t, a[1], b[1])
}

func TestDeepCopy_SimpleStruct(t *testing.T) {
	a := struct {
		A, B int
		C    string
		D    [2]float32
	}{A: 4, B: 6, C: "eight", D: [2]float32{5.555, 6.666}}
	b := deepCopyForMapstructure(a)
	a.B++
	a.C = "ten"
	a.D[0] *= 1.1
	assert.NotEqual(t, a.B, b.B)
	assert.NotEqual(t, a.C, b.C)
	assert.NotEqual(t, a.D, b.D)
}

func TestDeepCopy_Pointer(t *testing.T) {
	v := 10
	a := &v
	b := deepCopyForMapstructure(a)
	(*a)++
	assert.NotEqual(t, *a, *b)
}

func TestDeepCopy_Pointers(t *testing.T) {
	type L struct {
		L *L
		V *int
	}
	v := 10
	a := L{
		L: &L{
			L: &L{
				L: &L{
					V: &v,
				},
			},
		},
	}
	b := deepCopyForMapstructure(a)
	(*a.L.L.L.V)++
	assert.NotEqual(t, *a.L.L.L.V, *b.L.L.L.V)
}

func TestDeepCopy_Map(t *testing.T) {
	a := map[int]int{3: 5}
	b := deepCopyForMapstructure(a)
	a[3] = 7
	a[8] = 9
	assert.Equal(t, b[3], 5)
	assert.Zero(t, b[8])
}

func TestDeepCopy_MapMap(t *testing.T) {
	a := map[int]map[string]int{
		3: map[string]int{"three": 3},
		5: map[string]int{"five": 5},
	}
	b := deepCopyForMapstructure(a)
	a[5]["five"] = 3
	assert.Equal(t, b[5]["five"], 5)
}

func TestDeepCopy_OtherReferenceTypes_Nil(t *testing.T) {
	a := struct {
		I context.Context
		F func(string) string
		C chan error
	}{}
	assert.NotPanics(t, func() {
		_ = deepCopyForMapstructure(a)
	})
}

func TestDeepCopy_OtherReferenceTypes_NonNil(t *testing.T) {
	assert.Panics(t, func() {
		a := struct{ C context.Context }{C: context.Background()}
		_ = deepCopyForMapstructure(a)
	})
	assert.Panics(t, func() {
		a := struct{ F func(string) string }{F: strings.ToLower}
		_ = deepCopyForMapstructure(a)
	})
	assert.Panics(t, func() {
		a := struct{ C chan error }{C: make(chan error)}
		_ = deepCopyForMapstructure(a)
	})
}
