// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

package dynamicconfig

import (
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
