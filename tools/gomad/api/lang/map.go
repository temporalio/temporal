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

package lang

import (
	"fmt"
	"reflect"
	"sort"

	SIM "go.temporal.io/server/tools/gomad/runtime"
)

var mapKeySortOrderStateKey = "mapOrderState"

type mapKeySortOrderStateType = map[any]int64

func MapInit[K comparable, V any](values ...any) map[K]V {
	m := make(map[K]V, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key := values[i].(K)
		val := values[i+1].(V)
		m[key] = val
		MapKey(key)
	}
	return m
}

func MapInitPtr[K comparable, V any](values ...any) *map[K]V {
	m := MapInit[K, V](values...)
	return &m
}

func MapKey(key any) {
	if _, exists := getMapKeySortOrder(key); !exists {
		addMapKeySortOrder(key)
	}
}

func MapKeys[K comparable, V any](m map[K]V) []K {
	sim := SIM.TryAnySimulator()

	// init
	keys := make([]K, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	// sort keys
	sort.Slice(keys, func(left, right int) bool {
		order1, ok := getMapKeySortOrder(keys[left])
		if !ok {
			addMapKeySortOrder(keys[left])
			order1, _ = getMapKeySortOrder(keys[left])
		}
		order2, ok := getMapKeySortOrder(keys[right])
		if !ok {
			addMapKeySortOrder(keys[right])
			order2, _ = getMapKeySortOrder(keys[right])
		}
		if order1 == order2 {
			panic(fmt.Sprintf("keys have the same order: %v", order1))
		}
		return order1 < order2
	})

	// randomize keys (based on the simulator's seed)
	sim.Drng.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	return keys
}

func ReflectMapKeys(v reflect.Value) []reflect.Value {
	keys := v.MapKeys()

	sort.Slice(keys, func(left, right int) bool {
		keyLeft := keys[left].Interface()
		order1, ok := getMapKeySortOrder(keyLeft)
		if !ok {
			addMapKeySortOrder(keyLeft)
			order1, _ = getMapKeySortOrder(keyLeft)
		}
		keyRight := keys[right].Interface()
		order2, ok := getMapKeySortOrder(keyRight)
		if !ok {
			addMapKeySortOrder(keyRight)
			order2, _ = getMapKeySortOrder(keyRight)
		}
		if order1 == order2 {
			panic(fmt.Sprintf("keys have the same order: %v", order1))
		}
		return order1 < order2
	})
	return keys
}

func addMapKeySortOrder(key any) {
	sim := SIM.TryAnySimulator()
	sim.StateMu.Lock()
	defer sim.StateMu.Unlock()
	if _, exists := sim.State[mapKeySortOrderStateKey]; !exists {
		sim.State[mapKeySortOrderStateKey] = make(mapKeySortOrderStateType)
	}
	sim.State[mapKeySortOrderStateKey].(mapKeySortOrderStateType)[key] = sim.Drng.Int63()
}

func getMapKeySortOrder(key any) (v int64, ok bool) {
	sim := SIM.TryAnySimulator()
	sim.StateMu.Lock()
	defer sim.StateMu.Unlock()
	if _, exists := sim.State[mapKeySortOrderStateKey]; !exists {
		sim.State[mapKeySortOrderStateKey] = make(mapKeySortOrderStateType)
	}
	v, ok = sim.State[mapKeySortOrderStateKey].(mapKeySortOrderStateType)[key]
	return
}

func initMapKeySortOrder() {
	// intentionally left empty; initialization is lazy in addMapKeySortOrder/getMapKeySortOrder
}

func SetMapIndex(m, k, v reflect.Value) {
	m.SetMapIndex(k, v)
	MapKey(k.Interface())
}
