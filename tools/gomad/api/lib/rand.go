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

package lib

import (
	"math/rand"

	SIM "go.temporal.io/server/tools/gomad/runtime"
)

func ExpFloat64() float64                { return SIM.CurrentSimulator().Drng.ExpFloat64() }
func Float32() float32                   { return SIM.CurrentSimulator().Drng.Float32() }
func Float64() float64                   { return SIM.CurrentSimulator().Drng.Float64() }
func Int() int                           { return SIM.CurrentSimulator().Drng.Int() }
func Int31() int32                       { return SIM.CurrentSimulator().Drng.Int31() }
func Int31n(n int32) int32               { return SIM.CurrentSimulator().Drng.Int31n(n) }
func Int63() int64                       { return SIM.CurrentSimulator().Drng.Int63() }
func Int63n(n int64) int64               { return SIM.CurrentSimulator().Drng.Int63n(n) }
func Intn(n int) int                     { return SIM.CurrentSimulator().Drng.Intn(n) }
func NormFloat64() float64               { return SIM.CurrentSimulator().Drng.NormFloat64() }
func Perm(n int) []int                   { return SIM.CurrentSimulator().Drng.Perm(n) }
func Read(p []byte) (n int, err error)   { return SIM.CurrentSimulator().Drng.Read(p) }
func Seed(seed int64)                    { /* ignore */ }
func Shuffle(n int, swap func(i, j int)) { SIM.CurrentSimulator().Drng.Shuffle(n, swap) }
func Uint32() uint32                     { return SIM.CurrentSimulator().Drng.Uint32() }
func Uint64() uint64                     { return SIM.CurrentSimulator().Drng.Uint64() }

type source struct{}

func NewSource(int64) rand.Source { return &source{} }

func (s *source) Int63() int64 {
	return Int63()
}

func (s *source) Seed(seed int64) {
	// TODO ?
}
