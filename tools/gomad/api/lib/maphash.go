// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Copyright 2014 The Go Authors. All rights reserved.
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
	"hash/maphash"
	"reflect"
	"unsafe"
)

type Hash struct {
	inner maphash.Hash
}

func (h *Hash) Seed() maphash.Seed {
	h.initSeed()
	return h.inner.Seed()
}

func (h *Hash) Reset() {
	h.initSeed()
	h.inner.Reset()
}

func (h *Hash) Sum64() uint64 {
	h.initSeed()
	return h.inner.Sum64()
}

func (h *Hash) Sum(b []byte) []byte {
	h.initSeed()
	return h.inner.Sum(b)
}

func (h *Hash) Write(b []byte) (int, error) {
	h.initSeed()
	return h.inner.Write(b)
}

func (h *Hash) WriteByte(b byte) error {
	h.initSeed()
	return h.inner.WriteByte(b)
}

func (h *Hash) WriteString(s string) (int, error) {
	h.initSeed()
	return h.inner.WriteString(s)
}

func (h *Hash) Size() int {
	h.initSeed()
	return h.inner.Size()
}

func (h *Hash) SetSeed(seed maphash.Seed) {
	h.inner.SetSeed(seed)
}

func (h *Hash) BlockSize() int {
	h.initSeed()
	return h.inner.BlockSize()
}

func (h *Hash) initSeed() {
	h.inner.SetSeed(MakeSeed())
}

func MakeSeed() maphash.Seed {
	s := maphash.Seed{}
	rs := reflect.ValueOf(&s).Elem()
	rf := rs.Field(0)
	rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()
	rf.Set(reflect.ValueOf(Uint64()))
	return s
}
