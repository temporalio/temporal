// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination size_getter_mock.go

package cache

// SizeGetter is an interface that can be implemented by cache entries to provide their size.
// Cache uses CacheSize() to determine the size of a cache entry.
// Please be aware that if the size of the cache entry changes while the cache is being used without pinning enabled,
// the cache won't be able to automatically adjust for this size change. In such instances, it's necessary to call Put()
// again to ensure the cache size remains accurate.
type (
	SizeGetter interface {
		CacheSize() int
	}
)

func getSize(value interface{}) int {
	if v, ok := value.(SizeGetter); ok {
		return v.CacheSize()
	}
	// if the object does not have a CacheSize() method, assume is count limit cache, which size should be 1
	return 1
}
