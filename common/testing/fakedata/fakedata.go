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

// Package fakedata provides utilities for generating random data for testing. We recently migrated from gofakeit to
// go-faker. This was to avoid the problem described here: https://github.com/brianvoe/gofakeit/issues/281#issue-2071796056.
// To make such migrations easier in the future, and to ensure that we use [faker.SetIgnoreInterface] before any
// invocation, we created this package.
package fakedata

import (
	"github.com/go-faker/faker/v4"
)

func init() {
	// We need this option to prevent faker.FakeData from returning an error for any struct that has an interface{} field.
	faker.SetIgnoreInterface(true)
	// We need this option to prevent faker from taking a long time while generating random data for structs that have
	// map or slice fields. This is especially relevant for persistence.ShardInfo, which takes about 1s without this
	// option, but only ~100µs with it.
	if err := faker.SetRandomMapAndSliceMaxSize(2); err != nil {
		panic(err)
	}
}

// FakeStruct generates random data for the given struct pointer. Example usage:
//
//	var shardInfo persistencespb.ShardInfo
//	_ = fakedata.FakeStruct(&shardInfo)
func FakeStruct(a interface{}) error {
	return faker.FakeData(a)
}
