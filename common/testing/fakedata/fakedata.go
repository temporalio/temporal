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
