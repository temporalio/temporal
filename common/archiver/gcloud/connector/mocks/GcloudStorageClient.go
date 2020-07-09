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

// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import (
	mock "github.com/stretchr/testify/mock"
	connector "go.temporal.io/server/common/archiver/gcloud/connector"
)

// GcloudStorageClient is an autogenerated mock type for the GcloudStorageClient type
type GcloudStorageClient struct {
	mock.Mock
}

// Bucket provides a mock function with given fields: URI
func (_m *GcloudStorageClient) Bucket(URI string) connector.BucketHandleWrapper {
	ret := _m.Called(URI)

	var r0 connector.BucketHandleWrapper
	if rf, ok := ret.Get(0).(func(string) connector.BucketHandleWrapper); ok {
		r0 = rf(URI)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(connector.BucketHandleWrapper)
		}
	}

	return r0
}
