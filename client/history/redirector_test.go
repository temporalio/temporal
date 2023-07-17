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

package history

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
)

func TestTargetValidate(t *testing.T) {
	testcases := []struct {
		target operationTarget
		error  bool
	}{
		{
			target: operationTarget{
				shardID: 0,
				address: "",
			},
			error: true,
		},
		{
			target: operationTarget{
				shardID: -1,
				address: "foo",
			},
			error: true,
		},
		{
			target: operationTarget{
				shardID: 1,
				address: "foo",
			},
			error: true,
		},
		{
			target: operationTarget{
				shardID: 1,
				address: "",
			},
			error: false,
		},
		{
			target: operationTarget{
				shardID: 0,
				address: "foo",
			},
			error: false,
		},
	}

	for i, tc := range testcases {
		err := tc.target.validate()
		msg := fmt.Sprintf("test case %v", i)
		if tc.error {
			require.Error(t, err, msg)
			_, ok := err.(*serviceerror.InvalidArgument)
			require.True(t, ok, msg)
		} else {
			require.NoError(t, err, msg)
		}
	}
}
