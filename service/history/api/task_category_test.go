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

package api_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"

	enumspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/api"
)

func TestGetTaskCategory(t *testing.T) {
	t.Parallel()

	category, err := api.GetTaskCategory(enumspb.TASK_CATEGORY_TRANSFER)
	require.NoError(t, err)
	assert.Equal(t, int(enumspb.TASK_CATEGORY_TRANSFER), int(category.ID()))

	_, err = api.GetTaskCategory(enumspb.TASK_CATEGORY_UNSPECIFIED)
	require.Error(t, err)
	assert.ErrorContains(t, err, "Unspecified")
	assert.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
}
