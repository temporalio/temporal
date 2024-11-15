// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2024 Uber Technologies, Inc.
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

package deployment

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
)

// testMaxIDLengthLimit is the current default value used by dynamic config for
// MaxIDLengthLimit
const testMaxIDLengthLimit = 1000

func TestValidateDeploymentWfParams(t *testing.T) {
	testCases := []struct {
		Description   string
		FieldName     string
		Input         string
		ExpectedError error
	}{
		{
			Description:   "Empty Field",
			FieldName:     "DeploymentName",
			Input:         "",
			ExpectedError: serviceerror.NewInvalidArgument("DeploymentName cannot be empty"),
		},
		{
			Description:   "Large Field",
			FieldName:     "DeploymentName",
			Input:         strings.Repeat("s", 1000),
			ExpectedError: serviceerror.NewInvalidArgument("size of DeploymentName larger than the maximum allowed"),
		},
		{
			Description:   "Invalid character (|) in Field",
			FieldName:     "DeploymentName",
			Input:         "A" + DeploymentWorkflowIDDelimeter,
			ExpectedError: serviceerror.NewInvalidArgument(fmt.Sprintf("DeploymentName cannot contain reserved prefix %v", DeploymentWorkflowIDDelimeter)),
		},
		{
			Description:   "Valid field",
			FieldName:     "DeploymentName",
			Input:         "A",
			ExpectedError: nil,
		},
	}

	for _, test := range testCases {
		fieldName := test.FieldName
		field := test.Input
		err := ValidateDeploymentWfParams(fieldName, field, testMaxIDLengthLimit)

		if test.ExpectedError == nil {
			require.NoError(t, err)
			continue
		}

		var invalidArgument *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgument)
		require.Equal(t, test.ExpectedError.Error(), err.Error())
	}
}
