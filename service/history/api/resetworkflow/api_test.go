// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package resetworkflow

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/enums/v1"
)

type (
	resetWorkflowSuite struct {
		suite.Suite
	}
)

func TestResetWorkflowSuite(t *testing.T) {
	s := new(resetWorkflowSuite)
	suite.Run(t, s)
}

func (s *resetWorkflowSuite) TestGetResetReapplyExcludeTypes() {
	// Include all with no exclusions is no exclusions
	s.Equal(
		GetResetReapplyExcludeTypes(
			[]enums.ResetReapplyExcludeType{},
			enums.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
		),
		map[enums.ResetReapplyExcludeType]bool{},
	)
	// Include all with one exclusion is one exclusion
	s.Equal(
		GetResetReapplyExcludeTypes(
			[]enums.ResetReapplyExcludeType{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enums.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
		),
		map[enums.ResetReapplyExcludeType]bool{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: true},
	)
	// Include signal with no exclusions is no exclusions
	s.Equal(
		GetResetReapplyExcludeTypes(
			[]enums.ResetReapplyExcludeType{},
			enums.RESET_REAPPLY_TYPE_SIGNAL,
		),
		map[enums.ResetReapplyExcludeType]bool{},
	)
	// Include signal with exclude signal: exclude trumps deprecated include
	s.Equal(
		GetResetReapplyExcludeTypes(
			[]enums.ResetReapplyExcludeType{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enums.RESET_REAPPLY_TYPE_SIGNAL,
		),
		map[enums.ResetReapplyExcludeType]bool{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: true},
	)
	// Include none with no exclusions is all excluded
	s.Equal(
		GetResetReapplyExcludeTypes(
			[]enums.ResetReapplyExcludeType{},
			enums.RESET_REAPPLY_TYPE_NONE,
		),
		map[enums.ResetReapplyExcludeType]bool{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: true},
	)
	// Include none with exclude signal is all excluded
	s.Equal(
		GetResetReapplyExcludeTypes(
			[]enums.ResetReapplyExcludeType{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enums.RESET_REAPPLY_TYPE_NONE,
		),
		map[enums.ResetReapplyExcludeType]bool{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: true},
	)
}
