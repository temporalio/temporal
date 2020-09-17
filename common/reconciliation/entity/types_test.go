// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package entity

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/persistence"
)

const (
	domainID   = "test-domain-id"
	workflowID = "test-workflow-id"
	runID      = "test-run-id"
	treeID     = "test-tree-id"
	branchID   = "test-branch-id"
)

func TestUtilSuite(t *testing.T) {
	suite.Run(t, new(TypeSuite))
}

type TypeSuite struct {
	*require.Assertions
	suite.Suite
}

func (t *TypeSuite) SetupTest() {
	t.Assertions = require.New(t.T())
}

func (t *TypeSuite) TestValidateExecution() {
	testCases := []struct {
		execution   *ConcreteExecution
		expectError bool
	}{
		{
			execution:   &ConcreteExecution{},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID: -1,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID: 0,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:  0,
					DomainID: domainID,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
				},
				BranchToken: []byte{1, 2, 3},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
				},
				BranchToken: []byte{1, 2, 3},
				TreeID:      treeID,
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					State:      persistence.WorkflowStateCreated - 1,
				},
				BranchToken: []byte{1, 2, 3},
				TreeID:      treeID,
				BranchID:    branchID,
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					State:      persistence.WorkflowStateCorrupted + 1,
				},
				BranchToken: []byte{1, 2, 3},
				TreeID:      treeID,
				BranchID:    branchID,
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					State:      persistence.WorkflowStateCreated,
				},
				BranchToken: []byte{1, 2, 3},
				TreeID:      treeID,
				BranchID:    branchID,
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		err := tc.execution.Validate()
		if tc.expectError {
			t.Error(err)
		} else {
			t.NoError(err)
		}
	}
}
