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

package iterator

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/persistence"
)

const (
	treeID   = "test-tree-id"
	branchID = "test-branch-id"
)

var (
	validBranchToken   = []byte{89, 11, 0, 10, 0, 0, 0, 12, 116, 101, 115, 116, 45, 116, 114, 101, 101, 45, 105, 100, 11, 0, 20, 0, 0, 0, 14, 116, 101, 115, 116, 45, 98, 114, 97, 110, 99, 104, 45, 105, 100, 0}
	invalidBranchToken = []byte("invalid")
)

func TestPersistenceSuite(t *testing.T) {
	suite.Run(t, new(PersistenceSuite))
}

func (p *PersistenceSuite) SetupTest() {
	p.Assertions = require.New(p.T())
}

type PersistenceSuite struct {
	*require.Assertions
	suite.Suite
}

func (p *PersistenceSuite) TestGetBranchToken() {
	encoder := codec.NewThriftRWEncoder()
	testCases := []struct {
		entity      *persistence.ListConcreteExecutionsEntity
		expectError bool
		branchToken []byte
		treeID      string
		branchID    string
	}{
		{
			entity: &persistence.ListConcreteExecutionsEntity{
				ExecutionInfo: &persistence.WorkflowExecutionInfo{
					BranchToken: p.getValidBranchToken(encoder),
				},
			},
			expectError: false,
			branchToken: validBranchToken,
			treeID:      treeID,
			branchID:    branchID,
		},
		{
			entity: &persistence.ListConcreteExecutionsEntity{
				ExecutionInfo: &persistence.WorkflowExecutionInfo{
					BranchToken: invalidBranchToken,
				},
				VersionHistories: &persistence.VersionHistories{
					CurrentVersionHistoryIndex: 1,
					Histories: []*persistence.VersionHistory{
						{
							BranchToken: invalidBranchToken,
						},
						{
							BranchToken: validBranchToken,
						},
					},
				},
			},
			expectError: false,
			branchToken: validBranchToken,
			treeID:      treeID,
			branchID:    branchID,
		},
		{
			entity: &persistence.ListConcreteExecutionsEntity{
				ExecutionInfo: &persistence.WorkflowExecutionInfo{
					BranchToken: invalidBranchToken,
				},
				VersionHistories: &persistence.VersionHistories{
					CurrentVersionHistoryIndex: 1,
					Histories: []*persistence.VersionHistory{
						{
							BranchToken: validBranchToken,
						},
						{
							BranchToken: invalidBranchToken,
						},
					},
				},
			},
			expectError: true,
		},
		{
			entity: &persistence.ListConcreteExecutionsEntity{
				ExecutionInfo: &persistence.WorkflowExecutionInfo{
					BranchToken: invalidBranchToken,
				},
				VersionHistories: &persistence.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories:                  []*persistence.VersionHistory{},
				},
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		branchToken, treeID, branchID, err := GetBranchToken(tc.entity, encoder)
		if tc.expectError {
			p.Error(err)
			p.Nil(branchToken)
			p.Empty(treeID)
			p.Empty(branchID)
		} else {
			p.NoError(err)
			p.Equal(tc.branchToken, branchToken)
			p.Equal(tc.treeID, treeID)
			p.Equal(tc.branchID, branchID)
		}
	}
}

func (p *PersistenceSuite) getValidBranchToken(encoder *codec.ThriftRWEncoder) []byte {
	hb := &shared.HistoryBranch{
		TreeID:   common.StringPtr(treeID),
		BranchID: common.StringPtr(branchID),
	}
	bytes, err := encoder.Encode(hb)
	p.NoError(err)
	return bytes
}
