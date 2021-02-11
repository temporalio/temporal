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

package archiver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/payload"
)

type UtilSuite struct {
	*require.Assertions
	suite.Suite
}

func TestUtilSuite(t *testing.T) {
	suite.Run(t, new(UtilSuite))
}

func (s *UtilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *UtilSuite) TestHashDeterminism() {
	testCases := []struct {
		instance interface{}
	}{
		{
			instance: "some random string",
		},
		{
			instance: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
		},
		{
			instance: []string{"value1", "value2", "value3"},
		},
		{
			instance: ArchiveRequest{
				NamespaceID: "some random namespaceID",
				ShardID:     0,
				BranchToken: []byte{1, 2, 3},
				NextEventID: int64(123),
				Status:      enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
				Memo: &commonpb.Memo{
					Fields: map[string]*commonpb.Payload{
						"memoKey1": payload.EncodeBytes([]byte{1, 2, 3}),
						"memoKey2": payload.EncodeBytes([]byte{4, 5, 6}),
					},
				},
				SearchAttributes: &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						"customKey1": payload.EncodeBytes([]byte{1, 2, 3}),
						"customKey2": payload.EncodeBytes([]byte{4, 5, 6}),
					},
				},
				Targets: []ArchivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
			},
		},
	}

	for _, tc := range testCases {
		expectedHash := hash(tc.instance)
		for i := 0; i != 100; i++ {
			s.Equal(expectedHash, hash(tc.instance))
		}
	}
}

func (s *UtilSuite) TestHashesEqual() {
	testCases := []struct {
		a     []uint64
		b     []uint64
		equal bool
	}{
		{
			a:     nil,
			b:     nil,
			equal: true,
		},
		{
			a:     []uint64{1, 2, 3},
			b:     []uint64{1, 2, 3},
			equal: true,
		},
		{
			a:     []uint64{1, 2},
			b:     []uint64{1, 2, 3},
			equal: false,
		},
		{
			a:     []uint64{1, 2, 3},
			b:     []uint64{1, 2},
			equal: false,
		},
		{
			a:     []uint64{1, 2, 5, 5, 5},
			b:     []uint64{1, 2, 5, 5, 5},
			equal: true,
		},
		{
			a:     []uint64{1, 2, 5, 5},
			b:     []uint64{1, 2, 5, 5, 5},
			equal: false,
		},
		{
			a:     []uint64{1, 2, 5, 5, 5, 5},
			b:     []uint64{1, 2, 5, 5, 5},
			equal: false,
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.equal, hashesEqual(tc.a, tc.b))
	}
}
