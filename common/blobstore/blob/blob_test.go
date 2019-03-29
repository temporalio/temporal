// Copyright (c) 2017 Uber Technologies, Inc.
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

package blob

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type BlobSuite struct {
	*require.Assertions
	suite.Suite
}

func TestBlobSuite(t *testing.T) {
	suite.Run(t, new(BlobSuite))
}

func (s *BlobSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *BlobSuite) TestEquals() {
	testCases := []struct {
		blobA *Blob
		blobB *Blob
		equal bool
	}{
		{
			blobA: nil,
			blobB: nil,
			equal: true,
		},
		{
			blobA: NewBlob(nil, nil),
			blobB: nil,
			equal: false,
		},
		{
			blobA: NewBlob([]byte{1, 2, 3}, map[string]string{}),
			blobB: NewBlob([]byte{1, 2, 3}, map[string]string{"k1": "v1"}),
			equal: false,
		},
		{
			blobA: NewBlob(nil, map[string]string{"k1": "v1"}),
			blobB: NewBlob([]byte{1}, map[string]string{"k1": "v1"}),
			equal: false,
		},
		{
			blobA: NewBlob([]byte{1, 2, 3}, map[string]string{"k1": "v1"}),
			blobB: NewBlob([]byte{1, 2, 3, 4}, map[string]string{"k1": "v1"}),
			equal: false,
		},
		{
			blobA: NewBlob([]byte{1, 2, 3}, map[string]string{"k1": "v1"}),
			blobB: NewBlob([]byte{1, 2, 3}, map[string]string{"k1_x": "v1_x"}),
			equal: false,
		},
		{
			blobA: NewBlob([]byte{1, 2}, map[string]string{"k1": "v1", "k2": "v2"}),
			blobB: NewBlob([]byte{1, 2}, map[string]string{"k1": "v1"}),
			equal: false,
		},
		{
			blobA: NewBlob([]byte{1, 2}, map[string]string{"k1": "v1", "k2": "v2"}),
			blobB: NewBlob([]byte{1, 2}, map[string]string{"k1": "v1", "k2": "v2"}),
			equal: true,
		},
	}
	for _, tc := range testCases {
		s.Equal(tc.equal, tc.blobA.Equal(tc.blobB))
		s.Equal(tc.equal, tc.blobA.Equal(tc.blobB))
		aDeepCopy := tc.blobA.DeepCopy()
		bDeepCopy := tc.blobB.DeepCopy()
		s.Equal(tc.equal, aDeepCopy.Equal(bDeepCopy))
	}
}
