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

package archiver

import (
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/common"
	"testing"
)

type HistoryBlobReaderSuite struct {
	*require.Assertions
	suite.Suite
}

func TestHistoryBlobReader(t *testing.T) {
	suite.Run(t, new(HistoryBlobReaderSuite))
}

func (s *HistoryBlobReaderSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *HistoryBlobReaderSuite) TestGetBlob_AlreadyInCache() {
	reader := &historyBlobReader{
		itr: nil,
		cache: map[int]*HistoryBlob{1: {
			Header: &HistoryBlobHeader{
				DomainName: common.StringPtr("test-domain-name"),
			},
		}},
	}
	hb, err := reader.GetBlob(1)
	s.NoError(err)
	s.Equal("test-domain-name", *hb.Header.DomainName)
}

func (s *HistoryBlobReaderSuite) TestGetBlob_SuccessfulAddToCache() {
	mockIterator := &HistoryBlobIteratorMock{}
	mockIterator.On("HasNext").Return(true)
	mockIterator.On("Next").Return(&HistoryBlob{
		Header: &HistoryBlobHeader{
			DomainName:       common.StringPtr("test-domain-name"),
			CurrentPageToken: common.IntPtr(5),
		},
	}, nil)
	reader := historyBlobReader{
		itr:   mockIterator,
		cache: make(map[int]*HistoryBlob),
	}
	hb, err := reader.GetBlob(5)
	s.NoError(err)
	s.Equal("test-domain-name", *hb.Header.DomainName)
}

func (s *HistoryBlobReaderSuite) TestGetBlob_OutOfBounds() {
	mockIterator := &HistoryBlobIteratorMock{}
	mockIterator.On("HasNext").Return(false)
	reader := historyBlobReader{
		itr:   mockIterator,
		cache: make(map[int]*HistoryBlob),
	}
	hb, err := reader.GetBlob(1)
	s.Equal(errPageTokenOutOfBounds, err)
	s.Nil(hb)
}
