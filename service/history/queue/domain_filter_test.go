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

package queue

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	domainFilterSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestDomainFilterSuite(t *testing.T) {
	s := new(domainFilterSuite)
	suite.Run(t, s)
}

func (s *domainFilterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *domainFilterSuite) TestDomainFilter_Filter() {
	testCases := []struct {
		domainIDs      map[string]struct{}
		reverseMatch   bool
		testDomainIDs  []string
		expectedResult []bool
	}{
		{
			domainIDs:      nil,
			reverseMatch:   false,
			testDomainIDs:  []string{"some random domain"},
			expectedResult: []bool{false},
		},
		{
			domainIDs:      covertToDomainIDSet([]string{"testDomain1", "testDomain2"}),
			reverseMatch:   false,
			testDomainIDs:  []string{"testDomain1", "some random domain"},
			expectedResult: []bool{true, false},
		},
		{
			domainIDs:      covertToDomainIDSet([]string{}),
			reverseMatch:   true,
			testDomainIDs:  []string{"any domainID"},
			expectedResult: []bool{true},
		},
		{
			domainIDs:      covertToDomainIDSet([]string{"testDomain1", "testDomain2"}),
			reverseMatch:   true,
			testDomainIDs:  []string{"testDomain1", "some random domain"},
			expectedResult: []bool{false, true},
		},
	}

	for _, tc := range testCases {
		filter := NewDomainFilter(tc.domainIDs, tc.reverseMatch)
		for i, testDomain := range tc.testDomainIDs {
			result := filter.Filter(testDomain)
			s.Equal(tc.expectedResult[i], result)
		}
	}
}

func (s *domainFilterSuite) TestDomainFilter_Include() {
	testCases := []struct {
		domainIDs         map[string]struct{}
		reverseMatch      bool
		newDomainIDs      map[string]struct{}
		expectedDomainIDs map[string]struct{}
	}{
		{
			domainIDs:         covertToDomainIDSet([]string{"testDomain1", "testDomain2"}),
			reverseMatch:      false,
			newDomainIDs:      covertToDomainIDSet([]string{"testDomain2", "testDomain3"}),
			expectedDomainIDs: covertToDomainIDSet([]string{"testDomain1", "testDomain2", "testDomain3"}),
		},
		{
			domainIDs:         covertToDomainIDSet([]string{"testDomain1", "testDomain2"}),
			reverseMatch:      true,
			newDomainIDs:      covertToDomainIDSet([]string{"testDomain2", "testDomain3"}),
			expectedDomainIDs: covertToDomainIDSet([]string{"testDomain1"}),
		},
	}

	for _, tc := range testCases {
		baseFilter := NewDomainFilter(tc.domainIDs, tc.reverseMatch)
		newFilter := baseFilter.Include(tc.newDomainIDs)

		// check if the base filter got modified
		s.Equal(tc.domainIDs, baseFilter.DomainIDs)
		s.Equal(tc.reverseMatch, baseFilter.ReverseMatch)

		s.Equal(tc.expectedDomainIDs, newFilter.DomainIDs)
		s.Equal(tc.reverseMatch, newFilter.ReverseMatch)
	}
}

func (s *domainFilterSuite) TestDomainFilter_Exclude() {
	testCases := []struct {
		domainIDs         map[string]struct{}
		reverseMatch      bool
		newDomainIDs      map[string]struct{}
		expectedDomainIDs map[string]struct{}
	}{
		{
			domainIDs:         covertToDomainIDSet([]string{"testDomain1", "testDomain2"}),
			reverseMatch:      false,
			newDomainIDs:      covertToDomainIDSet([]string{"testDomain2", "testDomain3"}),
			expectedDomainIDs: covertToDomainIDSet([]string{"testDomain1"}),
		},
		{
			domainIDs:         covertToDomainIDSet([]string{"testDomain1", "testDomain2"}),
			reverseMatch:      true,
			newDomainIDs:      covertToDomainIDSet([]string{"testDomain2", "testDomain3"}),
			expectedDomainIDs: covertToDomainIDSet([]string{"testDomain1", "testDomain2", "testDomain3"}),
		},
	}

	for _, tc := range testCases {
		baseFilter := NewDomainFilter(tc.domainIDs, tc.reverseMatch)
		newFilter := baseFilter.Exclude(tc.newDomainIDs)

		// check if the base filter got modified
		s.Equal(tc.domainIDs, baseFilter.DomainIDs)
		s.Equal(tc.reverseMatch, baseFilter.ReverseMatch)

		s.Equal(tc.expectedDomainIDs, newFilter.DomainIDs)
		s.Equal(tc.reverseMatch, newFilter.ReverseMatch)
	}
}

func (s *domainFilterSuite) TestDomainFilter_Merge() {
	testCases := []struct {
		domainIDs            []map[string]struct{}
		reverseMatch         []bool
		expectedDomainIDs    map[string]struct{}
		expectedReverseMatch bool
	}{
		{
			domainIDs: []map[string]struct{}{
				covertToDomainIDSet([]string{"testDomain1", "testDomain2"}),
				covertToDomainIDSet([]string{"testDomain2", "testDomain3"}),
			},
			reverseMatch:         []bool{false, false},
			expectedDomainIDs:    covertToDomainIDSet([]string{"testDomain1", "testDomain2", "testDomain3"}),
			expectedReverseMatch: false,
		},
		{
			domainIDs: []map[string]struct{}{
				covertToDomainIDSet([]string{"testDomain1", "testDomain2"}),
				covertToDomainIDSet([]string{"testDomain2", "testDomain3"}),
			},
			reverseMatch:         []bool{true, true},
			expectedDomainIDs:    covertToDomainIDSet([]string{"testDomain2"}),
			expectedReverseMatch: true,
		},
		{
			domainIDs: []map[string]struct{}{
				covertToDomainIDSet([]string{"testDomain1", "testDomain2"}),
				covertToDomainIDSet([]string{"testDomain2", "testDomain3"}),
			},
			reverseMatch:         []bool{true, false},
			expectedDomainIDs:    covertToDomainIDSet([]string{"testDomain1"}),
			expectedReverseMatch: true,
		},
		{
			domainIDs: []map[string]struct{}{
				covertToDomainIDSet([]string{"testDomain1", "testDomain2"}),
				covertToDomainIDSet([]string{"testDomain2", "testDomain3"}),
			},
			reverseMatch:         []bool{false, true},
			expectedDomainIDs:    covertToDomainIDSet([]string{"testDomain3"}),
			expectedReverseMatch: true,
		},
	}

	for _, tc := range testCases {
		var filters []DomainFilter
		for i, domainIDs := range tc.domainIDs {
			filters = append(filters, NewDomainFilter(domainIDs, tc.reverseMatch[i]))
		}

		s.NotEmpty(filters)
		mergedFilter := filters[0]
		for _, f := range filters[1:] {
			mergedFilter = mergedFilter.Merge(f)
		}

		s.Equal(tc.expectedDomainIDs, mergedFilter.DomainIDs)
		s.Equal(tc.expectedReverseMatch, mergedFilter.ReverseMatch)
	}
}
