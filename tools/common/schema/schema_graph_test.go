// Copyright (c) 2020 Uber Technologies, Inc.
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

package schema

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type VersionGraphTestSuite struct {
	suite.Suite
}

func TestVersionGraphTestSuite(t *testing.T) {
	suite.Run(t, new(VersionGraphTestSuite))
}

func (s *VersionGraphTestSuite) TestFindShortestPath() {
	increments := []string{"v0.1", "v0.2", "v0.3", "v0.4", "v0.5", "v1.0", "v1.1", "v1.2", "v1.3"}
	tests := []struct {
		name      string
		to        string
		shortcuts []squashVersion
		want      []string
	}{
		{
			name: "increments only",
			want: []string{"v0.1", "v0.2", "v0.3", "v0.4", "v0.5", "v1.0", "v1.1", "v1.2", "v1.3"},
		},
		{
			name:      "single hop",
			shortcuts: []squashVersion{{prev: "0.0", ver: "1.3", dirName: "foo"}},
			want:      []string{"foo"},
		},
		{
			name: "middle hops",
			shortcuts: []squashVersion{
				{prev: "0.2", ver: "0.4", dirName: "foo"},
				{prev: "1.0", ver: "1.2", dirName: "bar"},
			},
			want: []string{"v0.1", "v0.2", "foo", "v0.5", "v1.0", "bar", "v1.3"},
		},
		{
			name: "hop at the start",
			shortcuts: []squashVersion{
				{prev: "0.0", ver: "0.4", dirName: "foo"},
			},
			want: []string{"foo", "v0.5", "v1.0", "v1.1", "v1.2", "v1.3"},
		},
		{
			name: "hop at the end",
			shortcuts: []squashVersion{
				{prev: "1.0", ver: "1.3", dirName: "foo"},
			},
			want: []string{"v0.1", "v0.2", "v0.3", "v0.4", "v0.5", "v1.0", "foo"},
		},
		{
			name: "out of range",
			to:   "v1.4",
			shortcuts: []squashVersion{
				{prev: "1.0", ver: "1.3", dirName: "foo"},
			},
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			to := increments[len(increments)-1]
			if tt.to != "" {
				to = tt.to
			}
			p, err := findShortestPath("0.0", dirToVersion(to), increments, tt.shortcuts)
			s.Require().NoError(err)
			s.Equal(tt.want, p)
		})
	}
}
