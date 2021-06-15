// The MIT License
//
// Copyright (c) 2021 Temporal Technologies Inc.  All rights reserved.
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

package timestamp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type ParseDurationSuite struct {
	suite.Suite
}

func TestParseDurationSuite(t *testing.T) {
	suite.Run(t, new(ParseDurationSuite))
}

func (s *ParseDurationSuite) TestParseDuration() {
	for _, c := range []struct {
		input    string
		expected time.Duration // -1 means error
	}{
		{"1h", time.Hour},
		{"3m30s", 3*time.Minute + 30*time.Second},
		{"1d", 24 * time.Hour},
		{"3d", 3 * 24 * time.Hour},
		{"5d6h15m", 5*24*time.Hour + 6*time.Hour + 15*time.Minute},
		{"5.25d15m", 5*24*time.Hour + 6*time.Hour + 15*time.Minute},
		{".5d", 12 * time.Hour},
		{"-10d12.25h", -(10*24*time.Hour + 12*time.Hour + 15*time.Minute)},
		{"3m2h1d", 3*time.Minute + 2*time.Hour + 1*24*time.Hour},
		{"8m7h6d5d4h3m", 8*time.Minute + 7*time.Hour + 6*24*time.Hour + 5*24*time.Hour + 4*time.Hour + 3*time.Minute},
		{"7", -1}, // error
		{"", -1},  // error
	} {
		got, err := ParseDuration(c.input)
		if c.expected == -1 {
			s.Error(err)
		} else {
			s.Equal(c.expected, got)
		}
	}
}

func (s *ParseDurationSuite) TestParseDurationDefaultDays() {
	for _, c := range []struct {
		input    string
		expected time.Duration // -1 means error
	}{
		{"3m30s", 3*time.Minute + 30*time.Second},
		{"7", 7 * 24 * time.Hour},
		{"7.5", 7*24*time.Hour + 12*time.Hour},
		{".75", 18 * time.Hour},
		{"2.75", 2*24*time.Hour + 18*time.Hour},
		{"", -1}, // error
	} {
		got, err := ParseDurationDefaultDays(c.input)
		if c.expected == -1 {
			s.Error(err)
		} else {
			s.Equal(c.expected, got)
		}
	}
}
