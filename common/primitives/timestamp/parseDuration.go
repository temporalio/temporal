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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	reUnitless = regexp.MustCompile(`^(\d+(\.\d*)?|(\.\d+))$`)
	reDays     = regexp.MustCompile(`(\d+(\.\d*)?|(\.\d+))d`)
)

// ParseDuration is like time.ParseDuration, but supports unit "d" for days
// (always interpreted as exactly 24 hours).
func ParseDuration(s string) (time.Duration, error) {
	s = reDays.ReplaceAllStringFunc(s, func(v string) string {
		fv, err := strconv.ParseFloat(strings.TrimSuffix(v, "d"), 64)
		if err != nil {
			return v // will cause time.ParseDuration to return an error
		}
		return fmt.Sprintf("%fh", 24*fv)
	})
	return time.ParseDuration(s)
}

// ParseDurationDefaultDays is like time.ParseDuration, but supports unit "d"
// for days (always interpreted as exactly 24 hours), and also supports
// unit-less numbers, which are interpreted as days.
func ParseDurationDefaultDays(s string) (time.Duration, error) {
	if reUnitless.MatchString(s) {
		s += "d"
	}
	return ParseDuration(s)
}
