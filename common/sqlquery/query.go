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

package sqlquery

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	QueryTemplate = "select * from dummy where %s"

	DefaultDateTimeFormat = time.RFC3339
)

func ConvertToTime(timeStr string) (time.Time, error) {
	ts, err := strconv.ParseInt(timeStr, 10, 64)
	if err == nil {
		return timestamp.UnixOrZeroTime(ts), nil
	}
	timestampStr, err := ExtractStringValue(timeStr)
	if err != nil {
		return time.Time{}, err
	}
	parsedTime, err := time.Parse(DefaultDateTimeFormat, timestampStr)
	if err != nil {
		return time.Time{}, err
	}
	return parsedTime, nil
}

func ExtractStringValue(s string) (string, error) {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1], nil
	}
	return "", fmt.Errorf("value %s is not a string value", s)
}

// ParseValue returns a string, int64 or float64 if the parsing succeeds.
func ParseValue(sqlValue string) (interface{}, error) {
	if sqlValue == "" {
		return "", nil
	}

	if sqlValue[0] == '\'' && sqlValue[len(sqlValue)-1] == '\'' {
		strValue := strings.Trim(sqlValue, "'")
		return strValue, nil
	}

	// Unquoted value must be a number. Try int64 first.
	if intValue, err := strconv.ParseInt(sqlValue, 10, 64); err == nil {
		return intValue, nil
	}

	// Then float64.
	if floatValue, err := strconv.ParseFloat(sqlValue, 64); err == nil {
		return floatValue, nil
	}

	return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid expression: unable to parse %s", sqlValue))
}
