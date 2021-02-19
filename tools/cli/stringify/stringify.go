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

package stringify

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/fatih/color"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
)

const (
	maxWordLength = 120 // if text length is larger than maxWordLength, it will be inserted spaces
)

func AnyToString(val interface{}, printFully bool, maxFieldLength int) string {
	v := reflect.ValueOf(val)
	if val == nil || (v.Kind() == reflect.Ptr && v.IsNil()) {
		return ""
	}

	// Special types
	switch tVal := val.(type) {
	case string:
		return tVal
	case time.Time:
		if tVal.IsZero() {
			return "<zero>"
		}
		return tVal.String()
	case *commonpb.Payload:
		return payload.ToString(tVal)
	case *commonpb.Payloads:
		return payloads.ToString(tVal)
	case int:
		return strconv.FormatInt(int64(tVal), 10)
	case int64:
		return strconv.FormatInt(tVal, 10)
	case int32:
		return strconv.FormatInt(int64(tVal), 10)
	case float64:
		return strconv.FormatFloat(tVal, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(tVal), 'f', -1, 64)
	case bool:
		return strconv.FormatBool(tVal)
	case byte:
		return strconv.FormatInt(int64(tVal), 10)
	case []byte:
		if len(tVal) == 0 {
			return ""
		}
		return fmt.Sprintf("[%v]", bytesToString(tVal))
	}

	switch v.Kind() {
	case reflect.Invalid:
		return ""
	case reflect.Slice:
		// All but []byte which is already handled.
		if v.Len() == 0 {
			return ""
		}
		return fmt.Sprintf("[len=%d]", v.Len())
	case reflect.Ptr:
		return AnyToString(v.Elem().Interface(), printFully, maxFieldLength)
	case reflect.Map:
		var b strings.Builder
		b.WriteString("map{")
		for _, mapKey := range v.MapKeys() {
			mapVal := v.MapIndex(mapKey)
			if !mapVal.CanInterface() {
				continue
			}
			mapStr := AnyToString(mapVal.Interface(), printFully, maxFieldLength)
			if mapStr == "" {
				continue
			}

			if b.Len() > 4 {
				b.WriteString(", ")
			}
			b.WriteString(mapKey.String())
			b.WriteRune(':')
			b.WriteString(mapStr)
		}
		if b.Len() == 4 { // "map{" only
			return ""
		}
		b.WriteRune('}')
		return b.String()
	case reflect.Struct:
		var b strings.Builder
		t := reflect.TypeOf(val)
		b.WriteRune('{')
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if f.Kind() == reflect.Invalid {
				continue
			}
			// Filter out private fields.
			if !f.CanInterface() {
				continue
			}

			fieldName := t.Field(i).Name
			fieldStr := AnyToString(f.Interface(), printFully, maxFieldLength)
			if fieldStr == "" {
				continue
			}
			if !isAttributeName(fieldName) && !strings.HasSuffix(fieldName, "Failure") {
				if !printFully {
					fieldStr = trimTextAndBreakWords(fieldStr, maxFieldLength)
				} else if maxFieldLength != 0 { // for command run workflow and observe history
					fieldStr = trimText(fieldStr, maxFieldLength)
				}
			}

			if b.Len() > 1 {
				b.WriteString(", ")
			}
			if strings.HasSuffix(fieldName, "Reason") ||
				strings.HasSuffix(fieldName, "Cause") ||
				strings.HasSuffix(fieldName, "Details") {
				b.WriteString(color.MagentaString(fieldName))
			} else if strings.HasSuffix(fieldName, "Input") ||
				strings.HasSuffix(fieldName, "Result") {
				b.WriteString(color.CyanString(fieldName))
			} else if strings.HasSuffix(fieldName, "Failure") ||
				strings.HasSuffix(fieldName, "Error") {
				b.WriteString(color.RedString(fieldName))
			} else {
				b.WriteString(fieldName)
			}
			b.WriteRune(':')
			b.WriteString(fieldStr)
		}
		if b.Len() == 1 { // '{' only
			return ""
		}
		b.WriteRune('}')
		return b.String()
	default:
		return fmt.Sprint(val)
	}
}

func bytesToString(val []byte) string {
	s := string(val)
	isPrintable := true
	for _, r := range s {
		if !unicode.IsPrint(r) {
			isPrintable = false
			break
		}
	}

	if isPrintable {
		return strings.TrimSpace(s)
	}

	return base64.StdEncoding.EncodeToString(val)
}

// limit the maximum length for each field
func trimText(input string, maxFieldLength int) string {
	if len(input) > maxFieldLength {
		input = fmt.Sprintf("%s ... %s", input[:maxFieldLength/2], input[(len(input)-maxFieldLength/2):])
	}
	return input
}

// limit the maximum length for each field, and break long words for table item correctly wrap words
func trimTextAndBreakWords(input string, maxFieldLength int) string {
	input = trimText(input, maxFieldLength)
	return breakLongWords(input, maxWordLength)
}

// long words will make output in table cell looks bad,
// break long text "ltltltltllt..." to "ltlt ltlt lt..." will make use of table autowrap so that output is pretty.
func breakLongWords(input string, maxWordLength int) string {
	if len(input) <= maxWordLength {
		return input
	}

	cnt := 0
	for i := 0; i < len(input); i++ {
		if cnt == maxWordLength {
			cnt = 0
			input = input[:i] + " " + input[i:]
			continue
		}
		cnt++
		if input[i] == ' ' {
			cnt = 0
		}
	}
	return input
}

func isAttributeName(name string) bool {
	eventType := strings.TrimSuffix(name, "EventAttributes")
	_, ok := enumspb.EventType_value[eventType]
	return ok
}
