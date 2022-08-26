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

package dynamicconfig

/*

func (fc *basicClient) logDiff(old configValueMap, new configValueMap) {
	for key, newValues := range new {
		oldValues, ok := old[key]
		if !ok {
			for _, newValue := range newValues {
				// new key added
				fc.logValueDiff(key, nil, &newValue)
			}
		} else {
			// compare existing keys
			fc.logConstraintsDiff(key, oldValues, newValues)
		}
	}

	// check for removed values
	for key, oldValues := range old {
		if _, ok := new[key]; !ok {
			for _, oldValue := range oldValues {
				fc.logValueDiff(key, &oldValue, nil)
			}
		}
	}
}

func (bc *basicClient) logConstraintsDiff(key string, oldValues []ConstrainedValue, newValues []ConstrainedValue) {
	for _, oldValue := range oldValues {
		matchFound := false
		for _, newValue := range newValues {
			if reflect.DeepEqual(oldValue.Constraints, newValue.Constraints) {
				matchFound = true
				if !reflect.DeepEqual(oldValue.Value, newValue.Value) {
					bc.logValueDiff(key, &oldValue, &newValue)
				}
			}
		}
		if !matchFound {
			bc.logValueDiff(key, &oldValue, nil)
		}
	}

	for _, newValue := range newValues {
		matchFound := false
		for _, oldValue := range oldValues {
			if reflect.DeepEqual(oldValue.Constraints, newValue.Constraints) {
				matchFound = true
			}
		}
		if !matchFound {
			bc.logValueDiff(key, nil, &newValue)
		}
	}
}

func (bc *basicClient) logValueDiff(key string, oldValue *ConstrainedValue, newValue *ConstrainedValue) {
	logLine := &strings.Builder{}
	logLine.Grow(128)
	logLine.WriteString("dynamic config changed for the key: ")
	logLine.WriteString(key)
	logLine.WriteString(" oldValue: ")
	bc.appendConstrainedValue(logLine, oldValue)
	logLine.WriteString(" newValue: ")
	bc.appendConstrainedValue(logLine, newValue)
	bc.logger.Info(logLine.String())
}

func (bc *basicClient) appendConstrainedValue(logLine *strings.Builder, value *ConstrainedValue) {
	if value == nil {
		logLine.WriteString("nil")
	} else {
		logLine.WriteString("{ constraints: {")
		for constraintKey, constraintValue := range value.Constraints {
			logLine.WriteString("{")
			logLine.WriteString(constraintKey)
			logLine.WriteString(":")
			logLine.WriteString(fmt.Sprintf("%v", constraintValue))
			logLine.WriteString("}")
		}
		logLine.WriteString(fmt.Sprint("} value: ", value.Value, " }"))
	}
}
*/
