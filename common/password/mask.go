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

package password

import (
	"reflect"

	"gopkg.in/yaml.v3"
)

const passwordMask = "******"

var (
	DefaultFieldNames     = []string{"Password", "KeyData"}
	DefaultYAMLFieldNames = []string{"password", "keyData"}
)

func maskMap(m map[string]interface{}, fns map[string]struct{}) {
	for key, value := range m {
		if _, ok := fns[key]; ok {
			m[key] = passwordMask
		}

		if m, ok := value.(map[string]interface{}); ok {
			maskMap(m, fns)
		}
	}
}

// MaskYaml replace password values with mask and returns copy of the string.
func MaskYaml(yamlStr string, fieldNamesToMask []string) string {
	fns := make(map[string]struct{}, len(fieldNamesToMask))
	for _, fieldName := range fieldNamesToMask {
		fns[fieldName] = struct{}{}
	}

	var parsedYaml map[string]interface{}
	err := yaml.Unmarshal([]byte(yamlStr), &parsedYaml)
	if err != nil {
		return yamlStr
	}

	maskMap(parsedYaml, fns)

	strBytes, err := yaml.Marshal(parsedYaml)
	if err != nil {
		return yamlStr
	}
	return string(strBytes)
}

// MaskStruct replace password values with mask and returns copy of the cfg.
// Original cfg value is not modified.
func MaskStruct(strct interface{}, fieldNamesToMask []string) interface{} {
	strctV := reflect.ValueOf(strct)
	for t := reflect.TypeOf(strct); t.Kind() == reflect.Ptr; t = t.Elem() {
		strctV = strctV.Elem()
	}

	// strctV is not a pointer now. Create a copy using assignment.
	strctCopy := strctV.Interface()
	strctCopyPV := pointerTo(strctCopy)
	strctCopyV := strctCopyPV.Elem()

	for _, passwordFieldName := range fieldNamesToMask {
		passwordF := strctCopyV.FieldByName(passwordFieldName)
		if passwordF.CanSet() && passwordF.Kind() == reflect.String {
			passwordF.SetString(passwordMask)
		}
	}

	return strctCopyPV.Interface()
}

func pointerTo(val interface{}) reflect.Value {
	valPtr := reflect.New(reflect.TypeOf(val))
	valPtr.Elem().Set(reflect.ValueOf(val))
	return valPtr
}
