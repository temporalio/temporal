// The MIT License
//
// Copyright (c) 2022 Temporal Technologies Inc.  All rights reserved.
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

package temporalapi

import (
	"reflect"
	"regexp"
)

var publicMethodRgx = regexp.MustCompile("^[A-Z]")

// WalkExportedMethods calls the provided callback on each method declared as public on the
// specified object.
// This prevents the `mustEmbedUnimplementedFooBarBaz` method required by the GRPC v2
// gateway from polluting our tests.
func WalkExportedMethods(obj any, cb func(reflect.Method)) {
	v := reflect.ValueOf(obj)
	if !v.IsValid() {
		return
	}

	t := v.Type()
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	for i := 0; i < t.NumMethod(); i++ {
		if publicMethodRgx.MatchString(t.Method(i).Name) {
			cb(t.Method(i))
		}
	}
}
