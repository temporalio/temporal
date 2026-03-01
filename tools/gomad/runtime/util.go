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

package sim_runtime

import (
	"fmt"
	"path"
	"runtime"
	"strings"
)

func currentSourceLocation() string {
	return sourceLocation(1)
}

func sourceLocation(skipCallers int) string {
	for {
		// TODO: use `Callers` instead; likely more efficient
		pc, file, line, _ := runtime.Caller(skipCallers)
		fn := runtime.FuncForPC(pc).Name()
		switch {
		case strings.HasPrefix(fn, "go.temporal.io/server/tools/gomad") && !strings.HasSuffix(file, "_test.go"), // during unit testing
			strings.HasPrefix(fn, "gomad.local/go.temporal.io/server/tools/gomad"), // when transformed
			strings.HasPrefix(fn, "reflect."),
			strings.HasPrefix(fn, "runtime."):
			skipCallers += 1
			continue
		}
		pkg, funcName := split(fn)
		return fmt.Sprintf("%v/%v:%d|%v()", pkg, path.Base(file), line, funcName)
	}
}

// turns `Simulator/domain/pkg1/pkg2.(*type).func_name` into `domain/pkg1/pkg2`
func split(fn string) (string, string) {
	//verify.T(func() bool { return fn != "" }, "sourceLocation: empty function path")
	pathParts := strings.Split(fn, "/")
	tailParts := strings.Split(pathParts[len(pathParts)-1], ".")
	pkg := fmt.Sprintf("%v/%v", strings.Join(pathParts[:len(pathParts)-1], "/"), tailParts[0])
	pkg = strings.TrimPrefix(pkg, "Simulator/")
	return pkg, tailParts[len(tailParts)-1]
}
