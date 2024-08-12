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

// Package assert implements in-code assertions.
//
// By default, assertions are disabled. Use build `-tags=with_assertions` to enable them.
// When disabled, all assertion calls will be stripped from the build by the Go compiler.
// Assertions should only be enabled during development and testing; not in production.
//
// Example:
// assert.That(object.state == "ready", "object is not ready: %v", object.state)
// assert.Fail("object %v should not be in state %v", object.state)
//
// Best practices:
// - use assertions to check for programming errors and internal invariants
// - assertions are *not* a substitute for proper error handling, validation or control flow
// - assertions *must* be side-effect free
// - do not use assertions with closures, defer, recover, select, etc. as they won't be inlined then
package assert
