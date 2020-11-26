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

package authorization

import (
	"testing"
)

func TestInvalidRoles(t *testing.T) {
	testInvalid(t, 32)
	testInvalid(t, 33)
	testInvalid(t, 64)
	testInvalid(t, 125)
}

func TestValidRoles(t *testing.T) {
	testValid(t, RoleUndefined)
	testValid(t, RoleWorker)
	testValid(t, RoleReader)
	testValid(t, RoleWriter)
	testValid(t, RoleAdmin)
	testValid(t, RoleWorker|RoleReader)
	testValid(t, RoleWorker|RoleWriter)
	testValid(t, RoleWorker|RoleAdmin)
	testValid(t, RoleWorker|RoleReader|RoleWriter)
	testValid(t, RoleWorker|RoleReader|RoleAdmin)
	testValid(t, RoleWorker|RoleReader|RoleWriter|RoleAdmin)
	testValid(t, RoleReader|RoleWriter)
	testValid(t, RoleReader|RoleAdmin)
	testValid(t, RoleReader|RoleWriter|RoleAdmin)
}

func testValid(t *testing.T, value Role) {
	if !value.IsValid() {
		t.Errorf("Valid role value %d reported as invalid.", value)
	}
}
func testInvalid(t *testing.T, value Role) {
	if value.IsValid() {
		t.Errorf("Invalid role value %d reported as valid.", value)
	}
}
