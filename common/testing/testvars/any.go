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

package testvars

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
)

type Any struct {
	testName string
	testHash uint32
}

func newAny(testName string, testHash uint32) Any {
	return Any{
		testName: testName,
		testHash: testHash,
	}
}

func (a Any) String() string {
	return a.testName + "_any_random_string_" + randString(5)
}

func (a Any) Payload() *commonpb.Payload {
	return payload.EncodeString(a.String())
}

func (a Any) Payloads() *commonpb.Payloads {
	return payloads.EncodeString(a.String())
}

func (a Any) Int() int {
	// This produces number in XXX000YYY format, where XXX is unique for every test and YYY is a random number.
	return randInt(a.testHash, 3, 3, 3)
}

func (a Any) EventID() int64 {
	// This produces EventID in XX0YY format, where XX is unique for every test and YY is a random number.
	return int64(randInt(a.testHash, 2, 1, 2))
}
