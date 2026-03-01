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

//go:build fixture

package fixtures

import (
	"net"

	SIMAPI "gomad.local/go.temporal.io/server/tools/gomad/api/lang"
	SIMLIB "gomad.local/go.temporal.io/server/tools/gomad/api/lib"
)

func net_fixture() {
	SIMAPI.FuncStart()
	SIMLIB.Listen("tcp", "127.0.0.1:80")
	SIMLIB.Dial("tcp", "127.0.0.1:80")

	var d net.Dialer
	SIMLIB.
		Dialer_Dial(&d, "network", "address")
	SIMLIB.
		Dialer_DialContext(&d, ctx, "network", "address")

	dp := &d
	SIMLIB.
		Dialer_Dial(dp, "network", "address")
	SIMLIB.
		Dialer_DialContext(dp, ctx, "network", "address")
	SIMLIB.
		Dialer_Dial(dialer(), "network", "address")
	SIMLIB.
		Dialer_DialContext(dialer(), ctx, "network", "address")
}

func dialer() *net.Dialer {}

var _ = net.Dial
var _ = net.Listen
