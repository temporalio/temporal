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
	fakegrpc "go.temporal.io/server/tools/gomad/api/ext-lib/fakegprc"
	SIMAPI "go.temporal.io/server/tools/gomad/api/lang"
	"google.golang.org/grpc"
)

func grpc_fixture() {
	SIMAPI.FuncStart()
	var i fakegrpc.UnaryClientInterceptor
	fakegrpc.NewServer(fakegrpc.ChainUnaryInterceptor(interceptor))
	fakegrpc.NewClient(addr)
}

func Dial(interceptors ...fakegrpc.UnaryClientInterceptor) {}

var _ = grpc.ChainUnaryInterceptor
var _ = grpc.NewClient
var _ = grpc.NewServer

type _ = grpc.UnaryClientInterceptor
