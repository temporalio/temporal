// Copyright (c) 2020 Temporal Technologies, Inc.
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

package history

import (
	"context"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/client"
)

var (
	// call header to cadence server
	headers = metadata.New(map[string]string{
		common.LibraryVersionHeaderName: "1.0.0",
		common.FeatureVersionHeaderName: client.GoWorkerConsistentQueryVersion,
		common.ClientImplHeaderName:     client.GoSDK,
		// TODO: remove these headers when server is vanilla gRPC (not YARPC)
		"rpc-caller":   "temporal-history",
		"rpc-service":  "cadence-frontend",
		"rpc-encoding": "proto",
	})
)

func createContextWithCancel(timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx := metadata.NewOutgoingContext(context.Background(), headers)
	ctx, cancel := context.WithTimeout(ctx, timeout)

	return ctx, cancel
}
