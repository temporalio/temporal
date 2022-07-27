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

package headers

import (
	"context"

	"google.golang.org/grpc/metadata"
)

const (
	CallerTypeAPI        = "api"
	CallerTypeBackground = "background"
)

type CallerInfo struct {
	CallerType string

	// TODO: add fields for CallerName and CallerInitiation
}

func NewCallerInfo(
	callerType string,
) CallerInfo {
	return CallerInfo{
		CallerType: callerType,
	}
}

// SetCallerInfo sets callerName and callerType value in incoming context
// if not already exists.
// TODO: consider only set the caller info to golang context instead of grpc metadata
// and propagate to grpc outgoing context upon making an rpc call
func SetCallerInfo(
	ctx context.Context,
	info CallerInfo,
) context.Context {
	mdIncoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		mdIncoming = metadata.MD{}
	}

	if len(mdIncoming.Get(callerTypeHeaderName)) == 0 {
		mdIncoming.Set(callerTypeHeaderName, string(info.CallerType))
	}

	return metadata.NewIncomingContext(ctx, mdIncoming)
}

func GetCallerInfo(
	ctx context.Context,
) CallerInfo {
	values := GetValues(ctx, callerTypeHeaderName)
	return CallerInfo{
		CallerType: values[0],
	}
}
