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

	CallerNameSystem = "system"
)

type CallerInfo struct {
	CallerName     string
	CallerType     string
	CallInitiation string
}

func NewCallerInfo(
	callerName string,
	callerType string,
	callInitiation string,
) CallerInfo {
	return CallerInfo{
		CallerName:     callerName,
		CallerType:     callerType,
		CallInitiation: callInitiation,
	}
}

// SetCallerInfo sets callerName, callerType and callInitiation in the context.
// Existing values will be overwritten if new value is not empty.
// TODO: consider only set the caller info to golang context instead of grpc metadata
// and propagate to grpc outgoing context upon making an rpc call
func SetCallerInfo(
	ctx context.Context,
	info CallerInfo,
) context.Context {
	return setIncomingMD(ctx, map[string]string{
		callerNameHeaderName:     info.CallerName,
		callerTypeHeaderName:     info.CallerType,
		callInitiationHeaderName: info.CallInitiation,
	})
}

// SetCallerName set caller name in the context.
// Existing caller name will be overwritten if exists and new caller name is not empty.
func SetCallerName(
	ctx context.Context,
	callerName string,
) context.Context {
	return setIncomingMD(ctx, map[string]string{callerNameHeaderName: callerName})
}

// SetCallerType set caller type in the context.
// Existing caller type will be overwritten if exists and new caller type is not empty.
func SetCallerType(
	ctx context.Context,
	callerType string,
) context.Context {
	return setIncomingMD(ctx, map[string]string{callerTypeHeaderName: callerType})
}

// SetCallInitiation set call initiation in the context.
// Existing call initiation will be overwritten if exists and new call initiation is not empty.
func SetCallInitiation(
	ctx context.Context,
	callInitiation string,
) context.Context {
	return setIncomingMD(ctx, map[string]string{callInitiationHeaderName: callInitiation})
}

func setIncomingMD(
	ctx context.Context,
	kv map[string]string,
) context.Context {
	mdIncoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		mdIncoming = metadata.MD{}
	}

	for k, v := range kv {
		if v != "" {
			mdIncoming.Set(k, v)
		}
	}

	return metadata.NewIncomingContext(ctx, mdIncoming)
}

// GetCallerInfo retrieves caller information from the context if exists. Empty value is returned
// if any piece of caller information is not specified in the context.
func GetCallerInfo(
	ctx context.Context,
) CallerInfo {
	values := GetValues(ctx, callerNameHeaderName, callerTypeHeaderName, callInitiationHeaderName)
	return CallerInfo{
		CallerName:     values[0],
		CallerType:     values[1],
		CallInitiation: values[2],
	}
}
