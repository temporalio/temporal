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
	CallerTypeOperator    = "operator"
	CallerTypeAPI         = "api"
	CallerTypeBackground  = "background"
	CallerTypePreemptable = "preemptable"

	CallerNameSystem = "system"
)

var (
	SystemBackgroundCallerInfo = CallerInfo{
		CallerName: CallerNameSystem,
		CallerType: CallerTypeBackground,
	}
	SystemPreemptableCallerInfo = CallerInfo{
		CallerName: CallerNameSystem,
		CallerType: CallerTypePreemptable,
	}
)

type (
	CallerInfo struct {
		// CallerName is the name of the caller.
		// It can either user namespace name or
		// the predefined CallerNameSystem.
		CallerName string

		// CallerType indicates if the call originates from
		// user API calls or from system background operations.
		CallerType string

		// CallOrigin is the first API method name in the call chain.
		// Currently, it is only specified when CallerType is CallerTypeAPI
		CallOrigin string
	}
)

// NewCallerInfo creates a new CallerInfo
func NewCallerInfo(
	callerName string,
	callerType string,
	callOrigin string,
) CallerInfo {
	return CallerInfo{
		CallerName: callerName,
		CallerType: callerType,
		CallOrigin: callOrigin,
	}
}

// NewBackgroundCallerInfo creates a new CallerInfo with Background callerType
// and empty callOrigin.
// This is equivalent to NewCallerInfo(callerName, CallerTypeBackground, "")
func NewBackgroundCallerInfo(
	callerName string,
) CallerInfo {
	return CallerInfo{
		CallerName: callerName,
		CallerType: CallerTypeBackground,
	}
}

// NewPreemptableCallerInfo creates a new CallerInfo with Preemptable callerType
// and empty callOrigin.
// This is equivalent to NewCallerInfo(callerName, CallerTypePreemptable, "")
func NewPreemptableCallerInfo(
	callerName string,
) CallerInfo {
	return CallerInfo{
		CallerName: callerName,
		CallerType: CallerTypePreemptable,
	}
}

// SetCallerInfo sets callerName, callerType and CallOrigin in the context.
// Existing values will be overwritten if new value is not empty.
// TODO: consider only set the caller info to golang context instead of grpc metadata
// and propagate to grpc outgoing context upon making an rpc call
func SetCallerInfo(
	ctx context.Context,
	info CallerInfo,
) context.Context {
	return setIncomingMD(ctx, map[string]string{
		callerNameHeaderName: info.CallerName,
		CallerTypeHeaderName: info.CallerType,
		callOriginHeaderName: info.CallOrigin,
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
	return setIncomingMD(ctx, map[string]string{CallerTypeHeaderName: callerType})
}

// SetOrigin set call origin in the context.
// Existing call origin will be overwritten if exists and new call origin is not empty.
func SetOrigin(
	ctx context.Context,
	callOrigin string,
) context.Context {
	return setIncomingMD(ctx, map[string]string{callOriginHeaderName: callOrigin})
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
	values := GetValues(ctx, callerNameHeaderName, CallerTypeHeaderName, callOriginHeaderName)
	return CallerInfo{
		CallerName: values[0],
		CallerType: values[1],
		CallOrigin: values[2],
	}
}
