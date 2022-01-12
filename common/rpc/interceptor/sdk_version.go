// The MIT License
//
// Copyright (c) 2022 Temporal Technologies Inc.  All rights reserved.
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

package interceptor

import (
	"context"
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/version/check"
	"google.golang.org/grpc"
)

type SDKInfoRecorder interface {
	RecordSDKInfo(name, version string)
	GetAndResetSDKInfo() []check.SDKInfo
}

type SDKVersionInterceptor struct {
	// Using a sync map to support effienct concurrent updates.
	// Key type is sdkNameVersion and value type is sdkCount.
	// Note that we never delete keys from this map as the cardinality of
	// client versions should be fairly low.
	sdkInfoCounter sync.Map
}

type sdkNameVersion struct {
	name    string
	version string
}

type sdkCount struct {
	count int64
}

var _ grpc.UnaryServerInterceptor = (*TelemetryInterceptor)(nil).Intercept

func NewSDKVersionInterceptor() *SDKVersionInterceptor {
	return &SDKVersionInterceptor{}
}

func (vi *SDKVersionInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	sdkName, sdkVersion := headers.GetClientNameAndVersion(ctx)
	if sdkName != "" && sdkVersion != "" {
		vi.RecordSDKInfo(sdkName, sdkVersion)
	}
	return handler(ctx, req)
}

func (vi *SDKVersionInterceptor) RecordSDKInfo(name, version string) {
	info := sdkNameVersion{name, version}
	valIface, ok := vi.sdkInfoCounter.Load(info)
	if !ok {
		// Store if wasn't added racy
		valIface, _ = vi.sdkInfoCounter.LoadOrStore(info, &sdkCount{})
	}
	atomic.AddInt64(&valIface.(*sdkCount).count, 1)
}

func (vi *SDKVersionInterceptor) GetAndResetSDKInfo() []check.SDKInfo {
	sdkInfo := make([]check.SDKInfo, 0)
	vi.sdkInfoCounter.Range(func(key, value interface{}) bool {
		timesSeen := atomic.SwapInt64(&value.(*sdkCount).count, 0)
		nameVersion := key.(sdkNameVersion)
		sdkInfo = append(sdkInfo, check.SDKInfo{Name: nameVersion.name, Version: nameVersion.version, TimesSeen: timesSeen})
		return true
	})
	return sdkInfo
}
