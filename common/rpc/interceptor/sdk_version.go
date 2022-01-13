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

	"go.temporal.io/server/common/headers"
	"go.temporal.io/version/check"
	"google.golang.org/grpc"
)

type sdkNameVersion struct {
	name    string
	version string
}

type SDKVersionInterceptor struct {
	sdkInfoSet           map[sdkNameVersion]bool
	lock                 sync.RWMutex
	infoSetSizeCapGetter func() int
}

var _ grpc.UnaryServerInterceptor = (*TelemetryInterceptor)(nil).Intercept

func NewSDKVersionInterceptor(infoSetSizeCapGetter func() int) *SDKVersionInterceptor {
	return &SDKVersionInterceptor{
		sdkInfoSet:           make(map[sdkNameVersion]bool),
		lock:                 sync.RWMutex{},
		infoSetSizeCapGetter: infoSetSizeCapGetter,
	}
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
	setSizeCap := vi.infoSetSizeCapGetter()

	vi.lock.RLock()
	overCap := len(vi.sdkInfoSet) >= setSizeCap
	_, found := vi.sdkInfoSet[info]
	vi.lock.RUnlock()

	if !overCap && !found {
		vi.lock.Lock()
		vi.sdkInfoSet[info] = true
		vi.lock.Unlock()
	}
}

func (vi *SDKVersionInterceptor) GetAndResetSDKInfo() []check.SDKInfo {
	vi.lock.Lock()
	currSet := vi.sdkInfoSet
	vi.sdkInfoSet = make(map[sdkNameVersion]bool)
	vi.lock.Unlock()

	sdkInfo := make([]check.SDKInfo, 0, len(currSet))
	for k := range currSet {
		sdkInfo = append(sdkInfo, check.SDKInfo{Name: k.name, Version: k.version})
	}
	return sdkInfo
}
