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
	"go.temporal.io/server/common/metrics"
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
	metricsClient        metrics.Client
}

var _ grpc.UnaryServerInterceptor = (*TelemetryInterceptor)(nil).Intercept

func NewSDKVersionInterceptor(infoSetSizeCapGetter func() int, metricsClient metrics.Client) *SDKVersionInterceptor {
	return &SDKVersionInterceptor{
		sdkInfoSet:           make(map[sdkNameVersion]bool),
		lock:                 sync.RWMutex{},
		infoSetSizeCapGetter: infoSetSizeCapGetter,
		metricsClient:        metricsClient,
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
	scope := vi.metricsClient.Scope(metrics.SDKVersionRecordScope)
	info := sdkNameVersion{name, version}
	counter := metrics.SDKVersionRecordSuccessCount
	setSizeCap := vi.infoSetSizeCapGetter()
	shouldUpdate := false

	// Increment after unlocking
	defer func() { scope.IncCounter(counter) }()

	// Update after unlocking read lock
	defer func() {
		if shouldUpdate {
			vi.lock.Lock()
			vi.sdkInfoSet[info] = true
			vi.lock.Unlock()
		}
	}()

	vi.lock.RLock()
	defer vi.lock.RUnlock()

	if len(vi.sdkInfoSet) >= setSizeCap {
		counter = metrics.SDKVersionRecordFailedCount
		return
	}
	_, found := vi.sdkInfoSet[info]
	shouldUpdate = !found
}

func (vi *SDKVersionInterceptor) GetAndResetSDKInfo() []check.SDKInfo {
	vi.lock.Lock()
	defer vi.lock.Unlock()
	sdkInfo := make([]check.SDKInfo, len(vi.sdkInfoSet))
	i := 0
	for k := range vi.sdkInfoSet {
		sdkInfo[i] = check.SDKInfo{Name: k.name, Version: k.version}
		i += 1
	}
	vi.sdkInfoSet = make(map[sdkNameVersion]bool)
	return sdkInfo
}
