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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.temporal.io/version/check"

	"go.temporal.io/server/common/headers"
)

func TestSDKVersionRecorder(t *testing.T) {
	interceptor := &SDKVersionInterceptor{
		sdkInfoSet: make(map[check.SDKInfo]struct{}),
		maxSetSize: 2,
	}

	sdkVersion := "1.10.1"

	// Record first tuple
	ctx := headers.SetVersionsForTests(context.Background(), sdkVersion, headers.ClientNameGoSDK, headers.SupportedServerVersions, headers.AllFeatures)
	interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})

	// Record second tuple
	ctx = headers.SetVersionsForTests(context.Background(), sdkVersion, headers.ClientNameTypeScriptSDK, headers.SupportedServerVersions, headers.AllFeatures)
	interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})

	// Do not record when over capacity
	ctx = headers.SetVersionsForTests(context.Background(), sdkVersion, headers.ClientNameJavaSDK, headers.SupportedServerVersions, headers.AllFeatures)
	interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})

	// Empty SDK version should not be recorded
	ctx = headers.SetVersionsForTests(context.Background(), "", headers.ClientNameGoSDK, headers.SupportedServerVersions, headers.AllFeatures)
	interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})

	// Empty SDK name should not be recorded
	ctx = headers.SetVersionsForTests(context.Background(), sdkVersion, "", headers.SupportedServerVersions, headers.AllFeatures)
	interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})

	info := interceptor.GetAndResetSDKInfo()
	sort.SliceStable(info, func(i, j int) bool {
		return info[i].Name < info[j].Name
	})
	assert.Equal(t, 2, len(info))
	assert.Equal(t, headers.ClientNameGoSDK, info[0].Name)
	assert.Equal(t, sdkVersion, info[0].Version)
	assert.Equal(t, headers.ClientNameTypeScriptSDK, info[1].Name)
	assert.Equal(t, sdkVersion, info[1].Version)
}
