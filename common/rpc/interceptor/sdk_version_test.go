package interceptor

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/versioninfo"
)

func TestSDKVersionRecorder(t *testing.T) {
	interceptor := &SDKVersionInterceptor{
		sdkInfoSet:     make(map[versioninfo.SDKInfo]struct{}),
		maxSetSize:     2,
		versionChecker: headers.NewDefaultVersionChecker(),
	}

	sdkVersion := "1.10.1"

	// Record first tuple
	ctx := headers.SetVersionsForTests(context.Background(), sdkVersion, headers.ClientNameGoSDK, headers.SupportedServerVersions, headers.AllFeatures)
	_, err := interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	assert.NoError(t, err)

	// Record second tuple
	ctx = headers.SetVersionsForTests(context.Background(), sdkVersion, headers.ClientNameTypeScriptSDK, headers.SupportedServerVersions, headers.AllFeatures)
	_, err = interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	assert.NoError(t, err)

	// Do not record when over capacity
	ctx = headers.SetVersionsForTests(context.Background(), sdkVersion, headers.ClientNameJavaSDK, headers.SupportedServerVersions, headers.AllFeatures)
	_, err = interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	assert.NoError(t, err)

	// Empty SDK version should not be recorded
	ctx = headers.SetVersionsForTests(context.Background(), "", headers.ClientNameGoSDK, headers.SupportedServerVersions, headers.AllFeatures)
	_, err = interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	assert.NoError(t, err)

	// Empty SDK name should not be recorded
	ctx = headers.SetVersionsForTests(context.Background(), sdkVersion, "", headers.SupportedServerVersions, headers.AllFeatures)
	_, err = interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	assert.NoError(t, err)

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
