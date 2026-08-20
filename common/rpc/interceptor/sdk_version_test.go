package interceptor

import (
	"context"
	"sort"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/headers"
	interceptornexus "go.temporal.io/server/common/rpc/interceptor/nexus"
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
	_, err := interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	})
	assert.NoError(t, err)

	// Record second tuple
	ctx = headers.SetVersionsForTests(context.Background(), sdkVersion, headers.ClientNameTypeScriptSDK, headers.SupportedServerVersions, headers.AllFeatures)
	_, err = interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	})
	assert.NoError(t, err)

	// Do not record when over capacity
	ctx = headers.SetVersionsForTests(context.Background(), sdkVersion, headers.ClientNameJavaSDK, headers.SupportedServerVersions, headers.AllFeatures)
	_, err = interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	})
	assert.NoError(t, err)

	// Empty SDK version should not be recorded
	ctx = headers.SetVersionsForTests(context.Background(), "", headers.ClientNameGoSDK, headers.SupportedServerVersions, headers.AllFeatures)
	_, err = interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	})
	assert.NoError(t, err)

	// Empty SDK name should not be recorded
	ctx = headers.SetVersionsForTests(context.Background(), sdkVersion, "", headers.SupportedServerVersions, headers.AllFeatures)
	_, err = interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req any) (any, error) {
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

func TestSDKVersionInterceptNexus(t *testing.T) {
	clientVersion := "1.10.1"
	for _, tc := range []struct {
		name            string
		ctx             context.Context
		expectedOutcome string
	}{
		{
			name: "supported client",
			ctx: headers.SetVersionsForTests(
				context.Background(),
				clientVersion,
				headers.ClientNameGoSDK,
				headers.SupportedServerVersions,
				headers.AllFeatures,
			),
		},
		{
			name: "unsupported client",
			ctx: headers.SetVersionsForTests(
				context.Background(),
				"unparseable.client.version",
				headers.ClientNameGoSDK,
				headers.SupportedServerVersions,
				headers.AllFeatures,
			),
			expectedOutcome: "unsupported_client",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			interceptor := NewSDKVersionInterceptor()
			nextCalled := false
			_, err := interceptor.InterceptNexus(
				tc.ctx,
				interceptornexus.NewStartOpInput("s", "o", testNamespace, nexus.StartOperationOptions{}, nil),
				func(context.Context, interceptornexus.InterceptorInput) (any, error) {
					nextCalled = true
					return nil, nil
				},
			)
			if tc.expectedOutcome != "" {
				var interceptorErr *interceptornexus.InterceptorError
				require.ErrorAs(t, err, &interceptorErr)
				require.Equal(t, tc.expectedOutcome, interceptorErr.Outcome)
				require.False(t, nextCalled)
			} else {
				require.True(t, nextCalled)
				require.NoError(t, err)
				require.Contains(t, interceptor.GetAndResetSDKInfo(), versioninfo.SDKInfo{Name: headers.ClientNameGoSDK, Version: clientVersion})
			}
		})
	}
}
