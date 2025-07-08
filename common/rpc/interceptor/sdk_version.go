package interceptor

import (
	"context"
	"sync"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/versioninfo"
	"google.golang.org/grpc"
)

type SDKVersionInterceptor struct {
	sync.RWMutex
	sdkInfoSet     map[versioninfo.SDKInfo]struct{}
	versionChecker headers.VersionChecker
	maxSetSize     int
}

const defaultMaxSetSize = 100

// NewSDKVersionInterceptor creates a new SDKVersionInterceptor with default max set size
func NewSDKVersionInterceptor() *SDKVersionInterceptor {
	return &SDKVersionInterceptor{
		sdkInfoSet:     make(map[versioninfo.SDKInfo]struct{}),
		versionChecker: headers.NewDefaultVersionChecker(),
		maxSetSize:     defaultMaxSetSize,
	}
}

// Intercept a grpc request
func (vi *SDKVersionInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	sdkName, sdkVersion := headers.GetClientNameAndVersion(ctx)
	if sdkName != "" && sdkVersion != "" {
		vi.RecordSDKInfo(sdkName, sdkVersion)
		if err := vi.versionChecker.ClientSupported(ctx); err != nil {
			return nil, err
		}
	}
	return handler(ctx, req)
}

// RecordSDKInfo records name and version tuple in memory
func (vi *SDKVersionInterceptor) RecordSDKInfo(name, version string) {
	info := versioninfo.SDKInfo{Name: name, Version: version}

	vi.RLock()
	overCap := len(vi.sdkInfoSet) >= vi.maxSetSize
	_, found := vi.sdkInfoSet[info]
	vi.RUnlock()

	if !overCap && !found {
		vi.Lock()
		vi.sdkInfoSet[info] = struct{}{}
		vi.Unlock()
	}
}

// GetAndResetSDKInfo gets all recorded name, version tuples and resets internal records
func (vi *SDKVersionInterceptor) GetAndResetSDKInfo() []versioninfo.SDKInfo {
	vi.Lock()
	currSet := vi.sdkInfoSet
	vi.sdkInfoSet = make(map[versioninfo.SDKInfo]struct{})
	vi.Unlock()

	sdkInfo := make([]versioninfo.SDKInfo, 0, len(currSet))
	for k := range currSet {
		sdkInfo = append(sdkInfo, k)
	}
	return sdkInfo
}
