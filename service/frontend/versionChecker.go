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

package frontend

import (
	"runtime"
	"sync"
	"time"

	enumsbp "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	versionpb "go.temporal.io/api/version/v1"
	"go.temporal.io/version/check"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
)

const VersionCheckInterval = 24 * time.Hour

type VersionChecker struct {
	resource.Resource
	config       *Config
	params       *resource.BootstrapParams
	shutdownChan chan struct{}
	metricsScope metrics.Scope
	startOnce    sync.Once
	stopOnce     sync.Once
}

func NewVersionChecker(
	resource resource.Resource,
	params *resource.BootstrapParams,
	config *Config,
) *VersionChecker {
	return &VersionChecker{Resource: resource, config: config, params: params, shutdownChan: make(chan struct{}),
		metricsScope: resource.GetMetricsClient().Scope(metrics.VersionCheckScope)}
}

func (vc *VersionChecker) Start() {
	if vc.config.EnableServerVersionCheck() {
		vc.startOnce.Do(func() {
			go vc.versionCheckLoop()
		})
	}
}

func (vc *VersionChecker) Stop() {
	if vc.config.EnableServerVersionCheck() {
		vc.stopOnce.Do(func() {
			close(vc.shutdownChan)
		})
	}
}

func (vc *VersionChecker) versionCheckLoop() {
	timer := time.NewTicker(VersionCheckInterval)
	defer timer.Stop()
	vc.performVersionCheck()
	for {
		select {
		case <-vc.shutdownChan:
			return
		case <-timer.C:
			vc.performVersionCheck()
		}
	}
}

func (vc *VersionChecker) performVersionCheck() {
	sw := vc.metricsScope.StartTimer(metrics.VersionCheckLatency)
	defer sw.Stop()
	clusterMetadataManager := vc.GetClusterMetadataManager()
	metadata, err := clusterMetadataManager.GetClusterMetadata()
	if err != nil {
		vc.metricsScope.IncCounter(metrics.VersionCheckFailedCount)
		return
	}

	if !isUpdateNeeded(metadata) {
		return
	}

	req, err := vc.createVersionCheckRequest(metadata)
	if err != nil {
		vc.metricsScope.IncCounter(metrics.VersionCheckFailedCount)
		return
	}
	resp, err := vc.getVersionInfo(req)
	if err != nil {
		vc.metricsScope.IncCounter(metrics.VersionCheckRequestFailedCount)
		vc.metricsScope.IncCounter(metrics.VersionCheckFailedCount)
		return
	}
	err = vc.saveVersionInfo(resp)
	if err != nil {
		vc.metricsScope.IncCounter(metrics.VersionCheckFailedCount)
		return
	}
	vc.metricsScope.IncCounter(metrics.VersionCheckSuccessCount)
}

func isUpdateNeeded(metadata *persistence.GetClusterMetadataResponse) bool {
	return metadata.VersionInfo == nil || (metadata.VersionInfo.LastUpdateTime != nil &&
		metadata.VersionInfo.LastUpdateTime.Before(time.Now().Add(-time.Hour)))
}

func (vc *VersionChecker) createVersionCheckRequest(metadata *persistence.GetClusterMetadataResponse) (*check.VersionCheckRequest, error) {
	return &check.VersionCheckRequest{
		Product:   headers.ClientNameServer,
		Version:   headers.ServerVersion,
		Arch:      runtime.GOARCH,
		OS:        runtime.GOOS,
		DB:        vc.GetClusterMetadataManager().GetName(),
		ClusterID: metadata.ClusterId,
		Timestamp: time.Now().UnixNano(),
	}, nil
}

func (vc *VersionChecker) getVersionInfo(req *check.VersionCheckRequest) (*check.VersionCheckResponse, error) {
	return check.NewCaller().Call(req)
}

func (vc *VersionChecker) saveVersionInfo(resp *check.VersionCheckResponse) error {
	clusterMetadataManager := vc.GetClusterMetadataManager()
	metadata, err := clusterMetadataManager.GetClusterMetadata()
	if err != nil {
		return err
	}
	metadata.VersionInfo = toVersionInfo(resp)
	saved, err := clusterMetadataManager.SaveClusterMetadata(&persistence.SaveClusterMetadataRequest{
		ClusterMetadata: metadata.ClusterMetadata, Version: metadata.Version})
	if err != nil {
		return err
	}
	if !saved {
		return serviceerror.NewInternal("version info update hasn't been applied")
	}
	return nil
}

func toVersionInfo(resp *check.VersionCheckResponse) *versionpb.VersionInfo {
	return &versionpb.VersionInfo{
		Current:        convertReleaseInfo(resp.Current),
		Recommended:    convertReleaseInfo(resp.Recommended),
		Instructions:   resp.Instructions,
		Alerts:         convertAlerts(resp.Alerts),
		LastUpdateTime: timestamp.TimePtr(time.Now().UTC()),
	}
}

func convertAlerts(alerts []check.Alert) []*versionpb.Alert {
	var result []*versionpb.Alert
	for _, alert := range alerts {
		result = append(result, &versionpb.Alert{
			Message:  alert.Message,
			Severity: enumsbp.Severity(alert.Severity),
		})
	}
	return result
}

func convertReleaseInfo(releaseInfo check.ReleaseInfo) *versionpb.ReleaseInfo {
	return &versionpb.ReleaseInfo{
		Version:     releaseInfo.Version,
		ReleaseTime: timestamp.UnixOrZeroTimePtr(releaseInfo.ReleaseTime),
		Notes:       releaseInfo.Notes,
	}
}
