package frontend

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/blang/semver/v4"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	versionpb "go.temporal.io/api/version/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/versioninfo"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const VersionCheckInterval = 24 * time.Hour

const (
	versionCheckTimeout         = 10 * time.Second
	versionCheckShutdownTimeout = 2 * time.Second
)

type VersionChecker struct {
	config                 *Config
	logger                 log.Logger
	shutdownChan           chan struct{}
	doneChan               chan struct{}
	metricsHandler         metrics.Handler
	clusterMetadataManager persistence.ClusterMetadataManager
	startOnce              sync.Once
	stopOnce               sync.Once
	sdkVersionRecorder     *interceptor.SDKVersionInterceptor
	versionInfoCaller      versioninfo.Caller
	versionCheckContext    context.Context
	cancelVersionCheck     context.CancelFunc
}

func NewVersionChecker(
	config *Config,
	metricsHandler metrics.Handler,
	clusterMetadataManager persistence.ClusterMetadataManager,
	sdkVersionRecorder *interceptor.SDKVersionInterceptor,
	logger log.SnTaggedLogger,
) *VersionChecker {
	ctx, cancel := context.WithCancel(headers.SetCallerInfo(context.Background(), headers.SystemBackgroundHighCallerInfo))
	return &VersionChecker{
		config:                 config,
		logger:                 logger,
		shutdownChan:           make(chan struct{}),
		doneChan:               make(chan struct{}),
		metricsHandler:         metricsHandler.WithTags(metrics.OperationTag(metrics.VersionCheckScope)),
		clusterMetadataManager: clusterMetadataManager,
		sdkVersionRecorder:     sdkVersionRecorder,
		versionInfoCaller:      versioninfo.NewCaller(),
		versionCheckContext:    ctx,
		cancelVersionCheck:     cancel,
	}
}

func (vc *VersionChecker) Start() {
	if vc.config.EnableServerVersionCheck() {
		vc.startOnce.Do(func() {
			go vc.versionCheckLoop(vc.versionCheckContext)
		})
	}
}

func (vc *VersionChecker) Stop() {
	vc.stopOnce.Do(func() {
		vc.cancelVersionCheck()
		close(vc.shutdownChan)
		vc.startOnce.Do(func() { close(vc.doneChan) })
	})
	<-vc.doneChan
}

func (vc *VersionChecker) versionCheckLoop(
	ctx context.Context,
) {
	defer close(vc.doneChan)
	timer := time.NewTicker(VersionCheckInterval)
	defer timer.Stop()
	vc.performVersionCheck(ctx)
	for {
		select {
		case <-vc.shutdownChan:
			vc.checkVersion(context.WithoutCancel(ctx), true)
			return
		case <-timer.C:
			vc.performVersionCheck(ctx)
		}
	}
}

func (vc *VersionChecker) performVersionCheck(
	ctx context.Context,
) {
	vc.checkVersion(ctx, false)
}

func (vc *VersionChecker) checkVersion(ctx context.Context, shutdown bool) {
	if !vc.config.EnableServerVersionCheck() {
		return
	}
	timeout := versionCheckTimeout
	checkType := "regular"
	if shutdown {
		timeout = versionCheckShutdownTimeout
		checkType = "shutdown"
	}
	metricsHandler := vc.metricsHandler.WithTags(metrics.VersionCheckTypeTag(checkType))
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	startTime := time.Now().UTC()
	defer func() {
		if ctx.Err() != context.Canceled {
			metrics.VersionCheckLatency.With(metricsHandler).Record(time.Since(startTime))
		}
	}()
	metadata, err := vc.clusterMetadataManager.GetCurrentClusterMetadata(ctx)
	if err != nil {
		if ctx.Err() != context.Canceled {
			metrics.VersionCheckFailedCount.With(metricsHandler).Record(1)
		}
		return
	}

	if !shutdown && !isUpdateNeeded(metadata) {
		return
	}

	req, err := vc.createVersionCheckRequest(metadata)
	if err != nil {
		metrics.VersionCheckFailedCount.With(metricsHandler).Record(1)
		return
	}
	resp, err := vc.getVersionInfo(ctx, req)
	if err != nil {
		if ctx.Err() == context.Canceled {
			// Include observations from the canceled check in the final report.
			for _, sdk := range req.SDKInfo {
				vc.sdkVersionRecorder.RecordSDKInfo(sdk.Name, sdk.Version)
			}
		} else {
			metrics.VersionCheckRequestFailedCount.With(metricsHandler).Record(1)
			metrics.VersionCheckFailedCount.With(metricsHandler).Record(1)
		}
		return
	}
	vc.logVersionInfo(resp)
	err = vc.saveVersionInfo(ctx, resp, shutdown)
	if err != nil {
		if ctx.Err() != context.Canceled {
			metrics.VersionCheckFailedCount.With(metricsHandler).Record(1)
		}
		return
	}
	metrics.VersionCheckSuccessCount.With(metricsHandler).Record(1)
}

func (vc *VersionChecker) logVersionInfo(resp *versioninfo.VersionCheckResponse) {
	for _, product := range resp.Products {
		current, currentErr := semver.Parse(product.Current.Version)
		recommended, recommendedErr := semver.Parse(product.Recommended.Version)
		upgradeRecommended := currentErr == nil && recommendedErr == nil && recommended.GT(current)
		if !upgradeRecommended && len(product.Alerts) == 0 {
			continue
		}
		message := "Version check reported issues; review the alerts"
		tags := []tag.Tag{
			tag.NewStringTag("product", product.Product),
			tag.NewStringTag("current-version", product.Current.Version),
		}
		if upgradeRecommended {
			message = "Upgrade to a new version is recommended"
			tags = append(tags,
				tag.NewStringTag("recommended-version", product.Recommended.Version),
				tag.NewTimeTag("release-time", timestamp.UnixOrZeroTime(product.Recommended.ReleaseTime)),
				tag.NewStringTag("release-notes", product.Recommended.Notes),
			)
		}
		if product.Instructions != "" {
			tags = append(tags, tag.NewStringTag("upgrade-instructions", product.Instructions))
		}
		if len(product.Alerts) > 0 {
			tags = append(tags, tag.NewAnyTag("alerts", product.Alerts))
		}
		warn := false
		for _, alert := range product.Alerts {
			if alert.Severity == versioninfo.SeverityHigh || alert.Severity == versioninfo.SeverityMedium {
				warn = true
				break
			}
		}
		if warn {
			vc.logger.Warn(message, tags...)
		} else {
			vc.logger.Info(message, tags...)
		}
	}
}

func isUpdateNeeded(metadata *persistence.GetClusterMetadataResponse) bool {
	if metadata.VersionInfo == nil {
		return true
	}
	// Check if the server version has changed since last version check.
	// This ensures upgrade notifications are cleared after cluster upgrade.
	if metadata.VersionInfo.Current == nil ||
		metadata.VersionInfo.Current.Version != headers.ServerVersion {
		return true
	}
	return metadata.VersionInfo.LastUpdateTime != nil &&
		metadata.VersionInfo.LastUpdateTime.AsTime().Before(time.Now().Add(-time.Hour))
}

func (vc *VersionChecker) createVersionCheckRequest(metadata *persistence.GetClusterMetadataResponse) (*versioninfo.VersionCheckRequest, error) {
	return &versioninfo.VersionCheckRequest{
		Product:   headers.ClientNameServer,
		Version:   headers.ServerVersion,
		Arch:      runtime.GOARCH,
		OS:        runtime.GOOS,
		DB:        vc.clusterMetadataManager.GetName(),
		ClusterID: metadata.ClusterId,
		Timestamp: time.Now().UnixNano(),
		SDKInfo:   vc.sdkVersionRecorder.GetAndResetSDKInfo(),
	}, nil
}

func (vc *VersionChecker) getVersionInfo(ctx context.Context, req *versioninfo.VersionCheckRequest) (*versioninfo.VersionCheckResponse, error) {
	return vc.versionInfoCaller.CallContext(ctx, req)
}

func (vc *VersionChecker) saveVersionInfo(ctx context.Context, resp *versioninfo.VersionCheckResponse, ignoreConflict bool) error {
	metadata, err := vc.clusterMetadataManager.GetCurrentClusterMetadata(ctx)
	if err != nil {
		return err
	}
	// TODO(bergundy): Extract and save version info per SDK
	versionInfo, err := toVersionInfo(resp)
	if err != nil {
		return err
	}
	metadata.VersionInfo = versionInfo
	saved, err := vc.clusterMetadataManager.SaveClusterMetadata(ctx, &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: metadata.ClusterMetadata, Version: metadata.Version})
	if err != nil {
		return err
	}
	if !saved && !ignoreConflict {
		return serviceerror.NewUnavailable("version info update hasn't been applied")
	}
	return nil
}

func toVersionInfo(resp *versioninfo.VersionCheckResponse) (*versionpb.VersionInfo, error) {
	for _, product := range resp.Products {
		if product.Product == headers.ClientNameServer {
			return &versionpb.VersionInfo{
				Current:        convertReleaseInfo(product.Current),
				Recommended:    convertReleaseInfo(product.Recommended),
				Instructions:   product.Instructions,
				Alerts:         convertAlerts(product.Alerts),
				LastUpdateTime: timestamppb.New(time.Now().UTC()),
			}, nil
		}
	}
	return nil, serviceerror.NewNotFound("version info update was not found in response")
}

func convertAlerts(alerts []versioninfo.Alert) []*versionpb.Alert {
	var result []*versionpb.Alert
	for _, alert := range alerts {
		result = append(result, &versionpb.Alert{
			Message:  alert.Message,
			Severity: enumspb.Severity(alert.Severity),
		})
	}
	return result
}

func convertReleaseInfo(releaseInfo versioninfo.ReleaseInfo) *versionpb.ReleaseInfo {
	return &versionpb.ReleaseInfo{
		Version:     releaseInfo.Version,
		ReleaseTime: timestamp.UnixOrZeroTimePtr(releaseInfo.ReleaseTime),
		Notes:       releaseInfo.Notes,
	}
}
