package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	versionpb "go.temporal.io/api/version/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/versioninfo"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestVersionCheckerShutdown(t *testing.T) {
	for _, test := range []struct {
		name              string
		disabled          bool
		disableAfterStart bool
		metadataError     error
		status            int
		upgrade           bool
		saveError         error
		conflict          bool
	}{
		{name: "fresh metadata", status: http.StatusOK},
		{name: "upgrade recommended", status: http.StatusOK, upgrade: true},
		{name: "save conflict", status: http.StatusOK, conflict: true},
		{name: "save error", status: http.StatusOK, saveError: errors.New("database unavailable")},
		{name: "disabled", disabled: true},
		{name: "disabled after start", disableAfterStart: true},
		{name: "metadata failure", metadataError: errors.New("metadata unavailable")},
		{name: "request failure", status: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			product := versioninfo.ProductVersionReport{
				Product:     headers.ClientNameServer,
				Current:     versioninfo.ReleaseInfo{Version: headers.ServerVersion},
				Recommended: versioninfo.ReleaseInfo{Version: headers.ServerVersion},
				Alerts:      []versioninfo.Alert{{Message: "test alert", Severity: versioninfo.SeverityLow}},
			}
			if test.upgrade {
				product.Current.Version = "1.0.0"
				product.Recommended.Version = "1.1.0"
			}
			// Count received requests, including those that get an error response.
			var requests atomic.Int32
			reported := make(chan versioninfo.VersionCheckRequest, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				var request versioninfo.VersionCheckRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				reported <- request
				w.WriteHeader(test.status)
				if test.status == http.StatusOK {
					err := json.NewEncoder(w).Encode(versioninfo.VersionCheckResponse{
						Products: []versioninfo.ProductVersionReport{product},
					})
					if err != nil {
						t.Error(err)
					}
				}
			}))
			defer server.Close()
			endpoint, err := url.Parse(server.URL)
			require.NoError(t, err)

			controller := gomock.NewController(t)
			manager := persistence.NewMockClusterMetadataManager(controller)
			logger := log.NewMockLogger(controller)
			// Fresh metadata suppresses the startup request, isolating the shutdown report.
			metadata := &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					ClusterId: "test-cluster",
					VersionInfo: &versionpb.VersionInfo{
						Current:        &versionpb.ReleaseInfo{Version: headers.ServerVersion},
						LastUpdateTime: timestamppb.Now(),
					},
				},
			}
			manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(metadata, test.metadataError).AnyTimes()
			manager.EXPECT().GetName().Return("test-db").AnyTimes()
			wantRequests := !test.disabled && !test.disableAfterStart && test.metadataError == nil
			saves := make(chan *persistence.SaveClusterMetadataRequest, 1)
			if wantRequests && test.status == http.StatusOK {
				message := "Version check reported issues; review the alerts"
				tags := []any{
					tag.NewStringTag("product", product.Product),
					tag.NewStringTag("current-version", product.Current.Version),
				}
				if test.upgrade {
					message = "Upgrade to a new version is recommended"
					tags = append(tags,
						tag.NewStringTag("recommended-version", product.Recommended.Version),
						tag.NewTimeTag("release-time", time.Unix(0, 0).UTC()),
						tag.NewStringTag("release-notes", ""),
					)
				}
				tags = append(tags, tag.NewAnyTag("alerts", product.Alerts))
				gomock.InOrder(
					logger.EXPECT().Info(message, tags...),
					manager.EXPECT().SaveClusterMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
						func(_ context.Context, request *persistence.SaveClusterMetadataRequest) (bool, error) {
							saves <- request
							return !test.conflict, test.saveError
						},
					),
				)
			}
			var enabled atomic.Bool
			enabled.Store(!test.disabled)
			recorder := interceptor.NewSDKVersionInterceptor()
			recorder.RecordSDKInfo("temporal-go", "1.0.0")
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)
			checker := NewVersionChecker(&Config{
				EnableServerVersionCheck: enabled.Load,
			}, handler, manager, recorder, logger)
			checker.versionInfoCaller = versioninfo.Caller{Scheme: endpoint.Scheme, Host: endpoint.Host}
			checker.Start()
			if test.disableAfterStart {
				enabled.Store(false)
			}
			checker.Stop()
			// Repeated lifecycle calls must not send another report or close channels twice.
			checker.Stop()
			checker.Start()
			if !wantRequests {
				require.Zero(t, requests.Load())
				return
			}
			require.EqualValues(t, 1, requests.Load())
			request := <-reported
			require.Equal(t, "test-cluster", request.ClusterID)
			require.Equal(t, headers.ServerVersion, request.Version)
			require.Equal(t, []versioninfo.SDKInfo{{Name: "temporal-go", Version: "1.0.0"}}, request.SDKInfo)
			require.Positive(t, request.Timestamp)
			if test.status == http.StatusOK {
				require.Len(t, saves, 1)
				saved := <-saves
				require.Equal(t, product.Current.Version, saved.VersionInfo.Current.Version)
				require.Equal(t, product.Recommended.Version, saved.VersionInfo.Recommended.Version)
				require.NotNil(t, saved.VersionInfo.LastUpdateTime)
			}
			snapshot := capture.Snapshot()
			if test.status == http.StatusOK && test.saveError == nil {
				require.Len(t, snapshot[metrics.VersionCheckSuccessCount.Name()], 1)
				require.Empty(t, snapshot[metrics.VersionCheckFailedCount.Name()])
			} else {
				require.Empty(t, snapshot[metrics.VersionCheckSuccessCount.Name()])
				require.Len(t, snapshot[metrics.VersionCheckFailedCount.Name()], 1)
			}
		})
	}
}

func TestVersionCheckerStopBeforeStart(t *testing.T) {
	checker := NewVersionChecker(&Config{
		EnableServerVersionCheck: dynamicconfig.GetBoolPropertyFn(true),
	}, metrics.NoopMetricsHandler, persistence.NewMockClusterMetadataManager(gomock.NewController(t)), interceptor.NewSDKVersionInterceptor(), log.NewNoopLogger())
	checker.Stop()
	checker.Start()
	checker.Stop()
}

func TestVersionCheckerRequestCancellation(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	endpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	manager := persistence.NewMockClusterMetadataManager(gomock.NewController(t))
	manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: &persistencespb.ClusterMetadata{ClusterId: "test-cluster"},
	}, nil)
	manager.EXPECT().GetName().Return("test-db")
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	checker := NewVersionChecker(&Config{
		EnableServerVersionCheck: dynamicconfig.GetBoolPropertyFn(true),
	}, handler, manager, interceptor.NewSDKVersionInterceptor(), log.NewNoopLogger())
	checker.versionInfoCaller = versioninfo.Caller{Scheme: endpoint.Scheme, Host: endpoint.Host}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	checker.checkVersion(ctx, false)
	require.Zero(t, requests.Load())
	snapshot := capture.Snapshot()
	require.Empty(t, snapshot[metrics.VersionCheckLatency.Name()])
	require.Empty(t, snapshot[metrics.VersionCheckSuccessCount.Name()])
	require.Empty(t, snapshot[metrics.VersionCheckFailedCount.Name()])
	require.Empty(t, snapshot[metrics.VersionCheckRequestFailedCount.Name()])
}

func TestVersionCheckerDisabledPeriodicCheck(t *testing.T) {
	var enabled atomic.Bool
	enabled.Store(true)
	manager := persistence.NewMockClusterMetadataManager(gomock.NewController(t))
	manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			VersionInfo: &versionpb.VersionInfo{
				Current:        &versionpb.ReleaseInfo{Version: headers.ServerVersion},
				LastUpdateTime: timestamppb.Now(),
			},
		},
	}, nil)
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	checker := NewVersionChecker(&Config{EnableServerVersionCheck: enabled.Load}, handler,
		manager, interceptor.NewSDKVersionInterceptor(), log.NewNoopLogger())
	checker.performVersionCheck(context.Background())
	enabled.Store(false)
	checker.performVersionCheck(context.Background())
	recordings := capture.Snapshot()[metrics.VersionCheckLatency.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, "regular", recordings[0].Tags["version_check_type"])
}

func TestVersionCheckerStopCancelsInFlightCheck(t *testing.T) {
	started := make(chan struct{})
	reported := make(chan versioninfo.VersionCheckRequest, 1)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request versioninfo.VersionCheckRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
			return
		}
		if requests.Add(1) == 1 {
			close(started)
			<-r.Context().Done()
			return
		}
		reported <- request
		if err := json.NewEncoder(w).Encode(versioninfo.VersionCheckResponse{
			Products: []versioninfo.ProductVersionReport{{
				Product:     headers.ClientNameServer,
				Current:     versioninfo.ReleaseInfo{Version: headers.ServerVersion},
				Recommended: versioninfo.ReleaseInfo{Version: headers.ServerVersion},
				Alerts:      []versioninfo.Alert{{Message: "test alert", Severity: versioninfo.SeverityHigh}},
			}},
		}); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	endpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	controller := gomock.NewController(t)
	manager := persistence.NewMockClusterMetadataManager(controller)
	logger := log.NewMockLogger(controller)
	manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: &persistencespb.ClusterMetadata{ClusterId: "test-cluster"},
	}, nil).Times(3)
	manager.EXPECT().GetName().Return("test-db").Times(2)
	manager.EXPECT().SaveClusterMetadata(gomock.Any(), gomock.Any()).Return(true, nil)
	logger.EXPECT().Warn("Version check reported issues; review the alerts",
		tag.NewStringTag("product", headers.ClientNameServer),
		tag.NewStringTag("current-version", headers.ServerVersion),
		tag.NewAnyTag("alerts", []versioninfo.Alert{{Message: "test alert", Severity: versioninfo.SeverityHigh}}),
	)
	recorder := interceptor.NewSDKVersionInterceptor()
	recorder.RecordSDKInfo("temporal-java", "1.0.0")
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	checker := NewVersionChecker(&Config{EnableServerVersionCheck: dynamicconfig.GetBoolPropertyFn(true)},
		handler, manager, recorder, logger)
	checker.versionInfoCaller = versioninfo.Caller{Scheme: endpoint.Scheme, Host: endpoint.Host}
	checker.Start()
	<-started
	recorder.RecordSDKInfo("temporal-go", "1.0.0")
	done := make(chan struct{})
	go func() {
		checker.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not cancel the in-flight check")
	}
	require.Equal(t, int32(2), requests.Load())
	require.ElementsMatch(t, []versioninfo.SDKInfo{
		{Name: "temporal-java", Version: "1.0.0"},
		{Name: "temporal-go", Version: "1.0.0"},
	}, (<-reported).SDKInfo)
	snapshot := capture.Snapshot()
	require.Len(t, snapshot[metrics.VersionCheckSuccessCount.Name()], 1)
	require.Equal(t, "shutdown", snapshot[metrics.VersionCheckSuccessCount.Name()][0].Tags["version_check_type"])
	require.Len(t, snapshot[metrics.VersionCheckLatency.Name()], 1)
	require.Equal(t, "shutdown", snapshot[metrics.VersionCheckLatency.Name()][0].Tags["version_check_type"])
	require.Empty(t, snapshot[metrics.VersionCheckFailedCount.Name()])
}

func TestVersionCheckerShutdownDeadline(t *testing.T) {
	manager := persistence.NewMockClusterMetadataManager(gomock.NewController(t))
	manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).DoAndReturn(func(ctx context.Context) (*persistence.GetClusterMetadataResponse, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), versionCheckShutdownTimeout)
		return nil, context.DeadlineExceeded
	})
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	checker := NewVersionChecker(&Config{EnableServerVersionCheck: dynamicconfig.GetBoolPropertyFn(true)},
		handler, manager, interceptor.NewSDKVersionInterceptor(), log.NewNoopLogger())
	checker.checkVersion(context.Background(), true)
	snapshot := capture.Snapshot()
	require.Len(t, snapshot[metrics.VersionCheckFailedCount.Name()], 1)
	require.Equal(t, "shutdown", snapshot[metrics.VersionCheckFailedCount.Name()][0].Tags["version_check_type"])
	require.Len(t, snapshot[metrics.VersionCheckLatency.Name()], 1)
	require.Equal(t, "shutdown", snapshot[metrics.VersionCheckLatency.Name()][0].Tags["version_check_type"])
}

func TestVersionCheckerLogsBeforeSaveFailure(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "save error", err: errors.New("save failed")},
		{name: "CAS conflict"},
	} {
		t.Run(test.name, func(t *testing.T) {
			alerts := []versioninfo.Alert{{Message: "test alert", Severity: versioninfo.SeverityLow}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if err := json.NewEncoder(w).Encode(versioninfo.VersionCheckResponse{
					Products: []versioninfo.ProductVersionReport{{
						Product:     headers.ClientNameServer,
						Current:     versioninfo.ReleaseInfo{Version: "1.0.0"},
						Recommended: versioninfo.ReleaseInfo{Version: "1.0.0"},
						Alerts:      alerts,
					}},
				}); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			endpoint, err := url.Parse(server.URL)
			require.NoError(t, err)
			controller := gomock.NewController(t)
			manager := persistence.NewMockClusterMetadataManager(controller)
			logger := log.NewMockLogger(controller)
			manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{ClusterId: "test-cluster"},
			}, nil).Times(2)
			manager.EXPECT().GetName().Return("test-db")
			gomock.InOrder(
				logger.EXPECT().Info("Version check reported issues; review the alerts",
					tag.NewStringTag("product", headers.ClientNameServer),
					tag.NewStringTag("current-version", "1.0.0"),
					tag.NewAnyTag("alerts", alerts),
				),
				manager.EXPECT().SaveClusterMetadata(gomock.Any(), gomock.Any()).Return(false, test.err),
			)
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)
			checker := NewVersionChecker(&Config{EnableServerVersionCheck: dynamicconfig.GetBoolPropertyFn(true)},
				handler, manager, interceptor.NewSDKVersionInterceptor(), logger)
			checker.versionInfoCaller = versioninfo.Caller{Scheme: endpoint.Scheme, Host: endpoint.Host}
			checker.performVersionCheck(context.Background())
			snapshot := capture.Snapshot()
			require.Len(t, snapshot[metrics.VersionCheckFailedCount.Name()], 1)
			require.Empty(t, snapshot[metrics.VersionCheckSuccessCount.Name()])
		})
	}
}

func TestVersionCheckerLogRecommendations(t *testing.T) {
	for _, test := range []struct {
		name        string
		product     string
		current     string
		recommended string
		wantLog     bool
	}{
		{name: "server upgrade", product: headers.ClientNameServer, current: "1.0.0", recommended: "1.1.0", wantLog: true},
		{name: "SDK upgrade", product: headers.ClientNameGoSDK, current: "1.0.0", recommended: "2.0.0", wantLog: true},
		{name: "equal", current: "1.0.0", recommended: "1.0.0"},
		{name: "older", current: "2.0.0", recommended: "1.0.0"},
		{name: "numeric comparison", current: "1.9.0", recommended: "1.10.0", wantLog: true},
		{name: "prerelease to release", current: "1.0.0-rc.1", recommended: "1.0.0", wantLog: true},
		{name: "build metadata", current: "1.0.0+build.1", recommended: "1.0.0+build.2"},
		{name: "invalid current", current: "invalid", recommended: "1.0.0"},
		{name: "invalid recommended", current: "1.0.0", recommended: "invalid"},
		{name: "missing versions"},
	} {
		t.Run(test.name, func(t *testing.T) {
			logger := log.NewMockLogger(gomock.NewController(t))
			releaseTime := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
			if test.wantLog {
				logger.EXPECT().Info("Upgrade to a new version is recommended",
					tag.NewStringTag("product", test.product),
					tag.NewStringTag("current-version", test.current),
					tag.NewStringTag("recommended-version", test.recommended),
					tag.NewTimeTag("release-time", releaseTime),
					tag.NewStringTag("release-notes", "release notes"),
					tag.NewStringTag("upgrade-instructions", "upgrade instructions"),
				)
			}
			checker := &VersionChecker{logger: logger}
			checker.logVersionInfo(&versioninfo.VersionCheckResponse{
				Products: []versioninfo.ProductVersionReport{{
					Product: test.product,
					Current: versioninfo.ReleaseInfo{Version: test.current},
					Recommended: versioninfo.ReleaseInfo{
						Version: test.recommended, ReleaseTime: releaseTime.UnixNano(), Notes: "release notes",
					},
					Instructions: "upgrade instructions",
				}},
			})
		})
	}
}

func TestVersionCheckerLogUpgradeWithAlerts(t *testing.T) {
	logger := log.NewMockLogger(gomock.NewController(t))
	alerts := []versioninfo.Alert{
		{Message: "An update is available", Severity: versioninfo.SeverityLow},
		{Message: "Upgrade to address an important issue", Severity: versioninfo.SeverityHigh},
	}
	releaseTime := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	logger.EXPECT().Warn("Upgrade to a new version is recommended",
		tag.NewStringTag("product", headers.ClientNameServer),
		tag.NewStringTag("current-version", "1.0.0"),
		tag.NewStringTag("recommended-version", "1.1.0"),
		tag.NewTimeTag("release-time", releaseTime),
		tag.NewStringTag("release-notes", "release notes"),
		tag.NewStringTag("upgrade-instructions", "upgrade instructions"),
		tag.NewAnyTag("alerts", alerts),
	)
	checker := &VersionChecker{logger: logger}
	checker.logVersionInfo(&versioninfo.VersionCheckResponse{
		Products: []versioninfo.ProductVersionReport{{
			Product: headers.ClientNameServer,
			Current: versioninfo.ReleaseInfo{Version: "1.0.0"},
			Recommended: versioninfo.ReleaseInfo{
				Version: "1.1.0", ReleaseTime: releaseTime.UnixNano(), Notes: "release notes",
			},
			Instructions: "upgrade instructions",
			Alerts:       alerts,
		}},
	})
}

func TestVersionCheckerLogAlerts(t *testing.T) {
	for _, test := range []struct {
		name     string
		severity versioninfo.Severity
		warn     bool
	}{
		{name: "high", severity: versioninfo.SeverityHigh, warn: true},
		{name: "medium", severity: versioninfo.SeverityMedium, warn: true},
		{name: "low", severity: versioninfo.SeverityLow},
		{name: "unspecified", severity: versioninfo.SeverityUnspecified},
		{name: "unknown", severity: versioninfo.Severity(99)},
	} {
		t.Run(test.name, func(t *testing.T) {
			logger := log.NewMockLogger(gomock.NewController(t))
			for _, product := range []string{headers.ClientNameServer, headers.ClientNameGoSDK} {
				tags := []any{
					tag.NewStringTag("product", product),
					tag.NewStringTag("current-version", "invalid"),
					tag.NewAnyTag("alerts", []versioninfo.Alert{
						{Message: "test alert", Severity: test.severity},
						{Message: "test alert", Severity: test.severity},
					}),
				}
				if test.warn {
					logger.EXPECT().Warn("Version check reported issues; review the alerts", tags...)
				} else {
					logger.EXPECT().Info("Version check reported issues; review the alerts", tags...)
				}
			}
			checker := &VersionChecker{logger: logger}
			response := &versioninfo.VersionCheckResponse{}
			for _, product := range []string{headers.ClientNameServer, headers.ClientNameGoSDK} {
				response.Products = append(response.Products, versioninfo.ProductVersionReport{
					Product: product,
					Current: versioninfo.ReleaseInfo{Version: "invalid"},
					Alerts: []versioninfo.Alert{
						{Message: "test alert", Severity: test.severity},
						{Message: "test alert", Severity: test.severity},
					},
				})
			}
			checker.logVersionInfo(response)
		})
	}
}

func TestIsUpdateNeeded(t *testing.T) {
	tests := []struct {
		name     string
		metadata *persistence.GetClusterMetadataResponse
		want     bool
	}{
		{
			name: "nil VersionInfo returns true",
			metadata: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					VersionInfo: nil,
				},
			},
			want: true,
		},
		{
			name: "nil Current returns true",
			metadata: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					VersionInfo: &versionpb.VersionInfo{
						Current:        nil,
						LastUpdateTime: timestamppb.New(time.Now()),
					},
				},
			},
			want: true,
		},
		{
			name: "different server version returns true",
			metadata: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					VersionInfo: &versionpb.VersionInfo{
						Current: &versionpb.ReleaseInfo{
							Version: "0.0.0-old-version",
						},
						LastUpdateTime: timestamppb.New(time.Now()),
					},
				},
			},
			want: true,
		},
		{
			name: "same version with recent LastUpdateTime returns false",
			metadata: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					VersionInfo: &versionpb.VersionInfo{
						Current: &versionpb.ReleaseInfo{
							Version: headers.ServerVersion,
						},
						LastUpdateTime: timestamppb.New(time.Now()),
					},
				},
			},
			want: false,
		},
		{
			name: "same version with old LastUpdateTime returns true",
			metadata: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					VersionInfo: &versionpb.VersionInfo{
						Current: &versionpb.ReleaseInfo{
							Version: headers.ServerVersion,
						},
						LastUpdateTime: timestamppb.New(time.Now().Add(-2 * time.Hour)),
					},
				},
			},
			want: true,
		},
		{
			name: "same version with nil LastUpdateTime returns false",
			metadata: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					VersionInfo: &versionpb.VersionInfo{
						Current: &versionpb.ReleaseInfo{
							Version: headers.ServerVersion,
						},
						LastUpdateTime: nil,
					},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isUpdateNeeded(tt.metadata)
			require.Equal(t, tt.want, got)
		})
	}
}
