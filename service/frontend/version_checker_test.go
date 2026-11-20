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
	"testing/synctest"
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
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/versioninfo"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestVersionCheckerSchedule(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "metadata read succeeds"},
		{name: "metadata failure", err: errors.New("database unavailable")},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				expected := []time.Duration{
					0, 5 * time.Minute, 30 * time.Minute, time.Hour, 4 * time.Hour,
					24 * time.Hour, 48 * time.Hour, 72 * time.Hour, 96 * time.Hour,
				}
				startedAt := time.Now()
				var checks []time.Duration
				manager := persistence.NewMockClusterMetadataManager(gomock.NewController(t))
				manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).DoAndReturn(
					func(context.Context) (*persistence.GetClusterMetadataResponse, error) {
						checks = append(checks, time.Since(startedAt))
						return &persistence.GetClusterMetadataResponse{
							ClusterMetadata: &persistencespb.ClusterMetadata{ClusterId: "test-cluster"},
						}, test.err
					},
				).Times(len(expected))
				requests := 0
				if test.err == nil {
					requests = len(expected)
				}
				manager.EXPECT().GetName().Return("test-db").Times(requests)
				handler := metricstest.NewCaptureHandler()
				capture := handler.StartCapture()
				defer handler.StopCapture(capture)
				checker := NewVersionChecker(&Config{
					EnableServerVersionCheck:   dynamicconfig.GetBoolPropertyFn(true),
					ServerVersionCheckSchedule: defaultVersionCheckSchedule(),
				}, handler, manager, interceptor.NewSDKVersionInterceptor(), log.NewNoopLogger())
				checker.versionInfoCaller = versioninfo.Caller{Scheme: "unsupported", Host: "version-info.invalid"}
				defer checker.Stop()
				checker.Start()
				checker.Start()
				for i, checkAt := range expected {
					if i > 0 {
						time.Sleep(checkAt - time.Since(startedAt) - time.Nanosecond) //nolint:forbidigo // Advances synctest's virtual clock.
						synctest.Wait()
						require.Equal(t, expected[:i], checks)
						time.Sleep(time.Nanosecond) //nolint:forbidigo // Advances synctest's virtual clock.
					}
					synctest.Wait()
					require.Equal(t, expected[:i+1], checks)
				}
				checker.Stop()
				checker.Stop()
				checker.Start()
				time.Sleep(48 * time.Hour) //nolint:forbidigo // Advances synctest's virtual clock.
				synctest.Wait()
				require.Equal(t, expected, checks)
				require.Len(t, capture.Snapshot()[metrics.VersionCheckRequestFailedCount.Name()], requests)
			})
		})
	}
}

func TestVersionCheckerTimeoutAllowsLaterChecks(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		startedAt := time.Now()
		var contexts []context.Context
		manager := persistence.NewMockClusterMetadataManager(gomock.NewController(t))
		manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).DoAndReturn(
			func(ctx context.Context) (*persistence.GetClusterMetadataResponse, error) {
				contexts = append(contexts, ctx)
				if len(contexts) == 1 {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return &persistence.GetClusterMetadataResponse{
					ClusterMetadata: &persistencespb.ClusterMetadata{
						ClusterId: "test-cluster",
						VersionInfo: &versionpb.VersionInfo{
							Current:        &versionpb.ReleaseInfo{Version: headers.ServerVersion},
							LastUpdateTime: timestamppb.Now(),
						},
					},
				}, nil
			},
		).Times(2)
		manager.EXPECT().GetName().Return("test-db")
		handler := metricstest.NewCaptureHandler()
		capture := handler.StartCapture()
		defer handler.StopCapture(capture)
		checker := NewVersionChecker(&Config{
			EnableServerVersionCheck:   dynamicconfig.GetBoolPropertyFn(true),
			ServerVersionCheckSchedule: defaultVersionCheckSchedule(),
		}, handler, manager, interceptor.NewSDKVersionInterceptor(), log.NewNoopLogger())
		checker.versionInfoCaller = versioninfo.Caller{Scheme: "unsupported", Host: "version-info.invalid"}
		defer checker.Stop()
		checker.Start()
		synctest.Wait()
		require.Len(t, contexts, 1)
		deadline, ok := contexts[0].Deadline()
		require.True(t, ok)
		require.Equal(t, startedAt.Add(versionCheckTimeout), deadline)
		time.Sleep(versionCheckTimeout - time.Nanosecond) //nolint:forbidigo // Advances synctest's virtual clock.
		synctest.Wait()
		require.NoError(t, contexts[0].Err())
		time.Sleep(time.Nanosecond) //nolint:forbidigo // Advances synctest's virtual clock.
		synctest.Wait()
		require.ErrorIs(t, contexts[0].Err(), context.DeadlineExceeded)
		require.Len(t, capture.Snapshot()[metrics.VersionCheckFailedCount.Name()], 1)
		time.Sleep(5*time.Minute - time.Since(startedAt)) //nolint:forbidigo // Advances synctest's virtual clock.
		synctest.Wait()
		require.Len(t, contexts, 2)
		deadline, ok = contexts[1].Deadline()
		require.True(t, ok)
		require.Equal(t, startedAt.Add(5*time.Minute+versionCheckTimeout), deadline)
		require.Len(t, capture.Snapshot()[metrics.VersionCheckLatency.Name()], 1)
		require.Len(t, capture.Snapshot()[metrics.VersionCheckFailedCount.Name()], 2)
		require.Len(t, capture.Snapshot()[metrics.VersionCheckRequestFailedCount.Name()], 1)
	})
}

func TestVersionCheckerDynamicEnable(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var enabled atomic.Bool
		var checks []time.Duration
		startedAt := time.Now()
		manager := persistence.NewMockClusterMetadataManager(gomock.NewController(t))
		manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).DoAndReturn(
			func(context.Context) (*persistence.GetClusterMetadataResponse, error) {
				checks = append(checks, time.Since(startedAt))
				return nil, errors.New("database unavailable")
			},
		).Times(2)
		checker := NewVersionChecker(&Config{
			EnableServerVersionCheck:   enabled.Load,
			ServerVersionCheckSchedule: defaultVersionCheckSchedule(),
		}, metrics.NoopMetricsHandler, manager, interceptor.NewSDKVersionInterceptor(), log.NewNoopLogger())
		defer checker.Stop()
		checker.Start()
		synctest.Wait()
		require.Empty(t, checks)
		enabled.Store(true)
		time.Sleep(5 * time.Minute) //nolint:forbidigo // Advances synctest's virtual clock.
		synctest.Wait()
		require.Equal(t, []time.Duration{5 * time.Minute}, checks)
		enabled.Store(false)
		time.Sleep(25 * time.Minute) //nolint:forbidigo // Advances synctest's virtual clock.
		synctest.Wait()
		require.Equal(t, []time.Duration{5 * time.Minute}, checks)
		enabled.Store(true)
		time.Sleep(30 * time.Minute) //nolint:forbidigo // Advances synctest's virtual clock.
		synctest.Wait()
		require.Equal(t, []time.Duration{5 * time.Minute, time.Hour}, checks)
	})
}

func TestVersionCheckerPerformVersionCheck(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name           string
		status         int
		disableLogging bool
	}{
		{name: "success", status: http.StatusOK},
		{name: "logging disabled", status: http.StatusOK, disableLogging: true},
		{name: "request failure", status: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			product := versioninfo.ProductVersionReport{
				Product:     headers.ClientNameServer,
				Current:     versioninfo.ReleaseInfo{Version: headers.ServerVersion},
				Recommended: versioninfo.ReleaseInfo{Version: headers.ServerVersion},
				Alerts:      []versioninfo.Alert{{Message: "test alert", Severity: versioninfo.SeverityLow}},
			}
			var requests atomic.Int32
			reported := make(chan versioninfo.VersionCheckRequest, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				var request versioninfo.VersionCheckRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				await.Snd(t, reported, request)
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
			reads := 1
			if test.status == http.StatusOK {
				reads++
				save := manager.EXPECT().SaveClusterMetadata(gomock.Any(), gomock.Any()).Return(true, nil)
				if !test.disableLogging {
					gomock.InOrder(
						logger.EXPECT().Info("Version check reported issues; review the alerts",
							tag.NewStringTag("product", product.Product),
							tag.NewStringTag("current-version", product.Current.Version),
							tag.NewAnyTag("alerts", product.Alerts),
						),
						save,
					)
				}
			}
			manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{ClusterId: "test-cluster"},
			}, nil).Times(reads)
			manager.EXPECT().GetName().Return("test-db")
			recorder := interceptor.NewSDKVersionInterceptor()
			recorder.RecordSDKInfo("temporal-go", "1.0.0")
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)
			checker := NewVersionChecker(&Config{
				EnableServerVersionCheck:        dynamicconfig.GetBoolPropertyFn(true),
				EnableServerVersionCheckLogging: dynamicconfig.GetBoolPropertyFn(!test.disableLogging),
			}, handler, manager, recorder, logger)
			checker.versionInfoCaller = versioninfo.Caller{Scheme: endpoint.Scheme, Host: endpoint.Host}
			defer checker.Stop()
			checker.performVersionCheck(t.Context())
			checker.Stop()
			require.EqualValues(t, 1, requests.Load())
			request := await.Rcv(t, reported)
			require.Equal(t, "test-cluster", request.ClusterID)
			require.Equal(t, []versioninfo.SDKInfo{{Name: "temporal-go", Version: "1.0.0"}}, request.SDKInfo)
			snapshot := capture.Snapshot()
			require.Len(t, snapshot[metrics.VersionCheckLatency.Name()], 1)
			if test.status == http.StatusOK {
				require.Len(t, snapshot[metrics.VersionCheckSuccessCount.Name()], 1)
				require.Empty(t, snapshot[metrics.VersionCheckFailedCount.Name()])
			} else {
				require.Empty(t, snapshot[metrics.VersionCheckSuccessCount.Name()])
				require.Len(t, snapshot[metrics.VersionCheckFailedCount.Name()], 1)
				require.Len(t, snapshot[metrics.VersionCheckRequestFailedCount.Name()], 1)
			}
		})
	}
}

func TestVersionCheckerStopCancelsInFlightCheck(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"metadata read", "HTTP request", "metadata save"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			blocked := make(chan context.Context, 1)
			alerts := []versioninfo.Alert{{Message: "test alert", Severity: versioninfo.SeverityLow}}
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					t.Error(err)
					return
				}
				if stage == "HTTP request" {
					await.Snd(t, blocked, r.Context())
					<-r.Context().Done()
					return
				}
				err := json.NewEncoder(w).Encode(versioninfo.VersionCheckResponse{
					Products: []versioninfo.ProductVersionReport{{
						Product:     headers.ClientNameServer,
						Current:     versioninfo.ReleaseInfo{Version: headers.ServerVersion},
						Recommended: versioninfo.ReleaseInfo{Version: headers.ServerVersion},
						Alerts:      alerts,
					}},
				})
				if err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			endpoint, err := url.Parse(server.URL)
			require.NoError(t, err)
			controller := gomock.NewController(t)
			manager := persistence.NewMockClusterMetadataManager(controller)
			logger := log.NewMockLogger(controller)
			metadata := &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{ClusterId: "test-cluster"},
			}
			switch stage {
			case "metadata read":
				manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).DoAndReturn(
					func(ctx context.Context) (*persistence.GetClusterMetadataResponse, error) {
						await.Snd(t, blocked, ctx)
						<-ctx.Done()
						return nil, ctx.Err()
					},
				)
			case "HTTP request":
				manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(metadata, nil)
				manager.EXPECT().GetName().Return("test-db")
			case "metadata save":
				manager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(metadata, nil).Times(2)
				manager.EXPECT().GetName().Return("test-db")
				logger.EXPECT().Info("Version check reported issues; review the alerts",
					tag.NewStringTag("product", headers.ClientNameServer),
					tag.NewStringTag("current-version", headers.ServerVersion),
					tag.NewAnyTag("alerts", alerts),
				)
				manager.EXPECT().SaveClusterMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, _ *persistence.SaveClusterMetadataRequest) (bool, error) {
						await.Snd(t, blocked, ctx)
						<-ctx.Done()
						return false, ctx.Err()
					},
				)
			default:
				t.Fatalf("unknown stage: %s", stage)
			}
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)
			checker := NewVersionChecker(&Config{
				EnableServerVersionCheck:        dynamicconfig.GetBoolPropertyFn(true),
				EnableServerVersionCheckLogging: dynamicconfig.GetBoolPropertyFn(true),
				ServerVersionCheckSchedule:      defaultVersionCheckSchedule(),
			}, handler, manager, interceptor.NewSDKVersionInterceptor(), logger)
			checker.versionInfoCaller = versioninfo.Caller{Scheme: endpoint.Scheme, Host: endpoint.Host}
			defer checker.Stop()
			checker.Start()
			ctx := await.Rcv(t, blocked)
			done := make(chan struct{})
			go func() {
				checker.Stop()
				close(done)
			}()
			await.Rcv(t, done)
			await.Rcv(t, ctx.Done())
			checker.Stop()
			checker.Start()
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			if stage == "metadata read" {
				require.Zero(t, requests.Load())
			} else {
				require.EqualValues(t, 1, requests.Load())
			}
			snapshot := capture.Snapshot()
			require.Len(t, snapshot[metrics.VersionCheckFailedCount.Name()], 1)
			require.Empty(t, snapshot[metrics.VersionCheckSuccessCount.Name()])
			if stage == "HTTP request" {
				require.Len(t, snapshot[metrics.VersionCheckRequestFailedCount.Name()], 1)
			} else {
				require.Empty(t, snapshot[metrics.VersionCheckRequestFailedCount.Name()])
			}
			if stage == "metadata read" {
				require.Empty(t, snapshot[metrics.VersionCheckLatency.Name()])
			} else {
				require.Len(t, snapshot[metrics.VersionCheckLatency.Name()], 1)
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

func TestVersionCheckerSkipsAlreadyCanceledCheck(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	endpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	manager := persistence.NewMockClusterMetadataManager(gomock.NewController(t))

	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	checker := NewVersionChecker(&Config{
		EnableServerVersionCheck: dynamicconfig.GetBoolPropertyFn(true),
	}, handler, manager, interceptor.NewSDKVersionInterceptor(), log.NewNoopLogger())
	checker.versionInfoCaller = versioninfo.Caller{Scheme: endpoint.Scheme, Host: endpoint.Host}
	defer checker.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	checker.performVersionCheck(ctx)
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
		ClusterMetadata: &persistencespb.ClusterMetadata{ClusterId: "test-cluster"},
	}, nil)
	manager.EXPECT().GetName().Return("test-db")
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	checker := NewVersionChecker(&Config{EnableServerVersionCheck: enabled.Load, EnableServerVersionCheckLogging: dynamicconfig.GetBoolPropertyFn(true)}, handler,
		manager, interceptor.NewSDKVersionInterceptor(), log.NewNoopLogger())
	checker.versionInfoCaller = versioninfo.Caller{Scheme: "unsupported", Host: "version-info.invalid"}
	checker.performVersionCheck(context.Background())
	enabled.Store(false)
	checker.performVersionCheck(context.Background())
	recordings := capture.Snapshot()[metrics.VersionCheckLatency.Name()]
	require.Len(t, recordings, 1)
}

func TestVersionCheckerSaveVersionInfo(t *testing.T) {
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
				err := json.NewEncoder(w).Encode(versioninfo.VersionCheckResponse{
					Products: []versioninfo.ProductVersionReport{{
						Product:     headers.ClientNameServer,
						Current:     versioninfo.ReleaseInfo{Version: "1.0.0"},
						Recommended: versioninfo.ReleaseInfo{Version: "1.0.0"},
						Alerts:      alerts,
					}},
				})
				if err != nil {
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
			checker := NewVersionChecker(&Config{EnableServerVersionCheck: dynamicconfig.GetBoolPropertyFn(true), EnableServerVersionCheckLogging: dynamicconfig.GetBoolPropertyFn(true)},
				handler, manager, interceptor.NewSDKVersionInterceptor(), logger)
			checker.versionInfoCaller = versioninfo.Caller{Scheme: endpoint.Scheme, Host: endpoint.Host}
			defer checker.Stop()
			checker.performVersionCheck(context.Background())
			snapshot := capture.Snapshot()
			require.Empty(t, snapshot[metrics.VersionCheckSuccessCount.Name()])
			if test.err != nil {
				require.Len(t, snapshot[metrics.VersionCheckFailedCount.Name()], 1)
			} else {
				require.Empty(t, snapshot[metrics.VersionCheckFailedCount.Name()])
			}
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
			checker := &VersionChecker{logger: logger, config: &Config{EnableServerVersionCheckLogging: dynamicconfig.GetBoolPropertyFn(true)}}
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
	checker := &VersionChecker{logger: logger, config: &Config{EnableServerVersionCheckLogging: dynamicconfig.GetBoolPropertyFn(true)}}
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
			checker := &VersionChecker{logger: logger, config: &Config{EnableServerVersionCheckLogging: dynamicconfig.GetBoolPropertyFn(true)}}
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

func defaultVersionCheckSchedule() dynamicconfig.TypedPropertyFn[[]time.Duration] {
	return dynamicconfig.ServerVersionCheckSchedule.Get(dynamicconfig.NewNoopCollection())
}
