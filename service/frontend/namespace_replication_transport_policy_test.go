package frontend

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	otellog "go.opentelemetry.io/otel/log"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
)

// syncEventLogger captures events from the handler and from shadow goroutines.
type syncEventLogger struct {
	otellog.Logger
	mu      sync.Mutex
	records []otellog.Record
}

func (l *syncEventLogger) Emit(_ context.Context, record otellog.Record) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, record)
}

func (l *syncEventLogger) Enabled(context.Context, otellog.EnabledParameters) bool { return true }

// phaseDetails returns the details of the only event with the given phase.
func (l *syncEventLogger) phaseDetails(t *testing.T, phase string) map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	var found []map[string]any
	for _, record := range l.records {
		attrs := map[string]string{}
		record.WalkAttributes(func(kv otellog.KeyValue) bool {
			attrs[kv.Key] = kv.Value.AsString()
			return true
		})
		if attrs["phase"] != phase {
			continue
		}
		var details map[string]any
		require.NoError(t, json.Unmarshal([]byte(attrs["details"]), &details))
		found = append(found, details)
	}
	require.Len(t, found, 1)
	return found[0]
}

func TestNamespaceReplicationTransportPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		mode       string
		hold       bool
		noPolicy   bool
		members    string
		wantCalls  int32
		wantCHASM  bool
		wantShadow bool
		wantRoute  map[string]any
	}{
		{name: "chasm cell, not held", mode: dynamicconfig.NamespaceReplicationTransportModeCHASM, wantCalls: 1, wantCHASM: true,
			wantRoute: map[string]any{"transport": "chasm", "held_on_legacy": false, "peer_cells": []any{"cell-b"}}},
		{name: "chasm cell, held on legacy", mode: dynamicconfig.NamespaceReplicationTransportModeCHASM, hold: true, wantCalls: 1,
			wantRoute: map[string]any{"transport": "legacy", "held_on_legacy": true}},
		{name: "chasm cell without a policy", mode: dynamicconfig.NamespaceReplicationTransportModeCHASM, noPolicy: true, wantCHASM: true,
			wantRoute: map[string]any{"transport": "chasm", "held_on_legacy": false, "peer_cells": []any{"cell-b"}}},
		{name: "shadow cell, not held", mode: dynamicconfig.NamespaceReplicationTransportModeShadow, wantCalls: 1, wantShadow: true,
			wantRoute: map[string]any{"transport": "shadow", "held_on_legacy": false}},
		// A held namespace is not shadowed either: its peers may not be ready for CHASM deliveries.
		{name: "shadow cell, held on legacy", mode: dynamicconfig.NamespaceReplicationTransportModeShadow, hold: true, wantCalls: 1,
			wantRoute: map[string]any{"transport": "legacy", "held_on_legacy": true}},
		// The policy can only hold a namespace back, so a legacy cell never asks it.
		{name: "legacy cell", mode: dynamicconfig.NamespaceReplicationTransportModeLegacy, hold: true,
			wantRoute: map[string]any{"transport": "legacy", "held_on_legacy": false}},
		// Holding keeps the legacy path's existing publish decision, which reads
		// ReplicationConfig.Clusters (only cell-a here) rather than the resolver.
		// The legacy path does not replicate this write, so the event carries no
		// route.
		{name: "held, resolver members outside the cluster list", mode: dynamicconfig.NamespaceReplicationTransportModeCHASM,
			hold: true, members: "cell-a,cell-c", wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			metadata := persistence.NewMockMetadataManager(ctrl)
			clusters := cluster.NewMockMetadata(ctrl)
			queue := persistence.NewMockNamespaceReplicationQueue(ctrl)
			logger := log.NewNoopLogger()
			dc := dynamicconfig.NewNoopCollection()
			cfg := NewConfig(dc, 32)
			cfg.NamespaceReplicationTransportMode = dynamicconfig.GetStringPropertyFn(tc.mode)
			cfg.EmitNamespaceLifecycleEvents = dynamicconfig.GetBoolPropertyFn(true)
			events := &syncEventLogger{}
			client := &captureNamespaceReplicationClient{requests: make(chan *namespacereplicationpb.TriggerNamespaceMutationRequest, 1)}
			var calls atomic.Int32
			var policy nsreplication.TransportPolicy
			if !tc.noPolicy {
				policy = func(_ context.Context, detail *persistencespb.NamespaceDetail) bool {
					calls.Add(1)
					// The policy sees the snapshot the write replicates.
					require.Equal(t, "updated", detail.GetInfo().GetDescription())
					return tc.hold
				}
			}
			handler := newNamespaceHandler(logger, events, metadata, nil, clusters,
				nsreplication.NewReplicator(queue, logger, nil, dynamicconfig.GetBoolPropertyFn(false), "cell-a"),
				archiver.NewArchivalMetadata(dc, "", false, "", false, &config.ArchivalNamespaceDefaults{}),
				nil, clock.NewEventTimeSource(), cfg, client, metrics.NoopMetricsHandler, dataMembershipFactory(), policy)
			clusters.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
			clusters.EXPECT().IsMasterCluster().Return(true).AnyTimes()
			clusters.EXPECT().GetCurrentClusterName().Return("cell-a").AnyTimes()
			clusters.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
				"cell-a": {Enabled: true}, "cell-b": {Enabled: true},
			}).AnyTimes()
			info := &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "ns", State: enumspb.NAMESPACE_STATE_REGISTERED}
			clusterList := []string{"cell-a", "cell-b"}
			if tc.members != "" {
				info.Data = map[string]string{"members": tc.members}
				clusterList = []string{"cell-a"}
			}
			metadata.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 7}, nil)
			metadata.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
				Namespace: &persistencespb.NamespaceDetail{
					Info:              info,
					Config:            &persistencespb.NamespaceConfig{BadBinaries: &namespacepb.BadBinaries{}},
					ReplicationConfig: &persistencespb.NamespaceReplicationConfig{ActiveClusterName: "cell-a", Clusters: clusterList},
					FailoverVersion:   1,
				}, IsGlobalNamespace: true,
			}, nil)
			if !tc.wantCHASM {
				// The legacy path writes locally, and publishes when its cluster list replicates.
				metadata.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
				if tc.wantRoute != nil {
					queue.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)
				}
			}

			_, err := handler.UpdateNamespace(t.Context(), &workflowservice.UpdateNamespaceRequest{
				Namespace: "ns", UpdateInfo: &namespacepb.UpdateNamespaceInfo{Description: "updated"},
			})
			require.NoError(t, err)

			// Shadow triggers asynchronously, so wait briefly either way.
			select {
			case request := <-client.requests:
				require.True(t, tc.wantCHASM || tc.wantShadow, "unexpected CHASM trigger")
				require.Equal(t, tc.wantShadow, request.GetMutation().GetShadow())
			case <-time.After(2 * time.Second):
				require.False(t, tc.wantCHASM || tc.wantShadow, "expected a CHASM trigger")
			}
			require.Equal(t, tc.wantCalls, calls.Load())
			details := events.phaseDetails(t, wideevents.PhaseNamespaceUpdated)
			if tc.wantRoute == nil {
				require.NotContains(t, details, "replication")
				return
			}
			require.Equal(t, tc.wantRoute, details["replication"])
		})
	}
}

func TestNamespaceReplicationRoutes(t *testing.T) {
	t.Parallel()
	require.Nil(t, legacyReplicationRoute(namespaceReplicationTransportShadow, false, false),
		"a write the legacy path does not replicate has no route")
	// CHASM mode sends the writes CHASM does not replicate down the legacy path.
	require.Equal(t, &wideevents.NamespaceReplicationRoute{Transport: "legacy"},
		legacyReplicationRoute(namespaceReplicationTransportCHASM, false, true))
	require.Equal(t, &wideevents.NamespaceReplicationRoute{Transport: "shadow"},
		legacyReplicationRoute(namespaceReplicationTransportShadow, false, true))
	require.Equal(t, &wideevents.NamespaceReplicationRoute{Transport: "legacy", HeldOnLegacy: true},
		legacyReplicationRoute(namespaceReplicationTransportLegacy, true, true))
	require.Equal(t, &wideevents.NamespaceReplicationRoute{
		Transport: "chasm", PeerCells: []string{"cell-b", "cell-c"}, RemovedPeers: []string{"cell-c"},
	}, chasmReplicationRoute(&namespacereplicationpb.NamespaceMutation{
		PeerCells:    []string{"cell-b", "cell-c"},
		RemovedPeers: &namespacereplicationpb.RemovedPeers{Cells: []string{"cell-c"}},
	}))
}
