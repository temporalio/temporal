package nsregistry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
)

func TestRefreshNamespacesRevalidatesOmittedID(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		state           enumspb.NamespaceState
		err             error
		conversionError bool
		reuseOldName    bool
	}{
		{name: "renamed", state: enumspb.NAMESPACE_STATE_REGISTERED},
		{name: "old name reused", state: enumspb.NAMESPACE_STATE_REGISTERED, reuseOldName: true},
		{name: "deprecated", state: enumspb.NAMESPACE_STATE_DEPRECATED},
		{name: "soft deleted", state: enumspb.NAMESPACE_STATE_DELETED},
		{name: "physically deleted", err: serviceerror.NewNamespaceNotFound("omitted")},
		{name: "wrapped not found", err: fmt.Errorf("lookup: %w", serviceerror.NewNamespaceNotFound("omitted"))},
		{name: "unavailable", err: serviceerror.NewUnavailable("rename in progress")},
		{name: "cancelled", err: context.Canceled},
		{name: "conversion error", conversionError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := NewMockPersistence(gomock.NewController(t))
			reg := NewRegistry(
				store, true, "active", dynamicconfig.GetDurationPropertyFn(time.Hour),
				dynamicconfig.GetBoolPropertyFn(false), metrics.NoopMetricsHandler, log.NewNoopLogger(),
				namespace.NewDefaultReplicationResolverFactory(), DefaultNamespaceStateChanged,
			)
			omitted := &persistence.GetNamespaceResponse{
				Namespace: &persistencespb.NamespaceDetail{
					Info: &persistencespb.NamespaceInfo{
						Id: namespace.NewID().String(), Name: "z-namespace", State: enumspb.NAMESPACE_STATE_REGISTERED,
					},
					Config:            &persistencespb.NamespaceConfig{},
					ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
				},
				NotificationVersion: 1,
			}
			store.EXPECT().ListNamespaces(ctx, gomock.Any()).Return(&persistence.ListNamespacesResponse{
				Namespaces: []*persistence.GetNamespaceResponse{omitted},
			}, nil)
			require.NoError(t, reg.refreshNamespaces(ctx))
			oldEntries := reg.GetAllNamespaces()
			var changed []*namespace.Namespace
			var deleted []bool
			reg.RegisterStateChangeCallback("test", func(ns *namespace.Namespace, removed bool) {
				changed = append(changed, ns)
				deleted = append(deleted, removed)
			})
			changed, deleted = nil, nil // Discard registration's catch-up callback.
			var scanned []*persistence.GetNamespaceResponse
			var replacementID namespace.ID
			if tc.reuseOldName {
				replacementID = namespace.NewID()
				scanned = []*persistence.GetNamespaceResponse{{
					Namespace: &persistencespb.NamespaceDetail{
						Info: &persistencespb.NamespaceInfo{
							Id: replacementID.String(), Name: "z-namespace", State: enumspb.NAMESPACE_STATE_REGISTERED,
						},
						Config: &persistencespb.NamespaceConfig{}, ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
					},
					NotificationVersion: 8,
				}}
			}

			store.EXPECT().ListNamespaces(ctx, &persistence.ListNamespacesRequest{
				PageSize: CacheRefreshPageSize, IncludeDeleted: true,
			}).Return(&persistence.ListNamespacesResponse{NextPageToken: []byte("after-m")}, nil)
			store.EXPECT().ListNamespaces(ctx, &persistence.ListNamespacesRequest{
				PageSize: CacheRefreshPageSize, IncludeDeleted: true, NextPageToken: []byte("after-m"),
			}).Return(&persistence.ListNamespacesResponse{Namespaces: scanned}, nil)
			renamed := &persistence.GetNamespaceResponse{
				Namespace: &persistencespb.NamespaceDetail{
					Info: &persistencespb.NamespaceInfo{
						Id: omitted.Namespace.Info.Id, Name: "a-namespace", State: tc.state,
					},
					Config: omitted.Namespace.Config, ReplicationConfig: omitted.Namespace.ReplicationConfig,
				},
				IsGlobalNamespace: true, NotificationVersion: 7,
			}
			store.EXPECT().GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: omitted.Namespace.Info.Id}).
				Return(renamed, tc.err)
			if tc.conversionError {
				reg.replicationResolverFactory = func(*persistencespb.NamespaceDetail) namespace.ReplicationResolver {
					return nil
				}
			}
			var notFound *serviceerror.NamespaceNotFound
			lookupFailed := tc.err != nil && !errors.As(tc.err, &notFound)
			if lookupFailed || tc.conversionError {
				reg.stateChangedDuringReadthrough = oldEntries
			}

			err := reg.refreshNamespaces(ctx)
			if lookupFailed || tc.conversionError {
				if tc.conversionError {
					require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
				} else {
					require.ErrorIs(t, err, tc.err)
				}
				require.Equal(t, oldEntries, reg.GetAllNamespaces())
				require.Equal(t, oldEntries, reg.stateChangedDuringReadthrough)
				require.Empty(t, changed)
				return
			}
			require.NoError(t, err)
			if tc.reuseOldName {
				require.Len(t, changed, 2)
				require.Equal(t, []bool{false, false}, deleted)
				require.Len(t, reg.GetAllNamespaces(), 2)
				replacement, err := reg.getNamespace("z-namespace")
				require.NoError(t, err)
				require.Equal(t, replacementID, replacement.ID())
				renamed, err := reg.getNamespace("a-namespace")
				require.NoError(t, err)
				require.Equal(t, namespace.ID(omitted.Namespace.Info.Id), renamed.ID())
				return
			}
			require.Len(t, changed, 1)
			if tc.err != nil {
				require.Equal(t, []bool{true}, deleted)
				require.Equal(t, oldEntries[0], changed[0])
				require.Empty(t, reg.GetAllNamespaces())
				return
			}
			require.Equal(t, []bool{false}, deleted)
			require.Equal(t, namespace.Name("a-namespace"), changed[0].Name())
			require.Equal(t, tc.state, changed[0].State())
			require.True(t, changed[0].IsGlobalNamespace())
			require.Equal(t, int64(7), changed[0].NotificationVersion())
			require.Equal(t, changed, reg.GetAllNamespaces())
			_, err = reg.getNamespace("z-namespace")
			require.ErrorAs(t, err, new(*serviceerror.NamespaceNotFound))
			entry, err := reg.getNamespace("a-namespace")
			require.NoError(t, err)
			require.Equal(t, changed[0], entry)
		})
	}
}
