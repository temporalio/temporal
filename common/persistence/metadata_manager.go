package persistence

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (

	// metadataManagerImpl implements MetadataManager based on MetadataStore and Serializer
	metadataManagerImpl struct {
		serializer  serialization.Serializer
		persistence MetadataStore
		logger      log.Logger
		clusterName string
	}
)

var _ MetadataManager = (*metadataManagerImpl)(nil)

// NewMetadataManagerImpl returns new MetadataManager
func NewMetadataManagerImpl(
	persistence MetadataStore,
	serializer serialization.Serializer,
	logger log.Logger,
	clusterName string,
) MetadataManager {
	return &metadataManagerImpl{
		serializer:  serializer,
		persistence: persistence,
		logger:      logger,
		clusterName: clusterName,
	}
}

func (m *metadataManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *metadataManagerImpl) CreateNamespace(
	ctx context.Context,
	request *CreateNamespaceRequest,
) (*CreateNamespaceResponse, error) {
	datablob, err := m.serializer.NamespaceDetailToBlob(request.Namespace)
	if err != nil {
		return nil, err
	}

	return m.persistence.CreateNamespace(ctx, &InternalCreateNamespaceRequest{
		ID:        request.Namespace.Info.Id,
		Name:      request.Namespace.Info.Name,
		IsGlobal:  request.IsGlobalNamespace,
		Namespace: datablob,
	})
}

func (m *metadataManagerImpl) GetNamespace(
	ctx context.Context,
	request *GetNamespaceRequest,
) (*GetNamespaceResponse, error) {
	resp, err := m.persistence.GetNamespace(ctx, request)
	if err != nil {
		return nil, err
	}
	return ConvertInternalGetNamespaceResponse(m.serializer, m.clusterName, resp)
}

func (m *metadataManagerImpl) UpdateNamespace(
	ctx context.Context,
	request *UpdateNamespaceRequest,
) error {
	datablob, err := m.serializer.NamespaceDetailToBlob(request.Namespace)
	if err != nil {
		return err
	}

	return m.persistence.UpdateNamespace(ctx, &InternalUpdateNamespaceRequest{
		Id:                  request.Namespace.Info.Id,
		Name:                request.Namespace.Info.Name,
		Namespace:           datablob,
		NotificationVersion: request.NotificationVersion,
		IsGlobal:            request.IsGlobalNamespace,
	})
}

func (m *metadataManagerImpl) RenameNamespace(
	ctx context.Context,
	request *RenameNamespaceRequest,
) error {
	ns, err := m.GetNamespace(ctx, &GetNamespaceRequest{
		Name: request.PreviousName,
	})
	if err != nil {
		return err
	}

	metadata, err := m.GetMetadata(ctx)
	if err != nil {
		return err
	}

	previousName := ns.Namespace.Info.Name
	ns.Namespace.Info.Name = request.NewName

	nsDataBlob, err := m.serializer.NamespaceDetailToBlob(ns.Namespace)
	if err != nil {
		return err
	}

	renameRequest := &InternalRenameNamespaceRequest{
		InternalUpdateNamespaceRequest: &InternalUpdateNamespaceRequest{
			Id:                  ns.Namespace.Info.Id,
			Name:                ns.Namespace.Info.Name,
			Namespace:           nsDataBlob,
			NotificationVersion: metadata.NotificationVersion,
			IsGlobal:            ns.IsGlobalNamespace,
		},
		PreviousName: previousName,
	}

	return m.persistence.RenameNamespace(ctx, renameRequest)
}

func (m *metadataManagerImpl) DeleteNamespace(
	ctx context.Context,
	request *DeleteNamespaceRequest,
) error {
	return m.persistence.DeleteNamespace(ctx, request)
}

func (m *metadataManagerImpl) DeleteNamespaceByName(
	ctx context.Context,
	request *DeleteNamespaceByNameRequest,
) error {
	return m.persistence.DeleteNamespaceByName(ctx, request)
}

func ConvertInternalGetNamespaceResponse(serializer serialization.Serializer, currentClusterName string, d *InternalGetNamespaceResponse) (*GetNamespaceResponse, error) {
	ns, err := serializer.NamespaceDetailFromBlob(d.Namespace)
	if err != nil {
		return nil, err
	}

	if ns.Info.Data == nil {
		ns.Info.Data = map[string]string{}
	}

	if ns.Config.BadBinaries == nil || ns.Config.BadBinaries.Binaries == nil {
		ns.Config.BadBinaries = &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}}
	}

	ns.ReplicationConfig.ActiveClusterName = GetOrUseDefaultActiveCluster(currentClusterName, ns.ReplicationConfig.ActiveClusterName)
	ns.ReplicationConfig.Clusters = GetOrUseDefaultClusters(currentClusterName, ns.ReplicationConfig.Clusters)
	return &GetNamespaceResponse{
		Namespace:           ns,
		IsGlobalNamespace:   d.IsGlobal,
		NotificationVersion: d.NotificationVersion,
	}, nil
}

func (m *metadataManagerImpl) ListNamespaces(
	ctx context.Context,
	request *ListNamespacesRequest,
) (*ListNamespacesResponse, error) {
	var namespaces []*GetNamespaceResponse
	nextPageToken := request.NextPageToken
	pageSize := request.PageSize

	for {
		resp, err := m.persistence.ListNamespaces(ctx, &InternalListNamespacesRequest{
			PageSize:      pageSize,
			NextPageToken: nextPageToken,
		})
		if err != nil {
			return nil, err
		}
		deletedNamespacesCount := 0
		for _, d := range resp.Namespaces {
			ret, err := ConvertInternalGetNamespaceResponse(m.serializer, m.clusterName, d)
			if err != nil {
				return nil, err
			}
			if ret.Namespace.Info.State == enumspb.NAMESPACE_STATE_DELETED && !request.IncludeDeleted {
				deletedNamespacesCount++
				continue
			}
			namespaces = append(namespaces, ret)
		}
		nextPageToken = resp.NextPageToken
		if len(nextPageToken) == 0 {
			// Page wasn't full, no more namespaces in DB.
			break
		}
		if deletedNamespacesCount == 0 {
			break
		}
		// Page was full but few namespaces weren't added. Read number of deleted namespaces for DB again.
		pageSize = deletedNamespacesCount
	}

	return &ListNamespacesResponse{
		Namespaces:    namespaces,
		NextPageToken: nextPageToken,
	}, nil
}

func (m *metadataManagerImpl) InitializeSystemNamespaces(
	ctx context.Context,
	currentClusterName string,
) error {
	_, err := m.CreateNamespace(ctx, &CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          primitives.SystemNamespaceID,
				Name:        primitives.SystemLocalNamespace,
				State:       enumspb.NAMESPACE_STATE_REGISTERED,
				Description: "Temporal internal system namespace",
				Owner:       "temporal-core@temporal.io",
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               durationpb.New(primitives.SystemNamespaceRetention),
				HistoryArchivalState:    enumspb.ARCHIVAL_STATE_DISABLED,
				VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: currentClusterName,
				Clusters:          []string{currentClusterName},
			},
			FailoverVersion:             common.EmptyVersion,
			FailoverNotificationVersion: -1,
		},
		IsGlobalNamespace: false,
	})

	if err != nil {
		if _, ok := err.(*serviceerror.NamespaceAlreadyExists); !ok {
			return err
		}
	}
	return nil
}

func (m *metadataManagerImpl) GetMetadata(
	ctx context.Context,
) (*GetMetadataResponse, error) {
	return m.persistence.GetMetadata(ctx)
}

func (m *metadataManagerImpl) Close() {
	m.persistence.Close()
}
