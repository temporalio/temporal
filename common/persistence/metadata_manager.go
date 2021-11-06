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

package persistence

import (
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
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

//NewMetadataManagerImpl returns new MetadataManager
func NewMetadataManagerImpl(persistence MetadataStore, logger log.Logger, clusterName string) MetadataManager {
	return &metadataManagerImpl{
		serializer:  serialization.NewSerializer(),
		persistence: persistence,
		logger:      logger,
		clusterName: clusterName,
	}
}

func (m *metadataManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *metadataManagerImpl) CreateNamespace(request *CreateNamespaceRequest) (*CreateNamespaceResponse, error) {
	datablob, err := m.serializer.NamespaceDetailToBlob(request.Namespace, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	return m.persistence.CreateNamespace(&InternalCreateNamespaceRequest{
		ID:        request.Namespace.Info.Id,
		Name:      request.Namespace.Info.Name,
		IsGlobal:  request.IsGlobalNamespace,
		Namespace: datablob,
	})
}

func (m *metadataManagerImpl) GetNamespace(request *GetNamespaceRequest) (*GetNamespaceResponse, error) {
	resp, err := m.persistence.GetNamespace(request)
	if err != nil {
		return nil, err
	}
	return m.ConvertInternalGetResponse(resp)
}

func (m *metadataManagerImpl) UpdateNamespace(request *UpdateNamespaceRequest) error {
	datablob, err := m.serializer.NamespaceDetailToBlob(request.Namespace, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return err
	}

	return m.persistence.UpdateNamespace(&InternalUpdateNamespaceRequest{
		Id:                  request.Namespace.Info.Id,
		Name:                request.Namespace.Info.Name,
		Namespace:           datablob,
		NotificationVersion: request.NotificationVersion,
		IsGlobal:            request.IsGlobalNamespace,
	})
}

func (m *metadataManagerImpl) DeleteNamespace(request *DeleteNamespaceRequest) error {
	return m.persistence.DeleteNamespace(request)
}

func (m *metadataManagerImpl) DeleteNamespaceByName(request *DeleteNamespaceByNameRequest) error {
	return m.persistence.DeleteNamespaceByName(request)
}

func (m *metadataManagerImpl) ConvertInternalGetResponse(d *InternalGetNamespaceResponse) (*GetNamespaceResponse, error) {
	ns, err := m.serializer.NamespaceDetailFromBlob(d.Namespace)
	if err != nil {
		return nil, err
	}

	if ns.Info.Data == nil {
		ns.Info.Data = map[string]string{}
	}

	if ns.Config.BadBinaries == nil || ns.Config.BadBinaries.Binaries == nil {
		ns.Config.BadBinaries = &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}}
	}

	ns.ReplicationConfig.ActiveClusterName = GetOrUseDefaultActiveCluster(m.clusterName, ns.ReplicationConfig.ActiveClusterName)
	ns.ReplicationConfig.Clusters = GetOrUseDefaultClusters(m.clusterName, ns.ReplicationConfig.Clusters)
	return &GetNamespaceResponse{
		Namespace:           ns,
		IsGlobalNamespace:   d.IsGlobal,
		NotificationVersion: d.NotificationVersion,
	}, nil
}

func (m *metadataManagerImpl) ListNamespaces(request *ListNamespacesRequest) (*ListNamespacesResponse, error) {
	resp, err := m.persistence.ListNamespaces(request)
	if err != nil {
		return nil, err
	}
	namespaces := make([]*GetNamespaceResponse, 0, len(resp.Namespaces))
	for _, d := range resp.Namespaces {
		ret, err := m.ConvertInternalGetResponse(d)
		if err != nil {
			return nil, err
		}
		namespaces = append(namespaces, ret)
	}
	return &ListNamespacesResponse{
		Namespaces:    namespaces,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (m *metadataManagerImpl) InitializeSystemNamespaces(currentClusterName string) error {
	_, err := m.CreateNamespace(&CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          common.SystemNamespaceID,
				Name:        common.SystemLocalNamespace,
				State:       enumspb.NAMESPACE_STATE_REGISTERED,
				Description: "Temporal internal system namespace",
				Owner:       "temporal-core@temporal.io",
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               timestamp.DurationPtr(common.SystemNamespaceRetention),
				HistoryArchivalState:    enumspb.ARCHIVAL_STATE_DISABLED,
				VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: currentClusterName,
				Clusters:          GetOrUseDefaultClusters(currentClusterName, nil),
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

func (m *metadataManagerImpl) GetMetadata() (*GetMetadataResponse, error) {
	return m.persistence.GetMetadata()
}

func (m *metadataManagerImpl) Close() {
	m.persistence.Close()
}
