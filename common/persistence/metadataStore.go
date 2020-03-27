// Copyright (c) 2017 Uber Technologies, Inc.
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
	commonproto "go.temporal.io/temporal-proto/common"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
)

type (

	// metadataManagerImpl implements MetadataManager based on MetadataStore and PayloadSerializer
	metadataManagerImpl struct {
		serializer  PayloadSerializer
		persistence MetadataStore
		logger      log.Logger
	}
)

var _ MetadataManager = (*metadataManagerImpl)(nil)

//NewMetadataManagerImpl returns new MetadataManager
func NewMetadataManagerImpl(persistence MetadataStore, logger log.Logger) MetadataManager {
	return &metadataManagerImpl{
		serializer:  NewPayloadSerializer(),
		persistence: persistence,
		logger:      logger,
	}
}

func (m *metadataManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *metadataManagerImpl) CreateNamespace(request *CreateNamespaceRequest) (*CreateNamespaceResponse, error) {
	dc, err := m.serializeNamespaceConfig(request.Config)
	if err != nil {
		return nil, err
	}
	return m.persistence.CreateNamespace(&InternalCreateNamespaceRequest{
		Info:              request.Info,
		Config:            &dc,
		ReplicationConfig: request.ReplicationConfig,
		IsGlobalNamespace: request.IsGlobalNamespace,
		ConfigVersion:     request.ConfigVersion,
		FailoverVersion:   request.FailoverVersion,
	})
}

func (m *metadataManagerImpl) GetNamespace(request *GetNamespaceRequest) (*GetNamespaceResponse, error) {
	resp, err := m.persistence.GetNamespace(request)
	if err != nil {
		return nil, err
	}

	dc, err := m.deserializeNamespaceConfig(resp.Config)
	if err != nil {
		return nil, err
	}

	return &GetNamespaceResponse{
		Info:                        resp.Info,
		Config:                      &dc,
		ReplicationConfig:           resp.ReplicationConfig,
		IsGlobalNamespace:           resp.IsGlobalNamespace,
		ConfigVersion:               resp.ConfigVersion,
		FailoverVersion:             resp.FailoverVersion,
		FailoverNotificationVersion: resp.FailoverNotificationVersion,
		NotificationVersion:         resp.NotificationVersion,
	}, nil
}

func (m *metadataManagerImpl) UpdateNamespace(request *UpdateNamespaceRequest) error {
	dc, err := m.serializeNamespaceConfig(request.Config)
	if err != nil {
		return err
	}
	return m.persistence.UpdateNamespace(&InternalUpdateNamespaceRequest{
		Info:                        request.Info,
		Config:                      &dc,
		ReplicationConfig:           request.ReplicationConfig,
		ConfigVersion:               request.ConfigVersion,
		FailoverVersion:             request.FailoverVersion,
		FailoverNotificationVersion: request.FailoverNotificationVersion,
		NotificationVersion:         request.NotificationVersion,
	})
}

func (m *metadataManagerImpl) DeleteNamespace(request *DeleteNamespaceRequest) error {
	return m.persistence.DeleteNamespace(request)
}

func (m *metadataManagerImpl) DeleteNamespaceByName(request *DeleteNamespaceByNameRequest) error {
	return m.persistence.DeleteNamespaceByName(request)
}

func (m *metadataManagerImpl) ListNamespaces(request *ListNamespacesRequest) (*ListNamespacesResponse, error) {
	resp, err := m.persistence.ListNamespaces(request)
	if err != nil {
		return nil, err
	}
	namespaces := make([]*GetNamespaceResponse, 0, len(resp.Namespaces))
	for _, d := range resp.Namespaces {
		dc, err := m.deserializeNamespaceConfig(d.Config)
		if err != nil {
			return nil, err
		}
		namespaces = append(namespaces, &GetNamespaceResponse{
			Info:                        d.Info,
			Config:                      &dc,
			ReplicationConfig:           d.ReplicationConfig,
			IsGlobalNamespace:           d.IsGlobalNamespace,
			ConfigVersion:               d.ConfigVersion,
			FailoverVersion:             d.FailoverVersion,
			FailoverNotificationVersion: d.FailoverNotificationVersion,
			NotificationVersion:         d.NotificationVersion,
		})
	}
	return &ListNamespacesResponse{
		Namespaces:    namespaces,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (m *metadataManagerImpl) serializeNamespaceConfig(c *NamespaceConfig) (InternalNamespaceConfig, error) {
	if c == nil {
		return InternalNamespaceConfig{}, nil
	}
	if c.BadBinaries.Binaries == nil {
		c.BadBinaries.Binaries = map[string]*commonproto.BadBinaryInfo{}
	}
	badBinaries, err := m.serializer.SerializeBadBinaries(&c.BadBinaries, common.EncodingTypeThriftRW)
	if err != nil {
		return InternalNamespaceConfig{}, err
	}
	return InternalNamespaceConfig{
		Retention:                c.Retention,
		EmitMetric:               c.EmitMetric,
		HistoryArchivalStatus:    c.HistoryArchivalStatus,
		HistoryArchivalURI:       c.HistoryArchivalURI,
		VisibilityArchivalStatus: c.VisibilityArchivalStatus,
		VisibilityArchivalURI:    c.VisibilityArchivalURI,
		BadBinaries:              badBinaries,
	}, nil
}

func (m *metadataManagerImpl) deserializeNamespaceConfig(ic *InternalNamespaceConfig) (NamespaceConfig, error) {
	if ic == nil {
		return NamespaceConfig{}, nil
	}
	badBinaries, err := m.serializer.DeserializeBadBinaries(ic.BadBinaries)
	if err != nil {
		return NamespaceConfig{}, err
	}
	if badBinaries.Binaries == nil {
		badBinaries.Binaries = map[string]*commonproto.BadBinaryInfo{}
	}
	return NamespaceConfig{
		Retention:                ic.Retention,
		EmitMetric:               ic.EmitMetric,
		HistoryArchivalStatus:    ic.HistoryArchivalStatus,
		HistoryArchivalURI:       ic.HistoryArchivalURI,
		VisibilityArchivalStatus: ic.VisibilityArchivalStatus,
		VisibilityArchivalURI:    ic.VisibilityArchivalURI,
		BadBinaries:              *badBinaries,
	}, nil
}

func (m *metadataManagerImpl) GetMetadata() (*GetMetadataResponse, error) {
	return m.persistence.GetMetadata()
}

func (m *metadataManagerImpl) Close() {
	m.persistence.Close()
}
