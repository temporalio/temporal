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
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
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

func (m *metadataManagerImpl) CreateDomain(request *CreateDomainRequest) (*CreateDomainResponse, error) {
	dc, err := m.serializeDomainConfig(request.Config)
	if err != nil {
		return nil, err
	}
	return m.persistence.CreateDomain(&InternalCreateDomainRequest{
		Info:              request.Info,
		Config:            &dc,
		ReplicationConfig: request.ReplicationConfig,
		IsGlobalDomain:    request.IsGlobalDomain,
		ConfigVersion:     request.ConfigVersion,
		FailoverVersion:   request.FailoverVersion,
	})
}

func (m *metadataManagerImpl) GetDomain(request *GetDomainRequest) (*GetDomainResponse, error) {
	resp, err := m.persistence.GetDomain(request)
	if err != nil {
		return nil, err
	}

	dc, err := m.deserializeDomainConfig(resp.Config)
	if err != nil {
		return nil, err
	}

	return &GetDomainResponse{
		Info:                        resp.Info,
		Config:                      &dc,
		ReplicationConfig:           resp.ReplicationConfig,
		IsGlobalDomain:              resp.IsGlobalDomain,
		ConfigVersion:               resp.ConfigVersion,
		FailoverVersion:             resp.FailoverVersion,
		FailoverNotificationVersion: resp.FailoverNotificationVersion,
		NotificationVersion:         resp.NotificationVersion,
	}, nil
}

func (m *metadataManagerImpl) UpdateDomain(request *UpdateDomainRequest) error {
	dc, err := m.serializeDomainConfig(request.Config)
	if err != nil {
		return err
	}
	return m.persistence.UpdateDomain(&InternalUpdateDomainRequest{
		Info:                        request.Info,
		Config:                      &dc,
		ReplicationConfig:           request.ReplicationConfig,
		ConfigVersion:               request.ConfigVersion,
		FailoverVersion:             request.FailoverVersion,
		FailoverNotificationVersion: request.FailoverNotificationVersion,
		NotificationVersion:         request.NotificationVersion,
	})
}

func (m *metadataManagerImpl) DeleteDomain(request *DeleteDomainRequest) error {
	return m.persistence.DeleteDomain(request)
}

func (m *metadataManagerImpl) DeleteDomainByName(request *DeleteDomainByNameRequest) error {
	return m.persistence.DeleteDomainByName(request)
}

func (m *metadataManagerImpl) ListDomains(request *ListDomainsRequest) (*ListDomainsResponse, error) {
	resp, err := m.persistence.ListDomains(request)
	if err != nil {
		return nil, err
	}
	domains := make([]*GetDomainResponse, 0, len(resp.Domains))
	for _, d := range resp.Domains {
		dc, err := m.deserializeDomainConfig(d.Config)
		if err != nil {
			return nil, err
		}
		domains = append(domains, &GetDomainResponse{
			Info:                        d.Info,
			Config:                      &dc,
			ReplicationConfig:           d.ReplicationConfig,
			IsGlobalDomain:              d.IsGlobalDomain,
			ConfigVersion:               d.ConfigVersion,
			FailoverVersion:             d.FailoverVersion,
			FailoverNotificationVersion: d.FailoverNotificationVersion,
			NotificationVersion:         d.NotificationVersion,
		})
	}
	return &ListDomainsResponse{
		Domains:       domains,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (m *metadataManagerImpl) serializeDomainConfig(c *DomainConfig) (InternalDomainConfig, error) {
	if c == nil {
		return InternalDomainConfig{}, nil
	}
	if c.BadBinaries.Binaries == nil {
		c.BadBinaries.Binaries = map[string]*shared.BadBinaryInfo{}
	}
	badBinaries, err := m.serializer.SerializeBadBinaries(&c.BadBinaries, common.EncodingTypeThriftRW)
	if err != nil {
		return InternalDomainConfig{}, err
	}
	return InternalDomainConfig{
		Retention:                c.Retention,
		EmitMetric:               c.EmitMetric,
		HistoryArchivalStatus:    c.HistoryArchivalStatus,
		HistoryArchivalURI:       c.HistoryArchivalURI,
		VisibilityArchivalStatus: c.VisibilityArchivalStatus,
		VisibilityArchivalURI:    c.VisibilityArchivalURI,
		BadBinaries:              badBinaries,
	}, nil
}

func (m *metadataManagerImpl) deserializeDomainConfig(ic *InternalDomainConfig) (DomainConfig, error) {
	if ic == nil {
		return DomainConfig{}, nil
	}
	badBinaries, err := m.serializer.DeserializeBadBinaries(ic.BadBinaries)
	if err != nil {
		return DomainConfig{}, err
	}
	if badBinaries.Binaries == nil {
		badBinaries.Binaries = map[string]*shared.BadBinaryInfo{}
	}
	return DomainConfig{
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
