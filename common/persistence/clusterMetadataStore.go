// Copyright (c) 2020 Temporal Technologies, Inc.
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
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR

package persistence

import (
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
)

const (
	clusterMetadataEncoding = common.EncodingTypeThriftRW
)

type (
	// clusterMetadataManagerImpl implements MetadataManager based on MetadataStore and PayloadSerializer
	clusterMetadataManagerImpl struct {
		serializer  PayloadSerializer
		persistence ClusterMetadataStore
		logger      log.Logger
	}
)

var _ ClusterMetadataManager = (*clusterMetadataManagerImpl)(nil)

//NewClusterMetadataManagerImpl returns new ClusterMetadataManager
func NewClusterMetadataManagerImpl(persistence ClusterMetadataStore, logger log.Logger) ClusterMetadataManager {
	return &clusterMetadataManagerImpl{
		serializer:  NewPayloadSerializer(),
		persistence: persistence,
		logger:      logger,
	}
}

func (m *clusterMetadataManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *clusterMetadataManagerImpl) Close() {
	m.persistence.Close()
}

func (m *clusterMetadataManagerImpl) InitializeImmutableClusterMetadata(request *InitializeImmutableClusterMetadataRequest) (*InitializeImmutableClusterMetadataResponse, error) {
	icm, err := m.serializer.SerializeImmutableClusterMetadata(&request.ImmutableClusterMetadata, clusterMetadataEncoding)
	if err != nil {
		return nil, err
	}

	resp, err := m.persistence.InitializeImmutableClusterMetadata(&InternalInitializeImmutableClusterMetadataRequest{
		ImmutableClusterMetadata: icm,
	})

	if err != nil {
		return nil, err
	}

	deserialized, err := m.serializer.DeserializeImmutableClusterMetadata(resp.PersistedImmutableMetadata)

	if err != nil {
		return nil, err
	}

	return &InitializeImmutableClusterMetadataResponse{
		PersistedImmutableData: *deserialized,
		RequestApplied:         resp.RequestApplied,
	}, nil
}

func (m *clusterMetadataManagerImpl) GetImmutableClusterMetadata() (*GetImmutableClusterMetadataResponse, error) {
	resp, err := m.persistence.GetImmutableClusterMetadata()
	if err != nil {
		return nil, err
	}

	icm, err := m.serializer.DeserializeImmutableClusterMetadata(resp.ImmutableClusterMetadata)
	if err != nil {
		return nil, err
	}

	return &GetImmutableClusterMetadataResponse{*icm}, nil
}
