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
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/persistenceblobs/v1"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/log"
)

const (
	clusterMetadataEncoding = enumspb.ENCODING_TYPE_PROTO3
)

var (
	// ErrInvalidMembershipExpiry is used when upserting new cluster membership with an invalid duration
	ErrInvalidMembershipExpiry = errors.New("membershipExpiry duration should be atleast 1 second")

	// ErrIncompleteMembershipUpsert is used when upserting new cluster membership with missing fields
	ErrIncompleteMembershipUpsert = errors.New("membership upserts require all fields")
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

func (m *clusterMetadataManagerImpl) GetClusterMembers(request *GetClusterMembersRequest) (*GetClusterMembersResponse, error) {
	return m.persistence.GetClusterMembers(request)
}

func (m *clusterMetadataManagerImpl) UpsertClusterMembership(request *UpsertClusterMembershipRequest) error {
	if request.RecordExpiry.Seconds() < 1 {
		return ErrInvalidMembershipExpiry
	}
	if request.Role == All {
		return ErrIncompleteMembershipUpsert
	}
	if request.RPCAddress == nil {
		return ErrIncompleteMembershipUpsert
	}
	if request.RPCPort == 0 {
		return ErrIncompleteMembershipUpsert
	}
	if request.SessionStart.IsZero() {
		return ErrIncompleteMembershipUpsert
	}

	return m.persistence.UpsertClusterMembership(request)
}

func (m *clusterMetadataManagerImpl) PruneClusterMembership(request *PruneClusterMembershipRequest) error {
	return m.persistence.PruneClusterMembership(request)
}

func (m *clusterMetadataManagerImpl) GetClusterMetadata() (*GetClusterMetadataResponse, error) {
	resp, err := m.persistence.GetClusterMetadata()
	if err != nil {
		return nil, err
	}

	mcm, err := m.serializer.DeserializeClusterMetadata(resp.ClusterMetadata)
	if err != nil {
		return nil, err
	}
	return &GetClusterMetadataResponse{ClusterMetadata: *mcm, Version: resp.Version}, nil
}

func (m *clusterMetadataManagerImpl) SaveClusterMetadata(request *SaveClusterMetadataRequest) (bool, error) {
	mcm, err := m.serializer.SerializeClusterMetadata(&request.ClusterMetadata, clusterMetadataEncoding)
	if err != nil {
		return false, err
	}
	oldClusterMetadata, err := m.GetClusterMetadata()
	if _, notFound := err.(*serviceerror.NotFound); notFound {
		return m.persistence.SaveClusterMetadata(&InternalSaveClusterMetadataRequest{ClusterMetadata: mcm, Version: request.Version})
	}
	if err != nil {
		return false, err
	}
	if immutableFieldsChanged(oldClusterMetadata.ClusterMetadata, request.ClusterMetadata) {
		return false, nil
	}
	// TODO(vitarb): Check for Version != 0 is needed to allow legacy record override with a new cluster ID. Can be removed after v1.1 release.
	if request.Version != 0 && oldClusterMetadata.Version != request.Version {
		return false, serviceerror.NewInternal(fmt.Sprintf("SaveClusterMetadata encountered version mismatch, expected %v but got %v.",
			request.Version, oldClusterMetadata.Version))
	}
	// TODO(vitarb): Needed to handle legacy records during upgrade. Can be removed after v1.1 release.
	request.Version = oldClusterMetadata.Version

	return m.persistence.SaveClusterMetadata(&InternalSaveClusterMetadataRequest{ClusterMetadata: mcm, Version: request.Version})
}

// immutableFieldsChanged returns true if any of immutable fields changed.
func immutableFieldsChanged(old persistenceblobs.ClusterMetadata, cur persistenceblobs.ClusterMetadata) bool {
	return (old.ClusterName != "" && old.ClusterName != cur.ClusterName) ||
		(old.ClusterId != "" && old.ClusterId != cur.ClusterId) ||
		(old.HistoryShardCount != 0 && old.HistoryShardCount != cur.HistoryShardCount)
}
