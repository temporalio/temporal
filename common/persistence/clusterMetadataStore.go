package persistence

import (
	"errors"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
)

const (
	clusterMetadataEncoding = common.EncodingTypeProto3
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
