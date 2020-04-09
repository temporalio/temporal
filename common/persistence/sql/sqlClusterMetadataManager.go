package sql

import (
	"encoding/binary"
	"net"
	"time"

	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
)

type sqlClusterMetadataManager struct {
	sqlStore
}

var _ p.ClusterMetadataStore = (*sqlClusterMetadataManager)(nil)

func (s *sqlClusterMetadataManager) InitializeImmutableClusterMetadata(request *p.InternalInitializeImmutableClusterMetadataRequest) (*p.InternalInitializeImmutableClusterMetadataResponse, error) {
	resp, err := s.GetImmutableClusterMetadata()

	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// If we have received EntityNotExistsError, we have not yet initialized
			return s.InsertImmutableDataIfNotExists(request)
		}
		return nil, err
	}

	// Return our get result if we didn't need to initialize
	return &p.InternalInitializeImmutableClusterMetadataResponse{
		PersistedImmutableMetadata: resp.ImmutableClusterMetadata,
		RequestApplied:             false,
	}, nil

}

func (s *sqlClusterMetadataManager) InsertImmutableDataIfNotExists(request *p.InternalInitializeImmutableClusterMetadataRequest) (*p.InternalInitializeImmutableClusterMetadataResponse, error) {
	// InsertIfNotExists is idempotent and silently fails if already exists.
	// Assuming that if we make it here, no out-of-band method or tool is deleting the db row
	//	in between the Get above and Insert below as that would violate the immutability guarantees.
	// Alternative would be to make the insert non-idempotent and detect insert conflicts
	// or even move to a lock mechanism, but that doesn't appear worth the extra lines of code.
	_, err := s.db.InsertIfNotExistsIntoClusterMetadata(&sqlplugin.ClusterMetadataRow{
		ImmutableData:         request.ImmutableClusterMetadata.Data,
		ImmutableDataEncoding: *common.StringPtr(string(request.ImmutableClusterMetadata.Encoding)),
	})

	if err != nil {
		return nil, err
	}

	return &p.InternalInitializeImmutableClusterMetadataResponse{
		PersistedImmutableMetadata: request.ImmutableClusterMetadata,
		RequestApplied:             true,
	}, nil
}

func (s *sqlClusterMetadataManager) GetImmutableClusterMetadata() (*p.InternalGetImmutableClusterMetadataResponse, error) {
	row, err := s.db.GetClusterMetadata()

	if err != nil {
		return nil, convertCommonErrors("GetImmutableClusterMetadata", err)
	}

	return &p.InternalGetImmutableClusterMetadataResponse{
		ImmutableClusterMetadata: p.NewDataBlob(row.ImmutableData, common.EncodingType(row.ImmutableDataEncoding)),
	}, nil
}

func (s *sqlClusterMetadataManager) GetClusterMembers(request *p.GetClusterMembersRequest) (*p.GetClusterMembersResponse, error) {
	pageToken := uint64(0)
	if len(request.NextPageToken) > 0 {
		pageToken = binary.LittleEndian.Uint64(request.NextPageToken)
	}
	now := time.Now().UTC()
	filter := &sqlplugin.ClusterMembershipFilter{
		HostIDEquals:        request.HostIDEquals,
		RoleEquals:          request.RoleEquals,
		RecordExpiryAfter:   now,
		SessionStartedAfter: request.SessionStartedAfter,
		MaxRecordCount:      request.PageSize,
	}

	if request.LastHeartbeatWithin > 0 {
		filter.LastHeartbeatAfter = now.Add(-request.LastHeartbeatWithin)
	}

	if request.RPCAddressEquals != nil {
		filter.RPCAddressEquals = request.RPCAddressEquals.String()
	}

	if pageToken > 0 {
		filter.InsertionOrderGreaterThan = pageToken
	}

	rows, err := s.db.GetClusterMembers(filter)

	if err != nil {
		return nil, convertCommonErrors("GetClusterMembers", err)
	}

	convertedRows := make([]*p.ClusterMember, 0, len(rows))
	for _, row := range rows {
		convertedRows = append(convertedRows, &p.ClusterMember{
			HostID:        row.HostID,
			Role:          row.Role,
			RPCAddress:    net.ParseIP(row.RPCAddress),
			RPCPort:       row.RPCPort,
			SessionStart:  row.SessionStart,
			LastHeartbeat: row.LastHeartbeat,
			RecordExpiry:  row.RecordExpiry,
		})
	}

	var nextPageToken []byte
	if request.PageSize > 0 && len(rows) == request.PageSize {
		nextPageToken = make([]byte, 8)
		binary.LittleEndian.PutUint64(nextPageToken, rows[len(rows)-1].InsertionOrder)
	}

	return &p.GetClusterMembersResponse{ActiveMembers: convertedRows, NextPageToken: nextPageToken}, nil
}

func (s *sqlClusterMetadataManager) UpsertClusterMembership(request *p.UpsertClusterMembershipRequest) error {
	now := time.Now().UTC()
	recordExpiry := now.Add(request.RecordExpiry)
	_, err := s.db.UpsertClusterMembership(&sqlplugin.ClusterMembershipRow{
		Role:          request.Role,
		HostID:        request.HostID,
		RPCAddress:    request.RPCAddress.String(),
		RPCPort:       request.RPCPort,
		SessionStart:  request.SessionStart,
		LastHeartbeat: now,
		RecordExpiry:  recordExpiry})

	if err != nil {
		return convertCommonErrors("UpsertClusterMembership", err)
	}

	return nil
}

func (s *sqlClusterMetadataManager) PruneClusterMembership(request *p.PruneClusterMembershipRequest) error {
	_, err := s.db.PruneClusterMembership(&sqlplugin.PruneClusterMembershipFilter{
		PruneRecordsBefore: time.Now().UTC(),
		MaxRecordsAffected: request.MaxRecordsPruned})

	if err != nil {
		return convertCommonErrors("PruneClusterMembership", err)
	}

	return nil
}

func newClusterMetadataPersistence(db sqlplugin.DB,
	logger log.Logger) (p.ClusterMetadataStore, error) {
	return &sqlClusterMetadataManager{
		sqlStore: sqlStore{
			db:     db,
			logger: logger,
		},
	}, nil
}
