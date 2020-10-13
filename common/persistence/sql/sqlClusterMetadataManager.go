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

package sql

import (
	"database/sql"
	"fmt"
	"net"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type sqlClusterMetadataManager struct {
	sqlStore
}

var _ p.ClusterMetadataStore = (*sqlClusterMetadataManager)(nil)

func (s *sqlClusterMetadataManager) GetClusterMetadata() (*p.InternalGetClusterMetadataResponse, error) {
	row, err := s.db.GetClusterMetadata()

	if err != nil {
		return nil, convertCommonErrors("GetClusterMetadata", err)
	}

	// TODO(vitarb): immutable metadata is needed for backward compatibility only, remove after 1.1 release.
	if row.Data == nil {
		row.Data = row.ImmutableData
		row.DataEncoding = row.ImmutableDataEncoding
	}
	return &p.InternalGetClusterMetadataResponse{
		ClusterMetadata: p.NewDataBlob(row.Data, row.DataEncoding),
		Version:         row.Version,
	}, nil
}

func (s *sqlClusterMetadataManager) SaveClusterMetadata(request *p.InternalSaveClusterMetadataRequest) (bool, error) {
	err := s.txExecute("SaveClusterMetadata", func(tx sqlplugin.Tx) error {
		oldClusterMetadata, err := tx.WriteLockGetClusterMetadata()
		var lastVersion int64
		if err != nil {
			if err != sql.ErrNoRows {
				return serviceerror.NewInternal(fmt.Sprintf("SaveClusterMetadata operation failed. Error %v", err))
			}
		} else {
			lastVersion = oldClusterMetadata.Version
		}
		if request.Version != lastVersion {
			return serviceerror.NewInternal(fmt.Sprintf("SaveClusterMetadata encountered version mismatch, expected %v but got %v.",
				request.Version, oldClusterMetadata.Version))
		}
		_, err = tx.SaveClusterMetadata(&sqlplugin.ClusterMetadataRow{
			Data:         request.ClusterMetadata.Data,
			DataEncoding: request.ClusterMetadata.Encoding.String(),
			Version:      request.Version,
		})
		if err != nil {
			return convertCommonErrors("SaveClusterMetadata", err)
		}
		return nil
	})

	if err != nil {
		return false, serviceerror.NewInternal(err.Error())
	}
	return true, nil
}

func (s *sqlClusterMetadataManager) GetClusterMembers(request *p.GetClusterMembersRequest) (*p.GetClusterMembersResponse, error) {
	var lastSeenHostId []byte
	if len(request.NextPageToken) == 16 {
		lastSeenHostId = request.NextPageToken
	} else if len(request.NextPageToken) > 0 {
		return nil, serviceerror.NewInternal("page token is corrupted.")
	}

	now := time.Now().UTC()
	filter := &sqlplugin.ClusterMembershipFilter{
		HostIDEquals:        request.HostIDEquals,
		RoleEquals:          request.RoleEquals,
		RecordExpiryAfter:   now,
		SessionStartedAfter: request.SessionStartedAfter,
		MaxRecordCount:      request.PageSize,
	}

	if lastSeenHostId != nil && filter.HostIDEquals == nil {
		filter.HostIDGreaterThan = lastSeenHostId
	}

	if request.LastHeartbeatWithin > 0 {
		filter.LastHeartbeatAfter = now.Add(-request.LastHeartbeatWithin)
	}

	if request.RPCAddressEquals != nil {
		filter.RPCAddressEquals = request.RPCAddressEquals.String()
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
		lastRow := rows[len(rows)-1]
		nextPageToken = lastRow.HostID
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
