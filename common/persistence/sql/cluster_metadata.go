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
	"context"
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
	SqlStore
}

var _ p.ClusterMetadataStore = (*sqlClusterMetadataManager)(nil)

func (s *sqlClusterMetadataManager) ListClusterMetadata(
	ctx context.Context,
	request *p.InternalListClusterMetadataRequest,
) (*p.InternalListClusterMetadataResponse, error) {
	var clusterName string
	if request.NextPageToken != nil {
		err := gobDeserialize(request.NextPageToken, &clusterName)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("error deserializing page token: %v", err))
		}
	}

	rows, err := s.Db.ListClusterMetadata(ctx, &sqlplugin.ClusterMetadataFilter{ClusterName: clusterName, PageSize: &request.PageSize})
	if err != nil {
		if err == sql.ErrNoRows {
			return &p.InternalListClusterMetadataResponse{}, nil
		}
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("ListClusterMetadata operation failed. Failed to get cluster metadata rows. Error: %v", err))
	}

	var clusterMetadata []*p.InternalGetClusterMetadataResponse
	for _, row := range rows {
		resp := &p.InternalGetClusterMetadataResponse{
			ClusterMetadata: p.NewDataBlob(row.Data, row.DataEncoding),
			Version:         row.Version,
		}
		if err != nil {
			return nil, err
		}
		clusterMetadata = append(clusterMetadata, resp)
	}

	resp := &p.InternalListClusterMetadataResponse{ClusterMetadata: clusterMetadata}
	if len(rows) >= request.PageSize {
		nextPageToken, err := gobSerialize(rows[len(rows)-1].ClusterName)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("error serializing page token: %v", err))
		}
		resp.NextPageToken = nextPageToken
	}
	return resp, nil
}

func (s *sqlClusterMetadataManager) GetClusterMetadata(
	ctx context.Context,
	request *p.InternalGetClusterMetadataRequest,
) (*p.InternalGetClusterMetadataResponse, error) {
	row, err := s.Db.GetClusterMetadata(ctx, &sqlplugin.ClusterMetadataFilter{ClusterName: request.ClusterName})

	if err != nil {
		return nil, convertCommonErrors("GetClusterMetadata", err)
	}

	return &p.InternalGetClusterMetadataResponse{
		ClusterMetadata: p.NewDataBlob(row.Data, row.DataEncoding),
		Version:         row.Version,
	}, nil
}

func (s *sqlClusterMetadataManager) SaveClusterMetadata(
	ctx context.Context,
	request *p.InternalSaveClusterMetadataRequest,
) (bool, error) {
	err := s.txExecute(ctx, "SaveClusterMetadata", func(tx sqlplugin.Tx) error {
		oldClusterMetadata, err := tx.WriteLockGetClusterMetadata(
			ctx,
			&sqlplugin.ClusterMetadataFilter{ClusterName: request.ClusterName})
		var lastVersion int64
		if err != nil {
			if err != sql.ErrNoRows {
				return serviceerror.NewUnavailable(fmt.Sprintf("SaveClusterMetadata operation failed. Error %v", err))
			}
		} else {
			lastVersion = oldClusterMetadata.Version
		}
		if request.Version != lastVersion {
			return serviceerror.NewUnavailable(fmt.Sprintf("SaveClusterMetadata encountered version mismatch, expected %v but got %v.",
				request.Version, oldClusterMetadata.Version))
		}
		_, err = tx.SaveClusterMetadata(ctx, &sqlplugin.ClusterMetadataRow{
			ClusterName:  request.ClusterName,
			Data:         request.ClusterMetadata.Data,
			DataEncoding: request.ClusterMetadata.EncodingType.String(),
			Version:      request.Version,
		})
		if err != nil {
			return convertCommonErrors("SaveClusterMetadata", err)
		}
		return nil
	})

	if err != nil {
		return false, serviceerror.NewUnavailable(err.Error())
	}
	return true, nil
}

func (s *sqlClusterMetadataManager) DeleteClusterMetadata(
	ctx context.Context,
	request *p.InternalDeleteClusterMetadataRequest,
) error {
	_, err := s.Db.DeleteClusterMetadata(ctx, &sqlplugin.ClusterMetadataFilter{ClusterName: request.ClusterName})

	if err != nil {
		return convertCommonErrors("DeleteClusterMetadata", err)
	}
	return nil
}

func (s *sqlClusterMetadataManager) GetClusterMembers(
	ctx context.Context,
	request *p.GetClusterMembersRequest,
) (*p.GetClusterMembersResponse, error) {
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

	rows, err := s.Db.GetClusterMembers(ctx, filter)

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

func (s *sqlClusterMetadataManager) UpsertClusterMembership(
	ctx context.Context,
	request *p.UpsertClusterMembershipRequest,
) error {
	now := time.Now().UTC()
	recordExpiry := now.Add(request.RecordExpiry)
	_, err := s.Db.UpsertClusterMembership(ctx, &sqlplugin.ClusterMembershipRow{
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

func (s *sqlClusterMetadataManager) PruneClusterMembership(
	ctx context.Context,
	request *p.PruneClusterMembershipRequest,
) error {
	_, err := s.Db.PruneClusterMembership(
		ctx,
		&sqlplugin.PruneClusterMembershipFilter{
			PruneRecordsBefore: time.Now().UTC(),
		},
	)

	if err != nil {
		return convertCommonErrors("PruneClusterMembership", err)
	}

	return nil
}

func newClusterMetadataPersistence(
	db sqlplugin.DB,
	logger log.Logger,
) (p.ClusterMetadataStore, error) {
	return &sqlClusterMetadataManager{
		SqlStore: NewSqlStore(db, logger),
	}, nil
}
