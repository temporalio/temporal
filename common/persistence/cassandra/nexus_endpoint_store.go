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

package cassandra

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	tableVersionEndpointID = `00000000-0000-0000-0000-000000000000`
)

const (
	rowTypePartitionStatus = iota
	rowTypeNexusEndpoint
)

const (
	// table templates
	templateCreateTableVersion = `INSERT INTO nexus_endpoints(partition, type, id, version) VALUES (0, ?, ?, ?) IF NOT EXISTS`
	templateGetTableVersion    = `SELECT version FROM nexus_endpoints WHERE partition = 0 AND type = ? AND id = ?`
	templateUpdateTableVersion = `UPDATE nexus_endpoints SET version = ? WHERE partition = 0 AND type = ? AND id = ? IF version = ?`

	// endpoint templates
	templateCreateEndpointQuery         = `INSERT INTO nexus_endpoints(partition, type, id, data, data_encoding, version) VALUES(0, ?, ?, ?, ?, ?) IF NOT EXISTS`
	templateUpdateEndpointQuery         = `UPDATE nexus_endpoints SET data = ?, data_encoding = ?, version = ? WHERE partition = 0 AND type = ? AND id = ? IF version = ?`
	templateDeleteEndpointQuery         = `DELETE FROM nexus_endpoints WHERE partition = 0 AND type = ? AND id = ? IF EXISTS`
	templateGetEndpointByIdQuery        = `SELECT data, data_encoding, version FROM nexus_endpoints WHERE partition = 0 AND type = ? AND id = ? LIMIT 1`
	templateBaseListEndpointsQuery      = `SELECT id, data, data_encoding, version FROM nexus_endpoints WHERE partition = 0`
	templateListEndpointsQuery          = templateBaseListEndpointsQuery + ` AND type = ?`
	templateListEndpointsFirstPageQuery = templateBaseListEndpointsQuery + ` ORDER BY type ASC`
)

type (
	NexusEndpointStore struct {
		session gocql.Session
		logger  log.Logger
	}
)

func NewNexusEndpointStore(
	session gocql.Session,
	logger log.Logger,
) p.NexusEndpointStore {
	return &NexusEndpointStore{
		session: session,
		logger:  logger,
	}
}

func (s *NexusEndpointStore) GetName() string {
	return cassandraPersistenceName
}

func (s *NexusEndpointStore) Close() {
	if s.session != nil {
		s.session.Close()
	}
}

func (s *NexusEndpointStore) CreateOrUpdateNexusEndpoint(
	ctx context.Context,
	request *p.InternalCreateOrUpdateNexusEndpointRequest,
) error {
	batch := s.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	if request.Endpoint.Version == 0 {
		batch.Query(templateCreateEndpointQuery,
			rowTypeNexusEndpoint,
			request.Endpoint.ID,
			request.Endpoint.Data.Data,
			request.Endpoint.Data.EncodingType.String(),
			1,
		)
	} else {
		batch.Query(templateUpdateEndpointQuery,
			request.Endpoint.Data.Data,
			request.Endpoint.Data.EncodingType.String(),
			request.Endpoint.Version+1,
			rowTypeNexusEndpoint,
			request.Endpoint.ID,
			request.Endpoint.Version,
		)
	}

	if request.LastKnownTableVersion == 0 {
		batch.Query(templateCreateTableVersion,
			rowTypePartitionStatus,
			tableVersionEndpointID,
			1)
	} else {
		batch.Query(templateUpdateTableVersion,
			request.LastKnownTableVersion+1,
			rowTypePartitionStatus,
			tableVersionEndpointID,
			request.LastKnownTableVersion)
	}

	previousPartitionStatus := make(map[string]interface{})
	applied, iter, err := s.session.MapExecuteBatchCAS(batch, previousPartitionStatus)

	if err != nil {
		return gocql.ConvertError("CreateOrUpdateNexusEndpoint", err)
	}

	previousEndpoint := make(map[string]interface{})
	iter.MapScan(previousEndpoint)

	err = iter.Close()
	if err != nil {
		return gocql.ConvertError("CreateOrUpdateNexusEndpoint", err)
	}

	if !applied {
		currentTableVersion, err := getTypedFieldFromRow[int64]("version", previousPartitionStatus)
		if err != nil {
			return fmt.Errorf("error retrieving current table version: %w", err)
		}
		if currentTableVersion != request.LastKnownTableVersion {
			return fmt.Errorf("%w. provided table version: %v current table version: %v",
				p.ErrNexusTableVersionConflict,
				request.LastKnownTableVersion,
				currentTableVersion)
		}

		currentVersion, err := getTypedFieldFromRow[int64]("version", previousEndpoint)
		if err != nil {
			return fmt.Errorf("error retrieving current endpoint version: %w", err)
		}
		if currentVersion != request.Endpoint.Version {
			return fmt.Errorf("%w. provided endpoint version: %v current endpoint version: %v",
				p.ErrNexusEndpointVersionConflict,
				request.Endpoint.Version,
				currentVersion)
		}

		// This should never happen. This means the request had the correct versions and gocql did not
		// return an error but for some reason the update was not applied.
		return serviceerror.NewInternal("CreateOrUpdateNexusEndpoint failed.")
	}

	return nil
}

func (s *NexusEndpointStore) GetNexusEndpoint(
	ctx context.Context,
	request *p.GetNexusEndpointRequest,
) (*p.InternalNexusEndpoint, error) {
	query := s.session.Query(templateGetEndpointByIdQuery, rowTypeNexusEndpoint, request.ID).WithContext(ctx)

	var data []byte
	var dataEncoding string
	var version int64

	err := query.Scan(&data, &dataEncoding, &version)
	if gocql.IsNotFoundError(err) {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("Nexus endpoint with ID `%v` not found", request.ID))
	}
	if err != nil {
		return nil, gocql.ConvertError("GetNexusEndpoint", err)
	}

	return &p.InternalNexusEndpoint{
		ID:      request.ID,
		Version: version,
		Data:    p.NewDataBlob(data, dataEncoding),
	}, nil
}

func (s *NexusEndpointStore) ListNexusEndpoints(
	ctx context.Context,
	request *p.ListNexusEndpointsRequest,
) (*p.InternalListNexusEndpointsResponse, error) {
	if request.LastKnownTableVersion == 0 && request.NextPageToken == nil {
		return s.listFirstPageWithVersion(ctx, request)
	}

	response := &p.InternalListNexusEndpointsResponse{}

	query := s.session.Query(templateListEndpointsQuery, rowTypeNexusEndpoint).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	endpoints, err := s.getEndpointList(iter)
	if err != nil {
		return nil, err
	}
	response.Endpoints = endpoints

	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("ListNexusEndpoints operation failed: %v", err))
	}

	currentTableVersion, err := s.getTableVersion(ctx)
	if err != nil {
		return nil, err
	}

	response.TableVersion = currentTableVersion

	if request.LastKnownTableVersion != 0 && request.LastKnownTableVersion != currentTableVersion {
		// If request.LastKnownTableVersion == 0 then caller does not care about checking whether they have the most
		// current view while paginating.
		// Otherwise, if there is a version mismatch, then the table has been updated during pagination, and throw
		// error to indicate caller must start over.
		return nil, fmt.Errorf("%w. provided table version: %v current table version: %v",
			p.ErrNexusTableVersionConflict,
			request.LastKnownTableVersion,
			currentTableVersion)
	}

	return response, nil
}

func (s *NexusEndpointStore) DeleteNexusEndpoint(
	ctx context.Context,
	request *p.DeleteNexusEndpointRequest,
) error {
	batch := s.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	batch.Query(templateDeleteEndpointQuery,
		rowTypeNexusEndpoint,
		request.ID)

	batch.Query(templateUpdateTableVersion,
		request.LastKnownTableVersion+1,
		rowTypePartitionStatus,
		tableVersionEndpointID,
		request.LastKnownTableVersion)

	previousPartitionStatus := make(map[string]interface{})
	applied, iter, err := s.session.MapExecuteBatchCAS(batch, previousPartitionStatus)

	if err != nil {
		return gocql.ConvertError("DeleteNexusEndpoint", err)
	}

	err = iter.Close()
	if err != nil {
		return gocql.ConvertError("DeleteNexusEndpoint", err)
	}

	if !applied {
		currentTableVersion, err := getTypedFieldFromRow[int64]("version", previousPartitionStatus)
		if err != nil {
			return fmt.Errorf("error retrieving current table version: %w", err)
		}
		if currentTableVersion != request.LastKnownTableVersion {
			return fmt.Errorf("%w. provided table version: %v current table version: %v",
				p.ErrNexusTableVersionConflict,
				request.LastKnownTableVersion,
				currentTableVersion)
		}

		return fmt.Errorf("%w. provided ID: %v",
			p.ErrNexusEndpointNotFound,
			request.ID)
	}

	return nil
}

func (s *NexusEndpointStore) listFirstPageWithVersion(
	ctx context.Context,
	request *p.ListNexusEndpointsRequest,
) (*p.InternalListNexusEndpointsResponse, error) {
	response := &p.InternalListNexusEndpointsResponse{}

	query := s.session.Query(templateListEndpointsFirstPageQuery).WithContext(ctx)
	iter := query.PageSize(request.PageSize + 1).PageState(nil).Iter() // Use PageSize+1 to account for partitionStatus row

	partitionStateRow := make(map[string]interface{})
	found := iter.MapScan(partitionStateRow)
	if !found {
		cassErr := iter.Close()
		if cassErr != nil && !gocql.IsNotFoundError(cassErr) {
			return nil, gocql.ConvertError("ListNexusEndpoints", cassErr)
		}
		// No result and no error means no endpoints have been inserted yet, so return empty response.
		response.TableVersion = 0
		return response, nil
	}

	tableVersion, err := getTypedFieldFromRow[int64]("version", partitionStateRow)
	if err != nil {
		return nil, err
	}
	response.TableVersion = tableVersion

	endpoints, err := s.getEndpointList(iter)
	if err != nil {
		return nil, err
	}
	response.Endpoints = endpoints

	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListNexusEndpoints", err)
	}

	return response, nil
}

func (s *NexusEndpointStore) getTableVersion(ctx context.Context) (int64, error) {
	query := s.session.Query(templateGetTableVersion, rowTypePartitionStatus, tableVersionEndpointID).WithContext(ctx)

	var version int64
	if err := query.Scan(&version); err != nil {
		return 0, gocql.ConvertError("GetNexusEndpointsTableVersion", err)
	}

	return version, nil
}

func (s *NexusEndpointStore) getEndpointList(iter gocql.Iter) ([]p.InternalNexusEndpoint, error) {
	var endpoints []p.InternalNexusEndpoint

	row := make(map[string]interface{})
	for iter.MapScan(row) {
		id, err := getTypedFieldFromRow[interface{}]("id", row)
		if err != nil {
			return nil, err
		}
		version, err := getTypedFieldFromRow[int64]("version", row)
		if err != nil {
			return nil, err
		}
		data, err := getTypedFieldFromRow[[]byte]("data", row)
		if err != nil {
			return nil, err
		}
		dataEncoding, err := getTypedFieldFromRow[string]("data_encoding", row)
		if err != nil {
			return nil, err
		}

		endpoints = append(endpoints, p.InternalNexusEndpoint{
			ID:      gocql.UUIDToString(id),
			Version: version,
			Data:    p.NewDataBlob(data, dataEncoding),
		})

		row = make(map[string]interface{})
	}

	return endpoints, nil
}
