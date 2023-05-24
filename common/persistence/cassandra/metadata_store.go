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
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/primitives"
)

const (
	constNamespacePartition     = 0
	namespaceMetadataRecordName = "temporal-namespace-metadata"
)

const (
	templateCreateNamespaceQuery = `INSERT INTO namespaces_by_id (` +
		`id, name) ` +
		`VALUES(?, ?) IF NOT EXISTS`

	templateGetNamespaceQuery = `SELECT name ` +
		`FROM namespaces_by_id ` +
		`WHERE id = ?`

	templateDeleteNamespaceQuery = `DELETE FROM namespaces_by_id ` +
		`WHERE id = ?`

	templateNamespaceColumns = `id, name, detail, detail_encoding, notification_version, is_global_namespace`

	templateCreateNamespaceByNameQueryWithinBatchV2 = `INSERT INTO namespaces ` +
		`( namespaces_partition, ` + templateNamespaceColumns + `) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateGetNamespaceByNameQueryV2 = templateListNamespaceQueryV2 + `and name = ?`

	templateUpdateNamespaceByNameQueryWithinBatchV2 = `UPDATE namespaces ` +
		`SET detail = ? ,` +
		`detail_encoding = ? ,` +
		`is_global_namespace = ? ,` +
		`notification_version = ? ` +
		`WHERE namespaces_partition = ? ` +
		`and name = ?`

	templateGetMetadataQueryV2 = `SELECT notification_version ` +
		`FROM namespaces ` +
		`WHERE namespaces_partition = ? ` +
		`and name = ? `

	templateUpdateMetadataQueryWithinBatchV2 = `UPDATE namespaces ` +
		`SET notification_version = ? ` +
		`WHERE namespaces_partition = ? ` +
		`and name = ? ` +
		`IF notification_version = ? `

	templateDeleteNamespaceByNameQueryV2 = `DELETE FROM namespaces ` +
		`WHERE namespaces_partition = ? ` +
		`and name = ?`

	templateListNamespaceQueryV2 = `SELECT ` +
		templateNamespaceColumns +
		` FROM namespaces ` +
		`WHERE namespaces_partition = ? `

	templateUpdateNamespaceByIdQuery = `UPDATE namespaces_by_id ` +
		`SET name = ? ` +
		`WHERE id = ?`
)

type (
	MetadataStore struct {
		session            gocql.Session
		logger             log.Logger
		currentClusterName string
	}
)

// NewMetadataStore is used to create an instance of the Namespace MetadataStore implementation
func NewMetadataStore(
	currentClusterName string,
	session gocql.Session,
	logger log.Logger,
) (p.MetadataStore, error) {
	return &MetadataStore{
		currentClusterName: currentClusterName,
		session:            session,
		logger:             logger,
	}, nil
}

// CreateNamespace create a namespace
// Cassandra does not support conditional updates across multiple tables.  For this reason we have to first insert into
// 'Namespaces' table and then do a conditional insert into namespaces_by_name table.  If the conditional write fails we
// delete the orphaned entry from namespaces table.  There is a chance delete entry could fail and we never delete the
// orphaned entry from namespaces table.  We might need a background job to delete those orphaned record.
func (m *MetadataStore) CreateNamespace(
	ctx context.Context,
	request *p.InternalCreateNamespaceRequest,
) (*p.CreateNamespaceResponse, error) {

	query := m.session.Query(templateCreateNamespaceQuery, request.ID, request.Name).WithContext(ctx)
	existingRow := make(map[string]interface{})
	applied, err := query.MapScanCAS(existingRow)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("CreateNamespace operation failed. Inserting into namespaces table. Error: %v", err))
	}

	if !applied {
		// if the id with the same name exists in `namespaces_by_id`, fall through and either add a row in `namespaces` table
		// or fail if name exists in that table already. This is to make sure we do not end up with a row in `namespaces_by_id`
		// table and no entry in `namespaces` table
		matched, err := hasNameConflict(existingRow, "name", request.Name)
		if err != nil {
			return nil, err
		}
		if !matched {
			msg := fmt.Sprintf("CreateNamespace with name %v and id %v failed because another namespace with name %v already exists with the same id.", request.Name, request.ID, existingRow["name"])
			return nil, serviceerror.NewNamespaceAlreadyExists(msg)
		}
	}
	return m.CreateNamespaceInV2Table(ctx, request)
}

// CreateNamespaceInV2Table is the temporary function used by namespace v1 -> v2 migration
func (m *MetadataStore) CreateNamespaceInV2Table(
	ctx context.Context,
	request *p.InternalCreateNamespaceRequest,
) (*p.CreateNamespaceResponse, error) {
	metadata, err := m.GetMetadata(ctx)
	if err != nil {
		return nil, err
	}

	batch := m.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(templateCreateNamespaceByNameQueryWithinBatchV2,
		constNamespacePartition,
		request.ID,
		request.Name,
		request.Namespace.Data,
		request.Namespace.EncodingType.String(),
		metadata.NotificationVersion,
		request.IsGlobal,
	)
	m.updateMetadataBatch(batch, metadata.NotificationVersion)

	previous := make(map[string]interface{})
	applied, iter, err := m.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("CreateNamespace operation failed. Inserting into namespaces table. Error: %v", err))
	}
	defer func() { _ = iter.Close() }()
	deleteOrphanNamespace := func() {
		// Delete namespace from `namespaces_by_id`
		if errDelete := m.session.Query(templateDeleteNamespaceQuery, request.ID).WithContext(ctx).Exec(); errDelete != nil {
			m.logger.Warn("Unable to delete orphan namespace record. Error", tag.Error(errDelete))
		}
	}

	if !applied {

		// if both conditions fail, find the one related to the first query
		matched, err := hasNameConflict(previous, "name", request.Name)
		if err != nil {
			return nil, err
		}
		if !matched {
			m := make(map[string]interface{})
			if iter.MapScan(m) {
				previous = m
			}
		}

		// if conditional failure is due to a duplicate name in namespaces table
		matched, err = hasNameConflict(previous, "name", request.Name)
		if err != nil {
			return nil, err
		}
		if matched {
			var existingID string
			if id, ok := previous["id"]; ok {
				existingID = gocql.UUIDToString(id)
				if existingID != request.ID {
					// Delete orphan namespace record before returning back to user
					deleteOrphanNamespace()
				}
			}

			msg := fmt.Sprintf("Namespace already exists.  NamespaceId: %v", existingID)
			return nil, serviceerror.NewNamespaceAlreadyExists(msg)

		}

		// If namespace does not exist already and applied is false,
		// notification_version does not match our expectations and it's conditional failure.
		// Delete orphan namespace record before returning back to user
		deleteOrphanNamespace()
		return nil, serviceerror.NewUnavailable("CreateNamespace operation failed because of conditional failure.")
	}

	return &p.CreateNamespaceResponse{ID: request.ID}, nil
}

func (m *MetadataStore) UpdateNamespace(
	ctx context.Context,
	request *p.InternalUpdateNamespaceRequest,
) error {
	batch := m.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(templateUpdateNamespaceByNameQueryWithinBatchV2,
		request.Namespace.Data,
		request.Namespace.EncodingType.String(),
		request.IsGlobal,
		request.NotificationVersion,
		constNamespacePartition,
		request.Name,
	)
	m.updateMetadataBatch(batch, request.NotificationVersion)

	previous := make(map[string]interface{})
	applied, iter, err := m.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("UpdateNamespace operation failed. Error: %v", err))
	}
	defer func() { _ = iter.Close() }()

	if !applied {
		return serviceerror.NewUnavailable("UpdateNamespace operation failed because of conditional failure.")
	}

	return nil
}

// RenameNamespace should be used with caution.
// Not every namespace can be renamed because namespace name are stored in the database.
// It may leave database in inconsistent state and must be retried until success.
// Step 1. Update row in `namespaces_by_id` table with the new name.
// Step 2. Batch of:
//
//	Insert row into `namespaces` table with new name and new `notification_version`.
//	Delete row from `namespaces` table with old name.
//	Update `notification_version` in metadata row.
//
// NOTE: `namespaces_by_id` is currently used only for `DescribeNamespace` API and namespace Id collision check.
func (m *MetadataStore) RenameNamespace(
	ctx context.Context,
	request *p.InternalRenameNamespaceRequest,
) error {
	// Step 1.
	if updateErr := m.session.Query(templateUpdateNamespaceByIdQuery,
		request.Name,
		request.Id,
	).WithContext(ctx).Exec(); updateErr != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("RenameNamespace operation failed to update 'namespaces_by_id' table. Error: %v", updateErr))
	}

	// Step 2.
	batch := m.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(templateCreateNamespaceByNameQueryWithinBatchV2,
		constNamespacePartition,
		request.Id,
		request.Name,
		request.Namespace.Data,
		request.Namespace.EncodingType.String(),
		request.NotificationVersion,
		request.IsGlobal,
	)
	batch.Query(templateDeleteNamespaceByNameQueryV2,
		constNamespacePartition,
		request.PreviousName,
	)
	m.updateMetadataBatch(batch, request.NotificationVersion)

	previous := make(map[string]interface{})
	applied, iter, err := m.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("RenameNamespace operation failed. Error: %v", err))
	}
	defer func() { _ = iter.Close() }()

	if !applied {
		return serviceerror.NewUnavailable("RenameNamespace operation failed because of conditional failure.")
	}

	return nil
}

func (m *MetadataStore) GetNamespace(
	ctx context.Context,
	request *p.GetNamespaceRequest,
) (*p.InternalGetNamespaceResponse, error) {
	var query gocql.Query
	var err error
	var detail []byte
	var detailEncoding string
	var notificationVersion int64
	var isGlobalNamespace bool

	if len(request.ID) > 0 && len(request.Name) > 0 {
		return nil, serviceerror.NewInvalidArgument("GetNamespace operation failed.  Both ID and Name specified in request.Namespace.")
	} else if len(request.ID) == 0 && len(request.Name) == 0 {
		return nil, serviceerror.NewInvalidArgument("GetNamespace operation failed.  Both ID and Name are empty.")
	}

	handleError := func(name string, ID string, err error) error {
		identity := name
		if gocql.IsNotFoundError(err) {
			if len(ID) > 0 {
				identity = ID
			}
			return serviceerror.NewNamespaceNotFound(identity)
		}
		return serviceerror.NewUnavailable(fmt.Sprintf("GetNamespace operation failed. Error %v", err))
	}

	namespace := request.Name
	if len(request.ID) > 0 {
		query = m.session.Query(templateGetNamespaceQuery, request.ID).WithContext(ctx)
		err = query.Scan(&namespace)
		if err != nil {
			return nil, handleError(request.Name, request.ID, err)
		}
	}

	query = m.session.Query(templateGetNamespaceByNameQueryV2, constNamespacePartition, namespace).WithContext(ctx)
	err = query.Scan(
		nil,
		nil,
		&detail,
		&detailEncoding,
		&notificationVersion,
		&isGlobalNamespace,
	)

	if err != nil {
		return nil, handleError(request.Name, request.ID, err)
	}

	return &p.InternalGetNamespaceResponse{
		Namespace:           p.NewDataBlob(detail, detailEncoding),
		IsGlobal:            isGlobalNamespace,
		NotificationVersion: notificationVersion,
	}, nil
}

func (m *MetadataStore) ListNamespaces(
	ctx context.Context,
	request *p.InternalListNamespacesRequest,
) (*p.InternalListNamespacesResponse, error) {
	query := m.session.Query(templateListNamespaceQueryV2, constNamespacePartition).WithContext(ctx)
	pageSize := request.PageSize
	nextPageToken := request.NextPageToken
	response := &p.InternalListNamespacesResponse{}

	for {
		iter := query.PageSize(pageSize).PageState(nextPageToken).Iter()
		skippedRows := 0

		for {
			var name string
			var detail []byte
			var detailEncoding string
			var notificationVersion int64
			var isGlobal bool
			if !iter.Scan(
				nil,
				&name,
				&detail,
				&detailEncoding,
				&notificationVersion,
				&isGlobal,
			) {
				// done iterating over all namespaces in this page
				break
			}

			// do not include the metadata record
			if name == namespaceMetadataRecordName {
				skippedRows++
				continue
			}
			response.Namespaces = append(response.Namespaces, &p.InternalGetNamespaceResponse{
				Namespace:           p.NewDataBlob(detail, detailEncoding),
				IsGlobal:            isGlobal,
				NotificationVersion: notificationVersion,
			})
		}
		if len(iter.PageState()) > 0 {
			nextPageToken = iter.PageState()
		} else {
			nextPageToken = nil
		}
		if err := iter.Close(); err != nil {
			return nil, serviceerror.NewUnavailable(fmt.Sprintf("ListNamespaces operation failed. Error: %v", err))
		}

		if len(nextPageToken) == 0 {
			// No more records in DB.
			break
		}
		if skippedRows == 0 {
			break
		}
		pageSize = skippedRows
	}

	response.NextPageToken = nextPageToken
	return response, nil
}

func (m *MetadataStore) DeleteNamespace(
	ctx context.Context,
	request *p.DeleteNamespaceRequest,
) error {
	var name string
	query := m.session.Query(templateGetNamespaceQuery, request.ID).WithContext(ctx)
	err := query.Scan(&name)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return nil
		}
		return err
	}

	parsedID, err := primitives.ParseUUID(request.ID)
	if err != nil {
		return err
	}
	return m.deleteNamespace(ctx, name, parsedID)
}

func (m *MetadataStore) DeleteNamespaceByName(
	ctx context.Context,
	request *p.DeleteNamespaceByNameRequest,
) error {
	var ID []byte
	query := m.session.Query(templateGetNamespaceByNameQueryV2, constNamespacePartition, request.Name).WithContext(ctx)
	err := query.Scan(&ID, nil, nil, nil, nil, nil)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return nil
		}
		return err
	}
	return m.deleteNamespace(ctx, request.Name, ID)
}

func (m *MetadataStore) GetMetadata(
	ctx context.Context,
) (*p.GetMetadataResponse, error) {
	var notificationVersion int64
	query := m.session.Query(templateGetMetadataQueryV2, constNamespacePartition, namespaceMetadataRecordName).WithContext(ctx)
	err := query.Scan(&notificationVersion)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			// this error can be thrown in the very beginning,
			// i.e. when namespaces is initialized
			return &p.GetMetadataResponse{NotificationVersion: 0}, nil
		}
		return nil, err
	}
	return &p.GetMetadataResponse{NotificationVersion: notificationVersion}, nil
}

func (m *MetadataStore) updateMetadataBatch(
	batch gocql.Batch,
	notificationVersion int64,
) {
	var nextVersion int64 = 1
	var currentVersion *int64
	if notificationVersion > 0 {
		nextVersion = notificationVersion + 1
		currentVersion = &notificationVersion
	}
	batch.Query(templateUpdateMetadataQueryWithinBatchV2,
		nextVersion,
		constNamespacePartition,
		namespaceMetadataRecordName,
		currentVersion,
	)
}

func (m *MetadataStore) deleteNamespace(ctx context.Context, name string, ID []byte) error {
	query := m.session.Query(templateDeleteNamespaceByNameQueryV2, constNamespacePartition, name).WithContext(ctx)
	if err := query.Exec(); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("DeleteNamespaceByName operation failed. Error %v", err))
	}

	query = m.session.Query(templateDeleteNamespaceQuery, ID).WithContext(ctx)
	if err := query.Exec(); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("DeleteNamespace operation failed. Error %v", err))
	}

	return nil
}

func (m *MetadataStore) GetName() string {
	return cassandraPersistenceName
}

func (m *MetadataStore) Close() {
	if m.session != nil {
		m.session.Close()
	}
}

func hasNameConflict[T comparable](row map[string]interface{}, column string, value T) (bool, error) {
	existingValue, ok := row[column]
	if !ok {
		msg := fmt.Sprintf("Unexpected error: column not found %q", column)
		return false, serviceerror.NewInternal(msg)
	}
	return existingValue == value, nil
}
