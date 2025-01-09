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

// Code generated by gowrap. DO NOT EDIT.
// template: gowrap_template
// gowrap: http://github.com/hexdigest/gowrap

package telemetry

//go:generate gowrap gen -p go.temporal.io/server/common/persistence -i ClusterMetadataStore -t gowrap_template -o cluster_metadata_store_gen.go -l ""

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	_sourcePersistence "go.temporal.io/server/common/persistence"
)

// telemetryClusterMetadataStore implements ClusterMetadataStore interface instrumented with OpenTelemetry.
type telemetryClusterMetadataStore struct {
	_sourcePersistence.ClusterMetadataStore
	tracer trace.Tracer
}

// newTelemetryClusterMetadataStore returns telemetryClusterMetadataStore.
func newTelemetryClusterMetadataStore(base _sourcePersistence.ClusterMetadataStore, tracer trace.Tracer) telemetryClusterMetadataStore {
	return telemetryClusterMetadataStore{
		ClusterMetadataStore: base,
		tracer:               tracer,
	}
}

// DeleteClusterMetadata wraps ClusterMetadataStore.DeleteClusterMetadata.
func (d telemetryClusterMetadataStore) DeleteClusterMetadata(ctx context.Context, request *_sourcePersistence.InternalDeleteClusterMetadataRequest) (err error) {
	ctx, span := d.tracer.Start(ctx, "persistence.ClusterMetadataStore/DeleteClusterMetadata")
	defer span.End()

	err = d.ClusterMetadataStore.DeleteClusterMetadata(ctx, request)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// GetClusterMembers wraps ClusterMetadataStore.GetClusterMembers.
func (d telemetryClusterMetadataStore) GetClusterMembers(ctx context.Context, request *_sourcePersistence.GetClusterMembersRequest) (gp1 *_sourcePersistence.GetClusterMembersResponse, err error) {
	ctx, span := d.tracer.Start(ctx, "persistence.ClusterMetadataStore/GetClusterMembers")
	defer span.End()

	gp1, err = d.ClusterMetadataStore.GetClusterMembers(ctx, request)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// GetClusterMetadata wraps ClusterMetadataStore.GetClusterMetadata.
func (d telemetryClusterMetadataStore) GetClusterMetadata(ctx context.Context, request *_sourcePersistence.InternalGetClusterMetadataRequest) (ip1 *_sourcePersistence.InternalGetClusterMetadataResponse, err error) {
	ctx, span := d.tracer.Start(ctx, "persistence.ClusterMetadataStore/GetClusterMetadata")
	defer span.End()

	ip1, err = d.ClusterMetadataStore.GetClusterMetadata(ctx, request)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// ListClusterMetadata wraps ClusterMetadataStore.ListClusterMetadata.
func (d telemetryClusterMetadataStore) ListClusterMetadata(ctx context.Context, request *_sourcePersistence.InternalListClusterMetadataRequest) (ip1 *_sourcePersistence.InternalListClusterMetadataResponse, err error) {
	ctx, span := d.tracer.Start(ctx, "persistence.ClusterMetadataStore/ListClusterMetadata")
	defer span.End()

	ip1, err = d.ClusterMetadataStore.ListClusterMetadata(ctx, request)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// PruneClusterMembership wraps ClusterMetadataStore.PruneClusterMembership.
func (d telemetryClusterMetadataStore) PruneClusterMembership(ctx context.Context, request *_sourcePersistence.PruneClusterMembershipRequest) (err error) {
	ctx, span := d.tracer.Start(ctx, "persistence.ClusterMetadataStore/PruneClusterMembership")
	defer span.End()

	err = d.ClusterMetadataStore.PruneClusterMembership(ctx, request)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// SaveClusterMetadata wraps ClusterMetadataStore.SaveClusterMetadata.
func (d telemetryClusterMetadataStore) SaveClusterMetadata(ctx context.Context, request *_sourcePersistence.InternalSaveClusterMetadataRequest) (b1 bool, err error) {
	ctx, span := d.tracer.Start(ctx, "persistence.ClusterMetadataStore/SaveClusterMetadata")
	defer span.End()

	b1, err = d.ClusterMetadataStore.SaveClusterMetadata(ctx, request)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// UpsertClusterMembership wraps ClusterMetadataStore.UpsertClusterMembership.
func (d telemetryClusterMetadataStore) UpsertClusterMembership(ctx context.Context, request *_sourcePersistence.UpsertClusterMembershipRequest) (err error) {
	ctx, span := d.tracer.Start(ctx, "persistence.ClusterMetadataStore/UpsertClusterMembership")
	defer span.End()

	err = d.ClusterMetadataStore.UpsertClusterMembership(ctx, request)
	if err != nil {
		span.RecordError(err)
	}

	return
}
