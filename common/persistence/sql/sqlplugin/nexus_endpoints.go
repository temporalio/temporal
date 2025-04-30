package sqlplugin

import (
	"context"
	"database/sql"
)

type (
	NexusEndpointsRow struct {
		ID           []byte
		Version      int64
		Data         []byte
		DataEncoding string
	}

	ListNexusEndpointsRequest struct {
		LastID []byte
		Limit  int
	}

	// NexusEndpoints is the SQL persistence interface for Nexus endpoints
	NexusEndpoints interface {
		InitializeNexusEndpointsTableVersion(ctx context.Context) (sql.Result, error)
		IncrementNexusEndpointsTableVersion(ctx context.Context, lastKnownTableVersion int64) (sql.Result, error)
		GetNexusEndpointsTableVersion(ctx context.Context) (int64, error)

		InsertIntoNexusEndpoints(ctx context.Context, row *NexusEndpointsRow) (sql.Result, error)
		UpdateNexusEndpoint(ctx context.Context, row *NexusEndpointsRow) (sql.Result, error)
		GetNexusEndpointByID(ctx context.Context, serviceID []byte) (*NexusEndpointsRow, error)
		ListNexusEndpoints(ctx context.Context, request *ListNexusEndpointsRequest) ([]NexusEndpointsRow, error)
		DeleteFromNexusEndpoints(ctx context.Context, id []byte) (sql.Result, error)
	}
)
