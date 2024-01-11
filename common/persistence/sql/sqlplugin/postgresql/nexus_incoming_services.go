// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package postgresql

import (
	"context"
	"database/sql"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

func (pdb *db) InitializeNexusIncomingServicesTableVersion(ctx context.Context) error {
	return serviceerror.NewUnimplemented("InitializeNexusIncomingServicesTableVersion is not implemented for PostgreSQL plugin")
}

func (pdb *db) IncrementNexusIncomingServicesTableVersion(
	ctx context.Context,
	lastKnownTableVersion int64,
) error {
	return serviceerror.NewUnimplemented("IncrementNexusIncomingServicesTableVersion is not implemented for PostgreSQL plugin")
}

func (pdb *db) InsertIntoNexusIncomingServices(
	ctx context.Context,
	row *sqlplugin.NexusIncomingServicesRow,
) error {
	return serviceerror.NewUnimplemented("InsertIntoNexusIncomingServices is not implemented for PostgreSQL plugin")
}

func (pdb *db) UpdateNexusIncomingService(
	ctx context.Context,
	row *sqlplugin.NexusIncomingServicesRow,
) error {
	return serviceerror.NewUnimplemented("UpdateNexusIncomingService is not implemented for PostgreSQL plugin")
}

func (pdb *db) ListNexusIncomingServices(
	ctx context.Context,
	request *sqlplugin.ListNexusIncomingServicesRequest,
) (*sqlplugin.ListNexusIncomingServicesResponse, error) {
	return nil, serviceerror.NewUnimplemented("ListNexusIncomingServices is not implemented for PostgreSQL plugin")
}

func (pdb *db) DeleteFromNexusIncomingServices(
	ctx context.Context,
	serviceID []byte,
) (sql.Result, error) {
	return nil, serviceerror.NewUnimplemented("DeleteFromNexusIncomingServices is not implemented for PostgreSQL plugin")
}
