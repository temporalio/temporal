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
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/gob"
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

// TODO: Rename all SQL Managers to Stores
type SqlStore struct {
	Db     sqlplugin.DB
	logger log.Logger
}

func NewSqlStore(db sqlplugin.DB, logger log.Logger) SqlStore {
	return SqlStore{
		Db:     db,
		logger: logger,
	}
}

func (m *SqlStore) GetName() string {
	return m.Db.PluginName()
}

func (m *SqlStore) Close() {
	if m.Db != nil {
		m.Db.Close()
	}
}

func (m *SqlStore) txExecute(ctx context.Context, operation string, f func(tx sqlplugin.Tx) error) error {
	tx, err := m.Db.BeginTx(ctx)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("%s failed. Failed to start transaction. Error: %v", operation, err))
	}
	err = f(tx)
	if err != nil {
		rollBackErr := tx.Rollback()
		if rollBackErr != nil {
			m.logger.Error("transaction rollback error", tag.Error(rollBackErr))
		}

		switch err.(type) {
		case *persistence.ConditionFailedError,
			*persistence.CurrentWorkflowConditionFailedError,
			*persistence.WorkflowConditionFailedError,
			*serviceerror.NamespaceAlreadyExists,
			*persistence.ShardOwnershipLostError,
			*serviceerror.Unavailable:
			return err
		default:
			return serviceerror.NewUnavailable(fmt.Sprintf("%v: %v", operation, err))
		}
	}
	if err := tx.Commit(); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("%s operation failed. Failed to commit transaction. Error: %v", operation, err))
	}
	return nil
}

func gobSerialize(x interface{}) ([]byte, error) {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(x)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Error in serialization: %v", err))
	}
	return b.Bytes(), nil
}

func gobDeserialize(a []byte, x interface{}) error {
	b := bytes.NewBuffer(a)
	d := gob.NewDecoder(b)
	err := d.Decode(x)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Error in deserialization: %v", err))
	}
	return nil
}

func serializePageToken(offset int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(offset))
	return b
}

func deserializePageToken(payload []byte) (int64, error) {
	if len(payload) != 8 {
		return 0, fmt.Errorf("invalid token of %v length", len(payload))
	}
	return int64(binary.LittleEndian.Uint64(payload)), nil
}

func convertCommonErrors(
	operation string,
	err error,
) error {
	if err == sql.ErrNoRows {
		return serviceerror.NewNotFound(fmt.Sprintf("%v failed. Error: %v ", operation, err))
	}

	return serviceerror.NewUnavailable(fmt.Sprintf("%v operation failed. Error: %v", operation, err))
}
