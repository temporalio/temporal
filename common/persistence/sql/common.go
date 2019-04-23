// Copyright (c) 2018 Uber Technologies, Inc.
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
	"encoding/binary"
	"encoding/gob"
	"fmt"

	"github.com/go-sql-driver/mysql"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
)

// TODO: Rename all SQL Managers to Stores
type sqlStore struct {
	db     sqldb.Interface
	logger log.Logger
}

func (m *sqlStore) GetName() string {
	return m.db.DriverName()
}

func (m *sqlStore) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

func (m *sqlStore) txExecute(operation string, f func(tx sqldb.Tx) error) error {
	tx, err := m.db.BeginTx()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("%s failed. Failed to start transaction. Error: %v", operation, err),
		}
	}
	err = f(tx)
	if err != nil {
		tx.Rollback()
		switch err.(type) {
		case *persistence.ConditionFailedError,
			*persistence.CurrentWorkflowConditionFailedError,
			*workflow.InternalServiceError,
			*persistence.WorkflowExecutionAlreadyStartedError,
			*workflow.DomainAlreadyExistsError,
			*persistence.ShardOwnershipLostError:
			return err
		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("%v: %v", operation, err),
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("%s operation failed. Failed to commit transaction. Error: %v", operation, err),
		}
	}
	return nil
}

// ErrDupEntry MySQL Error 1062 indicates a duplicate primary key i.e. the row already exists,
// so we don't do the insert and return a ConditionalUpdate error.
const ErrDupEntry = 1062

func isDupEntry(err error) bool {
	sqlErr, ok := err.(*mysql.MySQLError)
	return ok && sqlErr.Number == ErrDupEntry
}

func gobSerialize(x interface{}) ([]byte, error) {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(x)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Error in serialization: %v", err),
		}
	}
	return b.Bytes(), nil
}

func gobDeserialize(a []byte, x interface{}) error {
	b := bytes.NewBuffer(a)
	d := gob.NewDecoder(b)
	err := d.Decode(x)

	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Error in deserialization: %v", err),
		}
	}
	return nil
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func int64ToBool(i int64) bool {
	if i == 0 {
		return false
	}
	return true
}

func takeAddressIfNotNil(a []byte) *[]byte {
	if a != nil {
		return &a
	}
	return nil
}

func dereferenceIfNotNil(a *[]byte) []byte {
	if a != nil {
		return *a
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
		return 0, fmt.Errorf("Invalid token of %v length", len(payload))
	}
	return int64(binary.LittleEndian.Uint64(payload)), nil
}
