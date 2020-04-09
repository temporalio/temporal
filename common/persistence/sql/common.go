package sql

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/gob"
	"fmt"

	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
)

// TODO: Rename all SQL Managers to Stores
type sqlStore struct {
	db     sqlplugin.DB
	logger log.Logger
}

func (m *sqlStore) GetName() string {
	return m.db.PluginName()
}

func (m *sqlStore) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

func (m *sqlStore) txExecute(operation string, f func(tx sqlplugin.Tx) error) error {
	tx, err := m.db.BeginTx()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("%s failed. Failed to start transaction. Error: %v", operation, err))
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
			*serviceerror.Internal,
			*persistence.WorkflowExecutionAlreadyStartedError,
			*serviceerror.NamespaceAlreadyExists,
			*persistence.ShardOwnershipLostError:
			return err
		default:
			return serviceerror.NewInternal(fmt.Sprintf("%v: %v", operation, err))
		}
	}
	if err := tx.Commit(); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("%s operation failed. Failed to commit transaction. Error: %v", operation, err))
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
		return 0, fmt.Errorf("Invalid token of %v length", len(payload))
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

	return serviceerror.NewInternal(fmt.Sprintf("%v operation failed. Error: %v", operation, err))
}
