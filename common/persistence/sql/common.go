package sql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

// TODO: Rename all SQL Managers to Stores
type SqlStore struct {
	DB     sqlplugin.DB
	logger log.Logger
}

func NewSqlStore(db sqlplugin.DB, logger log.Logger) SqlStore {
	return SqlStore{
		DB:     db,
		logger: logger,
	}
}

func (m *SqlStore) GetName() string {
	return m.DB.PluginName()
}

func (m *SqlStore) GetDbName() string {
	return m.DB.DbName()
}

func (m *SqlStore) Close() {
	if m.DB != nil {
		err := m.DB.Close()
		if err != nil {
			m.logger.Error("Error closing SQL database", tag.Error(err))
		}
	}
}

func (m *SqlStore) txExecute(ctx context.Context, operation string, f func(tx sqlplugin.Tx) error) error {
	tx, err := m.DB.BeginTx(ctx)
	if err != nil {
		return serviceerror.NewUnavailablef("%s failed. Failed to start transaction. Error: %v", operation, err)
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
			*serviceerror.Unavailable,
			*serviceerror.NotFound:
			return err
		default:
			return serviceerror.NewUnavailablef("%v: %v", operation, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return serviceerror.NewUnavailablef("%s operation failed. Failed to commit transaction. Error: %v", operation, err)
	}
	return nil
}

func gobSerialize(x interface{}) ([]byte, error) {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(x)
	if err != nil {
		return nil, serviceerror.NewInternalf("Error in serialization: %v", err)
	}
	return b.Bytes(), nil
}

func gobDeserialize(a []byte, x interface{}) error {
	b := bytes.NewBuffer(a)
	d := gob.NewDecoder(b)
	err := d.Decode(x)
	if err != nil {
		return serviceerror.NewInternalf("Error in deserialization: %v", err)
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

func serializePageTokenJson[T any](token *T) ([]byte, error) {
	return json.Marshal(token)
}

func deserializePageTokenJson[T any](payload []byte) (*T, error) {
	var token T
	if err := json.Unmarshal(payload, &token); err != nil {
		return nil, err
	}
	return &token, nil
}

func convertCommonErrors(
	operation string,
	err error,
) error {
	if err == sql.ErrNoRows {
		return serviceerror.NewNotFoundf("%v failed. Error: %v ", operation, err)
	}

	return serviceerror.NewUnavailablef("%v operation failed. Error: %v", operation, err)
}
