package postgres

import (
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
)

// db represents a logical connection to mysql database
type db struct {
	db        *sqlx.DB
	tx        *sqlx.Tx
	conn      sqlplugin.Conn
	converter DataConverter
}

var _ sqlplugin.DB = (*db)(nil)
var _ sqlplugin.Tx = (*db)(nil)

// ErrDupEntry indicates a duplicate primary key i.e. the row already exists,
// check http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html
const ErrDupEntry = "23505"

func (pdb *db) IsDupEntryError(err error) bool {
	sqlErr, ok := err.(*pq.Error)
	return ok && sqlErr.Code == ErrDupEntry
}

// newDB returns an instance of DB, which is a logical
// connection to the underlying postgres database
func newDB(xdb *sqlx.DB, tx *sqlx.Tx) *db {
	mdb := &db{db: xdb, tx: tx}
	mdb.conn = xdb
	if tx != nil {
		mdb.conn = tx
	}
	mdb.converter = &converter{}
	return mdb
}

// BeginTx starts a new transaction and returns a reference to the Tx object
func (pdb *db) BeginTx() (sqlplugin.Tx, error) {
	xtx, err := pdb.db.Beginx()
	if err != nil {
		return nil, err
	}
	return newDB(pdb.db, xtx), nil
}

// Commit commits a previously started transaction
func (pdb *db) Commit() error {
	return pdb.tx.Commit()
}

// Rollback triggers rollback of a previously started transaction
func (pdb *db) Rollback() error {
	return pdb.tx.Rollback()
}

// Close closes the connection to the mysql db
func (pdb *db) Close() error {
	return pdb.db.Close()
}

// PluginName returns the name of the mysql plugin
func (pdb *db) PluginName() string {
	return PluginName
}
