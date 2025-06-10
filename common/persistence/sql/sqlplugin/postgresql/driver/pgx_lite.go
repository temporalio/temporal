//go:build lite

package driver

import (
	"errors"
	"github.com/jmoiron/sqlx"
)

type PGXDriver struct{}

func (p *PGXDriver) CreateConnection(string) (*sqlx.DB, error) {
	return nil, errors.New("pgx driver disabled in lite build")
}

func (p *PGXDriver) IsDupEntryError(error) bool         { return false }
func (p *PGXDriver) IsDupDatabaseError(error) bool      { return false }
func (p *PGXDriver) IsConnNeedsRefreshError(error) bool { return false }
