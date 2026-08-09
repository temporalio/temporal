package sqlplugin

import (
	"context"
	"database/sql/driver"
)

// RefreshingConnector is a driver.Connector that calls buildDSN on every
// Connect, so each new physical connection gets a fresh credential. This is
// used when passwordCommand is configured to fetch short-lived tokens.
type RefreshingConnector struct {
	buildDSN     func() (string, error)
	newConnector func(dsn string) (driver.Connector, error)
	driver       driver.Driver
}

func NewRefreshingConnector(
	buildDSN func() (string, error),
	newConnector func(dsn string) (driver.Connector, error),
	d driver.Driver,
) *RefreshingConnector {
	return &RefreshingConnector{
		buildDSN:     buildDSN,
		newConnector: newConnector,
		driver:       d,
	}
}

func (c *RefreshingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	dsn, err := c.buildDSN()
	if err != nil {
		return nil, err
	}
	connector, err := c.newConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(ctx)
}

func (c *RefreshingConnector) Driver() driver.Driver {
	return c.driver
}
