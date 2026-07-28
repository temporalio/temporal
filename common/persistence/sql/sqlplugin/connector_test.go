package sqlplugin

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeConn struct{}

func (fakeConn) Prepare(string) (driver.Stmt, error) { return nil, nil }
func (fakeConn) Close() error                        { return nil }
func (fakeConn) Begin() (driver.Tx, error)           { return nil, nil }

type fakeConnector struct{ dsn string }

func (c *fakeConnector) Connect(context.Context) (driver.Conn, error) { return fakeConn{}, nil }
func (c *fakeConnector) Driver() driver.Driver                        { return nil }

func TestRefreshingConnector_CallsBuildDSNOnEachConnect(t *testing.T) {
	callCount := 0
	c := NewRefreshingConnector(
		func() (string, error) {
			callCount++
			return "dsn-" + string(rune('0'+callCount)), nil
		},
		func(dsn string) (driver.Connector, error) {
			return &fakeConnector{dsn: dsn}, nil
		},
		nil,
	)

	for range 3 {
		_, err := c.Connect(context.Background())
		require.NoError(t, err)
	}
	require.Equal(t, 3, callCount)
}

func TestRefreshingConnector_PropagatesBuildDSNError(t *testing.T) {
	expectedErr := errors.New("token expired")
	c := NewRefreshingConnector(
		func() (string, error) {
			return "", expectedErr
		},
		func(dsn string) (driver.Connector, error) {
			t.Fatal("NewConnector should not be called when BuildDSN fails")
			return nil, nil
		},
		nil,
	)

	_, err := c.Connect(context.Background())
	require.ErrorIs(t, err, expectedErr)
}

func TestRefreshingConnector_PropagatesNewConnectorError(t *testing.T) {
	expectedErr := errors.New("bad dsn")
	c := NewRefreshingConnector(
		func() (string, error) {
			return "some-dsn", nil
		},
		func(dsn string) (driver.Connector, error) {
			return nil, expectedErr
		},
		nil,
	)

	_, err := c.Connect(context.Background())
	require.ErrorIs(t, err, expectedErr)
}
