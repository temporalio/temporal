package postgresql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/driver"
)

func TestBeginPipelinedTx_PQUnsupported(t *testing.T) {
	t.Parallel()
	pdb := newDB(sqlplugin.DbKindMain, "temporal", &driver.PQDriver{}, nil, nil, log.NewNoopLogger())

	_, err := pdb.BeginPipelinedTx(context.Background())
	require.ErrorIs(t, err, sqlplugin.ErrPipelineUnsupported)
}
