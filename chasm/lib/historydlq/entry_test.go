package historydlq

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
)

func TestEntryReadMissingTask(t *testing.T) {
	t.Parallel()
	entry := &Entry{EntryState: &historydlqpb.EntryState{}}
	_, err := entry.Read(&chasm.MockContext{}, MessageRef{}, 7)
	require.ErrorAs(t, err, new(*serviceerror.DataLoss))
}
