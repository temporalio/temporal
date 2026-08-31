package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestHasRecordedBackfillProgress(t *testing.T) {
	require.False(t, HasRecordedBackfillProgress(nil))
	require.True(t, HasRecordedBackfillProgress(timestamppb.New(time.Unix(0, 0))))
}
