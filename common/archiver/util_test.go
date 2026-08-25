package archiver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVisibilityArchivalRecordHash(t *testing.T) {
	require.Equal(
		t,
		"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
		VisibilityArchivalRecordHash([]byte("{}")),
	)
}
