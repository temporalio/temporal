package schedules

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGenerateWorkflowID(t *testing.T) {
	baseWorkflowID := "my-workflow"
	nominalTime := time.Date(2024, 6, 15, 10, 30, 45, 123456789, time.UTC)

	actual := GenerateWorkflowID(baseWorkflowID, nominalTime)
	require.Equal(t, "my-workflow-2024-06-15T10:30:45Z", actual)
}
