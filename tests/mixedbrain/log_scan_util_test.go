package mixedbrain

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubstringValidator(t *testing.T) {
	v := substringValidator{
		name:    "panic",
		include: "panic",
		exclude: []string{"Potential deadlock detected"},
	}

	require.NoError(t, v.Validate(`{"msg":"all good"}`), "no include match")
	require.NoError(t, v.Validate(`{"msg":"panic: Potential deadlock detected"}`), "excluded")
	require.Error(t, v.Validate(`{"msg":"panic: nil map write"}`), "included, not excluded")
}

func TestScanServerLogs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "server.log")
	content := `{"level":"info","msg":"starting"}
{"level":"error","msg":"failed assertion: shard closed","failed-assertion":true}
{"level":"error","msg":"failed assertion: found otherHasTasks in classic metadata","failed-assertion":true}
{"level":"info","msg":"panic recovered: Potential deadlock detected"}
`
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))

	// Only the un-excluded soft assertion on line 2 should be reported.
	problems, err := scanServerLogs(serverLogValidators, path)
	require.NoError(t, err)
	require.Len(t, problems, 1)
	require.Contains(t, problems[0], "server.log:2")
}
