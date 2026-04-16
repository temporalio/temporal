package authorization

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestOpenURI_FileScheme(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"keys":[]}`), 0644))

	provider := &defaultTokenKeyProvider{logger: log.NewNoopLogger()}

	r, err := provider.openURI("file://" + path)
	require.NoError(t, err)
	require.NoError(t, r.Close())
}

func TestOpenURI_FileScheme_Localhost(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"keys":[]}`), 0644))

	provider := &defaultTokenKeyProvider{logger: log.NewNoopLogger()}

	r, err := provider.openURI("file://localhost" + path)
	require.NoError(t, err)
	require.NoError(t, r.Close())
}

func TestOpenURI_FileScheme_RemoteHost(t *testing.T) {
	provider := &defaultTokenKeyProvider{logger: log.NewNoopLogger()}

	_, err := provider.openURI("file://remotehost/tmp/test.json")
	require.ErrorContains(t, err, "remote host")
}

func TestOpenURI_FileScheme_NotFound(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nonexistent.json")

	provider := &defaultTokenKeyProvider{logger: log.NewNoopLogger()}

	_, err := provider.openURI("file://" + path)
	require.Error(t, err)
}
