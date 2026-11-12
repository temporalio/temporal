package authorization

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
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

// TestProviderClose covers Close() for both refresh configurations. RefreshInterval == 0 means
// "no periodic refresh" - the same convention localStoreCertProvider follows - so initialize()
// leaves ticker and stop nil and Close() must tolerate their absence, as
// localStoreCertProvider.Close() already does. With a refresh interval set, Close() must still
// stop the ticker and signal the goroutine rather than being skipped by the nil guards.
func TestProviderClose(t *testing.T) {
	newProvider := func(refresh time.Duration) *defaultTokenKeyProvider {
		return NewDefaultTokenKeyProvider(&config.Authorization{
			JWTKeyProvider: config.JWTKeyProvider{
				KeySourceURIs:   []string{"file:///nonexistent-jwks.json"},
				RefreshInterval: refresh,
			},
		}, log.NewNoopLogger())
	}

	t.Run("no refresh interval: ticker and stop are nil", func(t *testing.T) {
		p := newProvider(0)
		require.Nil(t, p.ticker, "ticker should not be created")
		require.Nil(t, p.stop, "stop channel should not be created")
		require.NotPanics(t, func() { p.Close() })
	})

	t.Run("refresh interval set: Close still shuts the goroutine down", func(t *testing.T) {
		p := newProvider(time.Hour)
		require.NotNil(t, p.ticker)
		require.NotNil(t, p.stop)

		done := make(chan struct{})
		go func() { defer close(done); p.Close() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "Close() did not return", "the nil guards must not skip the real shutdown")
		}
	})
}
