package session

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/server/common/auth"
)

const testKrb5Conf = `[libdefaults]
default_realm = CORP.EXAMPLE.COM
dns_lookup_kdc = false
dns_lookup_realm = false

[realms]
CORP.EXAMPLE.COM = {
  kdc = kdc.example.com:88
  admin_server = kdc.example.com:749
}
`

func writeTempKrb5Conf(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "krb5.conf")
	require.NoError(t, os.WriteFile(path, []byte(testKrb5Conf), 0o600))
	return path
}

func TestKerberosGSSFactory_DisabledConfig_ReturnsError(t *testing.T) {
	factory := kerberosGSSFactory(&auth.Kerberos{Enabled: false})
	_, err := factory()
	require.ErrorContains(t, err, "disabled config")
}

func TestKerberosGSSFactory_NilConfig_ReturnsError(t *testing.T) {
	factory := kerberosGSSFactory(nil)
	_, err := factory()
	require.Error(t, err)
}

func TestKerberosGSSFactory_EnabledConfig_ReturnsInstance(t *testing.T) {
	factory := kerberosGSSFactory(&auth.Kerberos{Enabled: true})
	g, err := factory()
	require.NoError(t, err)
	require.NotNil(t, g)
	require.NotNil(t, g.cfg)
}

func TestKerberosGSS_ContinueBeforeInit_ReturnsError(t *testing.T) {
	g := &kerberosGSS{cfg: &auth.Kerberos{Enabled: true}}
	_, _, err := g.Continue(nil)
	require.ErrorContains(t, err, "Continue called before init token")
}

func TestLoadKrb5Config_UsesProvidedPath(t *testing.T) {
	path := writeTempKrb5Conf(t)
	cfg, err := loadKrb5Config(path)
	require.NoError(t, err)
	require.Equal(t, "CORP.EXAMPLE.COM", cfg.LibDefaults.DefaultRealm)
}

func TestLoadKrb5Config_MissingFile_ReturnsError(t *testing.T) {
	_, err := loadKrb5Config("/nonexistent/krb5.conf")
	require.Error(t, err)
}

func TestNewKrb5Client_KeytabRequiresUsernameAndRealm(t *testing.T) {
	path := writeTempKrb5Conf(t)
	_, err := newKrb5Client(&auth.Kerberos{
		Enabled:    true,
		ConfigFile: path,
		KeytabFile: "/tmp/does-not-matter.keytab",
	})
	require.ErrorContains(t, err, "username and realm are required")
}

func TestNewKrb5Client_MissingKeytabFile_ReturnsError(t *testing.T) {
	path := writeTempKrb5Conf(t)
	_, err := newKrb5Client(&auth.Kerberos{
		Enabled:    true,
		ConfigFile: path,
		Username:   "svc",
		Realm:      "CORP.EXAMPLE.COM",
		KeytabFile: "/nonexistent.keytab",
	})
	require.ErrorContains(t, err, "load keytab")
}

func TestNewKrb5Client_MissingCCache_ReturnsError(t *testing.T) {
	path := writeTempKrb5Conf(t)
	_, err := newKrb5Client(&auth.Kerberos{
		Enabled:             true,
		ConfigFile:          path,
		CredentialCacheFile: "/nonexistent.ccache",
	})
	require.ErrorContains(t, err, "load credential cache")
}

func TestResolveCCachePath_ExplicitWins(t *testing.T) {
	t.Setenv("KRB5CCNAME", "/env/path")
	got, err := resolveCCachePath("/explicit/path")
	require.NoError(t, err)
	require.Equal(t, "/explicit/path", got)
}

func TestResolveCCachePath_FilePrefixStripped(t *testing.T) {
	got, err := resolveCCachePath("FILE:/some/path")
	require.NoError(t, err)
	require.Equal(t, "/some/path", got)
}

func TestResolveCCachePath_EnvVarUsedWhenExplicitEmpty(t *testing.T) {
	t.Setenv("KRB5CCNAME", "/env/path")
	got, err := resolveCCachePath("")
	require.NoError(t, err)
	require.Equal(t, "/env/path", got)
}

func TestResolveCCachePath_DefaultWhenNothingSet(t *testing.T) {
	t.Setenv("KRB5CCNAME", "")
	got, err := resolveCCachePath("")
	require.NoError(t, err)
	require.Contains(t, got, "/tmp/krb5cc_", "should fall back to uid-based default")
}

