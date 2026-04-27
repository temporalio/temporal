package session

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"strings"

	krb5client "github.com/jcmturner/gokrb5/v8/client"
	krb5config "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/spnego"

	"go.temporal.io/server/common/auth"
)

const defaultKrb5ConfigPath = "/etc/krb5.conf"

// kerberosGSS is a per-connection GSSAPI provider that satisfies the
// pgconn.GSS interface. A fresh instance is returned by
// kerberosGSSFactory on every connection attempt so that SPNEGO
// state is not shared across connections.
type kerberosGSS struct {
	cfg       *auth.Kerberos
	spnegoCli *spnego.SPNEGO
}

// kerberosGSSFactory returns a closure that constructs a new
// kerberosGSS instance. It is registered with pgx via
// pgconn.RegisterGSSProvider. The config pointer is captured so
// changes to it will not affect already-running connections, but
// subsequent connections will see updated values.
func kerberosGSSFactory(cfg *auth.Kerberos) func() (*kerberosGSS, error) {
	return func() (*kerberosGSS, error) {
		if cfg == nil || !cfg.Enabled {
			return nil, errors.New("kerberos provider invoked with disabled config")
		}
		return &kerberosGSS{cfg: cfg}, nil
	}
}

// GetInitToken builds the initial SPNEGO token for service/host.
// pgx calls this when no explicit SPN is configured.
func (g *kerberosGSS) GetInitToken(host, service string) ([]byte, error) {
	if service == "" {
		service = "postgres"
	}
	return g.GetInitTokenFromSPN(service + "/" + host)
}

// GetInitTokenFromSPN builds the initial SPNEGO token for the given
// service principal name.
func (g *kerberosGSS) GetInitTokenFromSPN(spn string) ([]byte, error) {
	cl, err := newKrb5Client(g.cfg)
	if err != nil {
		return nil, err
	}
	g.spnegoCli = spnego.SPNEGOClient(cl, spn)
	if err := g.spnegoCli.AcquireCred(); err != nil {
		return nil, fmt.Errorf("kerberos: acquire credential for %q: %w", spn, err)
	}
	token, err := g.spnegoCli.InitSecContext()
	if err != nil {
		return nil, fmt.Errorf("kerberos: init security context for %q: %w", spn, err)
	}
	buf, err := token.Marshal()
	if err != nil {
		return nil, fmt.Errorf("kerberos: marshal SPNEGO token: %w", err)
	}
	return buf, nil
}

// Continue is called by pgx after the server's reply to the initial
// token. PostgreSQL does not require an additional client-side
// round-trip: the server's GSSContinue response carries the
// mutual-auth token (possibly empty) and is immediately followed by
// AuthenticationOk. We therefore mark the exchange complete after
// the first server reply.
func (g *kerberosGSS) Continue(_ []byte) (done bool, outToken []byte, err error) {
	if g.spnegoCli == nil {
		return false, nil, errors.New("kerberos: Continue called before init token")
	}
	return true, nil, nil
}

func newKrb5Client(cfg *auth.Kerberos) (*krb5client.Client, error) {
	krbCfg, err := loadKrb5Config(cfg.ConfigFile)
	if err != nil {
		return nil, err
	}
	settings := []func(*krb5client.Settings){}
	if cfg.DisableFAST {
		settings = append(settings, krb5client.DisablePAFXFAST(true))
	}

	if cfg.KeytabFile != "" {
		if cfg.Username == "" || cfg.Realm == "" {
			return nil, errors.New("kerberos: username and realm are required when keytabFile is set")
		}
		kt, err := keytab.Load(cfg.KeytabFile)
		if err != nil {
			return nil, fmt.Errorf("kerberos: load keytab %q: %w", cfg.KeytabFile, err)
		}
		cl := krb5client.NewWithKeytab(cfg.Username, cfg.Realm, kt, krbCfg, settings...)
		return cl, nil
	}

	ccPath, err := resolveCCachePath(cfg.CredentialCacheFile)
	if err != nil {
		return nil, err
	}
	ccache, err := credentials.LoadCCache(ccPath)
	if err != nil {
		return nil, fmt.Errorf("kerberos: load credential cache %q: %w", ccPath, err)
	}
	cl, err := krb5client.NewFromCCache(ccache, krbCfg, settings...)
	if err != nil {
		return nil, fmt.Errorf("kerberos: create client from ccache: %w", err)
	}
	return cl, nil
}

func loadKrb5Config(path string) (*krb5config.Config, error) {
	if path == "" {
		path = defaultKrb5ConfigPath
	}
	cfg, err := krb5config.Load(path)
	if err != nil {
		return nil, fmt.Errorf("kerberos: load krb5 config %q: %w", path, err)
	}
	return cfg, nil
}

// resolveCCachePath picks a credential cache path using the same
// precedence as libkrb5: explicit config > $KRB5CCNAME > default
// /tmp/krb5cc_<uid>. The "FILE:" prefix permitted by MIT Kerberos is
// accepted and stripped.
func resolveCCachePath(explicit string) (string, error) {
	candidate := explicit
	if candidate == "" {
		candidate = os.Getenv("KRB5CCNAME")
	}
	if candidate == "" {
		u, err := user.Current()
		if err != nil {
			return "", fmt.Errorf("kerberos: determine current user for default ccache: %w", err)
		}
		return "/tmp/krb5cc_" + u.Uid, nil
	}
	return strings.TrimPrefix(candidate, "FILE:"), nil
}
