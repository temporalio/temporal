package auth

type (
	// Kerberos describes GSSAPI/Kerberos authentication configuration.
	//
	// At present only the PostgreSQL persistence plugin consumes this
	// struct (and only when using the pgx driver, postgres12_pgx),
	// since lib/pq does not implement GSSAPI.
	Kerberos struct {
		Enabled bool `yaml:"enabled"`

		// Username is the Kerberos principal name (e.g. "temporal").
		// Combined with Realm it forms the full principal used to
		// obtain a service ticket. Required when KeytabFile is set.
		Username string `yaml:"username"`

		// Realm is the Kerberos realm (e.g. "CORP.EXAMPLE.COM").
		// Required when KeytabFile is set.
		Realm string `yaml:"realm"`

		// KeytabFile is a path to a keytab containing credentials for
		// Username@Realm. When set, credentials are loaded from the
		// keytab; otherwise a credential cache is used.
		KeytabFile string `yaml:"keytabFile"`

		// CredentialCacheFile is a path to a Kerberos credential
		// cache (ccache). When KeytabFile is empty, this is used to
		// locate existing credentials (e.g. populated by kinit).
		// If empty, the value of $KRB5CCNAME is honoured and falls
		// back to the OS default (/tmp/krb5cc_<uid>).
		CredentialCacheFile string `yaml:"credentialCacheFile"`

		// ConfigFile is a path to krb5.conf. Defaults to
		// /etc/krb5.conf when empty.
		ConfigFile string `yaml:"configFile"`

		// ServiceName is the PostgreSQL server's Kerberos service
		// name (the "krbsrvname" DSN parameter). Defaults to
		// "postgres" when empty, matching libpq.
		ServiceName string `yaml:"serviceName"`

		// SPN is an explicit service principal name to request
		// (the "krbspn" DSN parameter). When set it takes precedence
		// over ServiceName + host-derived SPN. Use for cases such
		// as SQL Server-style SPNs or cross-realm trusts.
		SPN string `yaml:"spn"`

		// DisableFAST turns off FAST (Flexible Authentication Secure
		// Tunneling) pre-authentication. Required against some
		// Active Directory KDCs that reject FAST-armored AS-REQs.
		DisableFAST bool `yaml:"disableFAST"`
	}
)
