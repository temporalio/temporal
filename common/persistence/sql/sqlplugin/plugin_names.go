package sqlplugin

// Names of the SQL plugins that ship with the server. They are defined here, and not only in the
// plugin packages, so that code can refer to a plugin without linking its database driver.
const (
	PluginNameMySQL         = "mysql8"
	PluginNamePostgreSQL    = "postgres12"
	PluginNamePostgreSQLPGX = "postgres12_pgx"
	PluginNameSQLite        = "sqlite"
)
