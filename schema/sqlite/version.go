package sqlite

// Version is the SQLite database release version
const Version = "0.11"

// VisibilityVersion is the SQLite visibility database release version
// VisibilityVersion is the current version of the SQLite visibility schema.
// Note: SQLite is used for development only and does not support incremental
// schema migrations — schema.sql reflects the complete current schema.
const VisibilityVersion = "1.14"
