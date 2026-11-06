package sqlplugin

// VisibilityVersionDB supports independent visibility schema version metadata.
type VisibilityVersionDB interface {
	CreateVisibilityVersionTables() error
	ReadVisibilityVersion(database string) (string, error)
	UpdateVisibilityVersion(database string, newVersion string, minCompatibleVersion string) error
	WriteVisibilityUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error
}
