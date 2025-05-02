package schema

import (
	"embed"
	"io/fs"
	"path/filepath"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

//go:embed *
var assets embed.FS

// Assets returns a file system with the contents of the schema directory
func Assets() fs.FS {
	return assets
}

// PathsByDir returns a list of paths to directories within the schema subdirectory that have versioned schemas in them
func PathsByDir(dbSubDir string) []string {
	logger := log.NewCLILogger()
	efs := Assets()
	dirs := make([]string, 0)
	err := fs.WalkDir(efs, dbSubDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "versioned" {
				dirs = append(dirs, filepath.ToSlash(filepath.Dir(path)))
				return fs.SkipDir
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("error walking embedded schema file system tree, could not generate valid paths", tag.Error(err))
	}
	return dirs
}

func PathsByDB(dbName string) []string {
	if dbName == "sql" {
		return append(PathsByDir("mysql"), PathsByDir("postgresql")...)
	}
	return PathsByDir(dbName)
}
