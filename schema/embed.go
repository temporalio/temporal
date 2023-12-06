// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
				dirs = append(dirs, filepath.Dir(path))
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
