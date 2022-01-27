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
	// In this context md5 is just used for versioning the current schema. It is a weak cryptographic primitive and
	// should not be used for anything more important (password hashes etc.). Marking it as #nosec because of how it's
	// being used.
	"crypto/md5" // #nosec
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/blang/semver/v4"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	// UpdateTask represents a task
	// that executes a cassandra schema upgrade
	UpdateTask struct {
		db     DB
		config *UpdateConfig
		logger log.Logger
	}

	// manifest is a value type that represents
	// the deserialized manifest.json file within
	// a schema version directory
	manifest struct {
		CurrVersion          string
		MinCompatibleVersion string
		Description          string
		SchemaUpdateCqlFiles []string
		md5                  string
	}

	// changeSet represents all the changes
	// corresponding to a single schema version
	changeSet struct {
		version  string
		manifest *manifest
		cqlStmts []string
	}
)

const (
	manifestFileName = "manifest.json"
)

var (
	whitelistedCQLPrefixes = [4]string{"CREATE", "ALTER", "INSERT", "DROP"}
	versionDirectoryRegex  = regexp.MustCompile(`^v[\d.]+`)
)

// NewUpdateSchemaTask returns a new instance of UpdateTask
func newUpdateSchemaTask(db DB, config *UpdateConfig, logger log.Logger) *UpdateTask {
	return &UpdateTask{
		db:     db,
		config: config,
		logger: logger,
	}
}

// Run executes the task
func (task *UpdateTask) Run() error {
	config := task.config

	task.logger.Info("UpdateSchemeTask started", tag.NewAnyTag("config", config))

	if config.IsDryRun {
		if err := task.setupDryRunDatabase(); err != nil {
			return fmt.Errorf("error creating dryrun database:%v", err.Error())
		}
	}

	currVer, err := task.db.ReadSchemaVersion()
	if err != nil {
		return fmt.Errorf("error reading current schema version:%v", err.Error())
	}

	updates, err := task.buildChangeSet(currVer)
	if err != nil {
		return err
	}

	err = task.executeUpdates(currVer, updates)
	if err != nil {
		return err
	}

	task.logger.Info("UpdateSchemeTask done")

	return nil
}

func (task *UpdateTask) executeUpdates(currVer string, updates []changeSet) error {
	if len(updates) == 0 {
		task.logger.Debug(fmt.Sprintf("found zero updates from current version %v", currVer))
		return nil
	}

	for _, cs := range updates {

		err := task.execStmts(cs.version, cs.cqlStmts)
		if err != nil {
			return err
		}
		err = task.updateSchemaVersion(currVer, &cs)
		if err != nil {
			return err
		}

		task.logger.Debug(fmt.Sprintf("Schema updated from %v to %v", currVer, cs.version))
		currVer = cs.version
	}

	return nil
}

func (task *UpdateTask) execStmts(ver string, stmts []string) error {
	task.logger.Debug(fmt.Sprintf("---- Executing updates for version %v ----", ver))
	for _, stmt := range stmts {
		task.logger.Debug(rmspaceRegex.ReplaceAllString(stmt, " "))
		e := task.db.Exec(stmt)
		if e != nil {
			return fmt.Errorf("error executing statement:%v", e)
		}
	}
	task.logger.Debug("---- Done ----")
	return nil
}

func (task *UpdateTask) updateSchemaVersion(oldVer string, cs *changeSet) error {

	err := task.db.UpdateSchemaVersion(cs.version, cs.manifest.MinCompatibleVersion)
	if err != nil {
		return fmt.Errorf("failed to update schema_version table, err=%v", err.Error())
	}

	err = task.db.WriteSchemaUpdateLog(oldVer, cs.manifest.CurrVersion, cs.manifest.md5, cs.manifest.Description)
	if err != nil {
		return fmt.Errorf("failed to add entry to schema_update_history, err=%v", err.Error())
	}

	return nil
}

func (task *UpdateTask) buildChangeSet(currVer string) ([]changeSet, error) {

	config := task.config

	verDirs, err := readSchemaDir(config.SchemaDir, currVer, config.TargetVersion, task.logger)
	if err != nil {
		return nil, fmt.Errorf("error listing schema dir:%v", err.Error())
	}

	task.logger.Debug(fmt.Sprintf("Schema Dirs: %s", verDirs))

	var result []changeSet

	for _, vd := range verDirs {

		dirPath := config.SchemaDir + "/" + vd

		m, e := readManifest(dirPath)
		if e != nil {
			return nil, fmt.Errorf("error processing manifest for version %v:%v", vd, e.Error())
		}

		if m.CurrVersion != dirToVersion(vd) {
			return nil, fmt.Errorf(
				"manifest version doesn't match with dirname, dir=%v,manifest.version=%v",
				vd, m.CurrVersion,
			)
		}

		stmts, e := task.parseSQLStmts(dirPath, m)
		if e != nil {
			return nil, e
		}

		e = validateCQLStmts(stmts)
		if e != nil {
			return nil, fmt.Errorf("error processing version %v:%v", vd, e.Error())
		}

		cs := changeSet{}
		cs.manifest = m
		cs.cqlStmts = stmts
		cs.version = m.CurrVersion
		result = append(result, cs)
	}

	return result, nil
}

func (task *UpdateTask) parseSQLStmts(dir string, manifest *manifest) ([]string, error) {

	result := make([]string, 0, 4)

	for _, file := range manifest.SchemaUpdateCqlFiles {
		path := dir + "/" + file
		task.logger.Info("Processing schema file: " + path)
		stmts, err := ParseFile(path)
		if err != nil {
			return nil, fmt.Errorf("error parsing file %v, err=%v", path, err)
		}
		result = append(result, stmts...)
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("found 0 updates in dir %v", dir)
	}

	return result, nil
}

func validateCQLStmts(stmts []string) error {
	for _, stmt := range stmts {
		valid := false
		for _, prefix := range whitelistedCQLPrefixes {
			if strings.HasPrefix(stmt, prefix) {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("CQL prefix not in whitelist, stmt=%v", stmt)
		}
	}
	return nil
}

func readManifest(dirPath string) (*manifest, error) {

	filePath := dirPath + "/" + manifestFileName
	jsonBlob, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var manifest manifest
	err = json.Unmarshal(jsonBlob, &manifest)
	if err != nil {
		return nil, err
	}

	currVer, err := normalizeVersionString(manifest.CurrVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid CurrVersion in manifest")
	}
	manifest.CurrVersion = currVer

	minVer, err := normalizeVersionString(manifest.MinCompatibleVersion)
	if err != nil {
		return nil, err
	}
	if len(manifest.MinCompatibleVersion) == 0 {
		return nil, fmt.Errorf("invalid MinCompatibleVersion in manifest")
	}
	manifest.MinCompatibleVersion = minVer

	if len(manifest.SchemaUpdateCqlFiles) == 0 {
		return nil, fmt.Errorf("manifest missing SchemaUpdateCqlFiles")
	}

	// See comment above. This is an appropriate usage of md5.
	// #nosec
	md5Bytes := md5.Sum(jsonBlob)
	manifest.md5 = hex.EncodeToString(md5Bytes[:])

	return &manifest, nil
}

// sortAndFilterVersions returns a sorted list of semantic versions the fall within the range
// (startVerExcl, endVerIncl]. If endVerIncl is not specified, returns all versions > startVerExcl.
// If endVerIncl is specified, it must be present in the list of versions.
func sortAndFilterVersions(versions []string, startVerExcl string, endVerIncl string, logger log.Logger) ([]string, error) {

	startVersionExclusive, err := semver.ParseTolerant(startVerExcl)
	if err != nil {
		return nil, err
	}

	var endVersionInclusive *semver.Version
	if len(endVerIncl) > 0 {
		evi, err := semver.ParseTolerant(endVerIncl)
		if err != nil {
			return nil, err
		}
		endVersionInclusive = &evi

		cmp := startVersionExclusive.Compare(*endVersionInclusive)
		if cmp > 0 {
			return nil, fmt.Errorf("start version '%s' must be less than end version '%s'", startVerExcl, endVerIncl)
		} else if cmp == 0 {
			logger.Warn(
				fmt.Sprintf(
					"Start version '%s' is equal to end version '%s'. Returning empty version list",
					startVerExcl,
					endVerIncl,
				),
			)
			return []string{}, nil
		}
	}

	var retVersions []string

	foundEndVer := false

	for _, version := range versions {
		semVer, err := semver.ParseTolerant(version)
		if err != nil {
			logger.Warn(fmt.Sprintf("Input '%s' is not a valid semver", version))
			continue
		}

		if startVersionExclusive.Compare(semVer) >= 0 {
			continue
		}

		if endVersionInclusive != nil && endVersionInclusive.Compare(semVer) < 0 {
			continue
		}

		if endVersionInclusive != nil && endVersionInclusive.Compare(semVer) == 0 {
			foundEndVer = true
		}

		retVersions = append(retVersions, version)
	}

	if endVersionInclusive != nil && !foundEndVer {
		return nil, fmt.Errorf("end version '%s' specified but not found. existing versions: %s", endVerIncl, versions)
	}

	sort.Slice(retVersions, func(i, j int) bool {
		verI, err := semver.ParseTolerant(retVersions[i])
		if err != nil {
			panic(err)
		}
		verJ, err := semver.ParseTolerant(retVersions[j])
		if err != nil {
			panic(err)
		}
		return verI.Compare(verJ) < 0
	})

	return retVersions, nil
}

// readSchemaDir returns a sorted list of subdir names that hold
// the schema changes for versions in the range startVer < ver <= endVer
// when endVer is empty this method returns all subdir names that are greater than startVer
func readSchemaDir(dir string, startVer string, endVer string, logger log.Logger) ([]string, error) {

	subDirs, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	if len(subDirs) == 0 {
		return nil, fmt.Errorf("directory '%s' contains no subDirs", dir)
	}

	dirNames := make([]string, 0, len(subDirs))
	for _, d := range subDirs {
		if !d.IsDir() {
			logger.Warn("not a directory: " + d.Name())
			continue
		}

		if !versionDirectoryRegex.MatchString(d.Name()) {
			logger.Warn("invalid directory name: " + d.Name())
			continue
		}

		dirNames = append(dirNames, d.Name())
	}

	return sortAndFilterVersions(dirNames, startVer, endVer, logger)
}

// sets up a temporary dryrun database for
// executing the cassandra schema update
func (task *UpdateTask) setupDryRunDatabase() error {
	setupConfig := &SetupConfig{
		Overwrite:      true,
		InitialVersion: "0.0",
	}
	setupTask := newSetupSchemaTask(task.db, setupConfig, task.logger)
	return setupTask.Run()
}

func dirToVersion(dir string) string {
	return dir[1:]
}
