// Copyright (c) 2017 Uber Technologies, Inc.
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
	"io/ioutil"
	"log"
	"sort"
	"strings"
)

type (
	// UpdateTask represents a task
	// that executes a cassandra schema upgrade
	UpdateTask struct {
		db     DB
		config *UpdateConfig
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

	// byVersion is a comparator type
	// for sorting a set of version
	// strings
	byVersion []string
)

const (
	manifestFileName = "manifest.json"
)

var (
	whitelistedCQLPrefixes = [3]string{"CREATE", "ALTER", "INSERT"}
)

// NewUpdateSchemaTask returns a new instance of UpdateTask
func newUpdateSchemaTask(db DB, config *UpdateConfig) *UpdateTask {
	return &UpdateTask{
		db:     db,
		config: config,
	}
}

// Run executes the task
func (task *UpdateTask) Run() error {
	config := task.config

	log.Printf("UpdateSchemeTask started, config=%+v\n", config)

	if config.IsDryRun {
		if err := task.setupDryrunDatabase(); err != nil {
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

	log.Printf("UpdateSchemeTask done\n")

	return nil
}

func (task *UpdateTask) executeUpdates(currVer string, updates []changeSet) error {

	if len(updates) == 0 {
		log.Printf("found zero updates from current version %v", currVer)
		return nil
	}

	for _, cs := range updates {

		err := task.execCQLStmts(cs.version, cs.cqlStmts)
		if err != nil {
			return err
		}
		err = task.updateSchemaVersion(currVer, &cs)
		if err != nil {
			return err
		}

		log.Printf("Schema updated from %v to %v\n", currVer, cs.version)
		currVer = cs.version
	}

	return nil
}

func (task *UpdateTask) execCQLStmts(ver string, stmts []string) error {
	log.Printf("---- Executing updates for version %v ----\n", ver)
	for _, stmt := range stmts {
		log.Println(rmspaceRegex.ReplaceAllString(stmt, " "))
		e := task.db.Exec(stmt)
		if e != nil {
			return fmt.Errorf("error executing CQL statement:%v", e)
		}
	}
	log.Printf("---- Done ----\n")
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

	verDirs, err := readSchemaDir(config.SchemaDir, currVer, config.TargetVersion)
	if err != nil {
		return nil, fmt.Errorf("error listing schema dir:%v", err.Error())
	}

	var result []changeSet

	for _, vd := range verDirs {

		dirPath := config.SchemaDir + "/" + vd

		m, e := readManifest(dirPath)
		if e != nil {
			return nil, fmt.Errorf("error processing manifest for version %v:%v", vd, e.Error())
		}

		if m.CurrVersion != dirToVersion(vd) {
			return nil, fmt.Errorf("manifest version doesn't match with dirname, dir=%v,manifest.version=%v",
				vd, m.CurrVersion)
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
	jsonStr, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	jsonBlob := []byte(jsonStr)

	var manifest manifest
	err = json.Unmarshal(jsonBlob, &manifest)
	if err != nil {
		return nil, err
	}

	currVer, err := parseValidateVersion(manifest.CurrVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid CurrVersion in manifest")
	}
	manifest.CurrVersion = currVer

	minVer, err := parseValidateVersion(manifest.MinCompatibleVersion)
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

// readSchemaDir returns a sorted list of subdir names that hold
// the schema changes for versions in the range startVer < ver <= endVer
// when endVer is empty this method returns all subdir names that are greater than startVer
// this method has an assumption that the subdirs containing the
// schema changes will be of the form vx.x, where x.x is the version
// returns error when
//  - startVer <= endVer
//  - endVer is empty and no subdirs have version >= startVer
//  - endVer is non-empty and subdir with version == endVer is not found
func readSchemaDir(dir string, startVer string, endVer string) ([]string, error) {

	subdirs, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	hasEndVer := len(endVer) > 0

	if hasEndVer && cmpVersion(startVer, endVer) >= 0 {
		return nil, fmt.Errorf("startVer (%v) must be less than endVer (%v)", startVer, endVer)
	}

	var endFound bool
	var highestVer string
	var result []string

	for _, dir := range subdirs {

		if !dir.IsDir() {
			continue
		}

		dirname := dir.Name()

		if !versionStrRegex.MatchString(dirname) {
			continue
		}

		ver := dirToVersion(dirname)

		if len(highestVer) == 0 {
			highestVer = ver
		} else if cmpVersion(ver, highestVer) > 0 {
			highestVer = ver
		}

		highcmp := 0
		lowcmp := cmpVersion(ver, startVer)
		if hasEndVer {
			highcmp = cmpVersion(ver, endVer)
		}

		if lowcmp <= 0 || highcmp > 0 {
			continue // out of range
		}

		endFound = endFound || (highcmp == 0)
		result = append(result, dirname)
	}

	// when endVer is specified, atleast one result MUST be found since startVer < endVer
	if hasEndVer && !endFound {
		return nil, fmt.Errorf("version dir not found for target version %v", endVer)
	}

	// when endVer is empty and no result is found, then the highest version
	// found must be equal to startVer, else return error
	if !hasEndVer && len(result) == 0 {
		if len(highestVer) == 0 || cmpVersion(startVer, highestVer) != 0 {
			return nil, fmt.Errorf("no subdirs found with version >= %v", startVer)
		}
		return result, nil
	}

	sort.Sort(byVersion(result))

	return result, nil
}

// sets up a temporary dryrun database for
// executing the cassandra schema update
func (task *UpdateTask) setupDryrunDatabase() error {
	setupConfig := &SetupConfig{
		Overwrite:      true,
		InitialVersion: "0.0",
	}
	setupTask := newSetupSchemaTask(task.db, setupConfig)
	return setupTask.Run()
}

func dirToVersion(dir string) string {
	return dir[1:]
}

func (v byVersion) Len() int {
	return len(v)
}

func (v byVersion) Less(i, j int) bool {
	v1 := dirToVersion(v[i])
	v2 := dirToVersion(v[j])
	return cmpVersion(v1, v2) < 0
}

func (v byVersion) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}
