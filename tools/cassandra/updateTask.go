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

package cassandra

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strings"
)

type (
	// UpdateSchemaTask represents a task
	// that executes a cassandra schema upgrade
	UpdateSchemaTask struct {
		client CQLClient
		config *UpdateSchemaConfig
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
	dryrunKeyspace   = "dryrun_"
	systemKeyspace   = "system"
	manifestFileName = "manifest.json"
)

var (
	whitelistedCQLPrefixes = [2]string{"CREATE", "ALTER"}
)

// NewUpdateSchemaTask returns a new instance of UpdateSchemaTask
func NewUpdateSchemaTask(config *UpdateSchemaConfig) (*UpdateSchemaTask, error) {

	keyspace := config.CassKeyspace
	if config.IsDryRun {
		keyspace = dryrunKeyspace
		err := setupDryrunKeyspace(config)
		if err != nil {
			return nil, fmt.Errorf("error creating dryrun keyspace:%v", err.Error())
		}
	}

	client, err := newCQLClient(config.CassHosts, config.CassPort, config.CassUser, config.CassPassword, keyspace,
		config.CassTimeout)
	if err != nil {
		return nil, err
	}

	return &UpdateSchemaTask{
		client: client,
		config: config,
	}, nil
}

// run executes the task
func (task *UpdateSchemaTask) run() error {

	config := task.config

	defer func() {
		if config.IsDryRun {
			task.client.DropKeyspace(dryrunKeyspace)
		}
		task.client.Close()
	}()

	log.Printf("UpdateSchemeTask started, config=%+v\n", config)

	currVer, err := task.client.ReadSchemaVersion()
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

func (task *UpdateSchemaTask) executeUpdates(currVer string, updates []changeSet) error {

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

func (task *UpdateSchemaTask) execCQLStmts(ver string, stmts []string) error {
	log.Printf("---- Executing updates for version %v ----\n", ver)
	for _, stmt := range stmts {
		log.Println(rmspaceRegex.ReplaceAllString(stmt, " "))
		e := task.client.Exec(stmt)
		if e != nil {
			return fmt.Errorf("error executing CQL statement:%v", e)
		}
	}
	log.Printf("---- Done ----\n")
	return nil
}

func (task *UpdateSchemaTask) updateSchemaVersion(oldVer string, cs *changeSet) error {

	err := task.client.UpdateSchemaVersion(cs.version, cs.manifest.MinCompatibleVersion)
	if err != nil {
		return fmt.Errorf("failed to update schema_version table, err=%v", err.Error())
	}

	err = task.client.WriteSchemaUpdateLog(oldVer, cs.manifest.CurrVersion, cs.manifest.md5, cs.manifest.Description)
	if err != nil {
		return fmt.Errorf("failed to add entry to schema_update_history, err=%v", err.Error())
	}

	return nil
}

func (task *UpdateSchemaTask) buildChangeSet(currVer string) ([]changeSet, error) {

	config := task.config

	verDirs, err := readSchemaDir(config.SchemaDir, currVer, config.TargetVersion)
	if err != nil {
		return nil, fmt.Errorf("error listing schema dir:%v", err.Error())
	}
	if len(verDirs) == 0 {
		return nil, fmt.Errorf("no schema dirs in version range [%v-%v]", currVer, config.TargetVersion)
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

		stmts, e := parseCQLStmts(dirPath, m)
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

func parseCQLStmts(dir string, manifest *manifest) ([]string, error) {

	result := make([]string, 0, 4)

	for _, file := range manifest.SchemaUpdateCqlFiles {
		path := dir + "/" + file
		stmts, err := ParseCQLFile(path)
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

	md5Bytes := md5.Sum(jsonBlob)
	manifest.md5 = hex.EncodeToString(md5Bytes[:])

	return &manifest, nil
}

// readSchemaDir returns a sorted list of subdir names that hold
// the schema changes for versions in the range [startVer - endVer]
// this method has an assumption that the subdirs containing the
// schema changes will be of the form vx.x, where x.x is the version
func readSchemaDir(dir string, startVer string, endVer string) ([]string, error) {

	subdirs, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var endFound bool
	var result []string

	hasEndVer := len(endVer) > 0

	for _, dir := range subdirs {

		if !dir.IsDir() {
			continue
		}

		dirname := dir.Name()

		if !versionStrRegex.MatchString(dirname) {
			continue
		}

		ver := dirToVersion(dirname)

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

	if !endFound {
		return nil, fmt.Errorf("version dir not found for target version %v", endVer)
	}

	sort.Sort(byVersion(result))

	return result, nil
}

// sets up a temporary dryrun keyspace for
// executing the cassandra schema update
func setupDryrunKeyspace(config *UpdateSchemaConfig) error {
	client, err := newCQLClient(config.CassHosts, config.CassPort, config.CassUser, config.CassPassword, systemKeyspace,
		config.CassTimeout)
	if err != nil {
		return err
	}
	defer client.Close()

	err = client.CreateKeyspace(dryrunKeyspace, 1)
	if err != nil {
		return err
	}

	setupConfig := &SetupSchemaConfig{
		BaseConfig: BaseConfig{
			CassHosts:    config.CassHosts,
			CassPort:     config.CassPort,
			CassKeyspace: dryrunKeyspace,
		},
		Overwrite:      true,
		InitialVersion: "0.0",
	}

	setupTask, err := newSetupSchemaTask(setupConfig)
	if err != nil {
		return err
	}

	return setupTask.run()
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
