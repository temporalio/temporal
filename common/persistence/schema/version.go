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
	"fmt"

	"github.com/blang/semver/v4"
)

// VerifyCompatibleVersion ensures that the installed version is greater than or equal to the expected version.
func VerifyCompatibleVersion(
	versionReader VersionReader,
	dbName string,
	expectedVersion string,
) error {

	version, err := versionReader.ReadSchemaVersion(dbName)
	if err != nil {
		return fmt.Errorf("unable to read DB schema version keyspace/database: %s error: %v", dbName, err.Error())
	}
	// In most cases, the versions should match. However if after a schema upgrade there is a code
	// rollback, the code version (expected version) would fall lower than the actual version in
	// Cassandra. This check to allow such rollbacks since we only make backwards compatible schema changes.
	versionParsed, _ := semver.ParseTolerant(version)
	expectedVersionParsed, _ := semver.ParseTolerant(expectedVersion)

	if versionParsed.LT(expectedVersionParsed) {
		return fmt.Errorf("version mismatch for keyspace/database: %q. Expected version: %s cannot be greater than Actual version: %s", dbName, expectedVersion, version)
	}
	return nil
}
