// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package hsm

import (
	"fmt"
	"strings"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// TombstoneLog tracks deleted nodes for state-based replication
type TombstoneLog map[string]*persistencespb.VersionedTransition

// NewTombstoneLog creates a new tombstone log
func NewTombstoneLog() TombstoneLog {
	return make(TombstoneLog)
}

// TrackDeletion records a node deletion
// If this path is already under a deleted parent, it will be ignored
// If this is a parent of existing deleted nodes, those will be removed
func (l TombstoneLog) TrackDeletion(k []Key, vt *persistencespb.VersionedTransition) {
	path := pathToString(k)
	// If this path is under an existing deletion, ignore it
	for existingPath := range l {
		if strings.HasPrefix(path, existingPath+"/") {
			// This node is already covered by a parent deletion
			return
		}
	}

	// Remove any existing paths under this one
	for existingPath := range l {
		if strings.HasPrefix(existingPath, path+"/") {
			delete(l, existingPath)
		}
	}

	l[path] = vt
}

// IsDeleted returns whether a node at the given path is deleted
func (l TombstoneLog) IsDeleted(k []Key) bool {
	path := pathToString(k)
	for existingPath := range l {
		if path == existingPath || strings.HasPrefix(path, existingPath+"/") {
			return true
		}
	}
	return false
}

// GetDeletion returns the versioned transition for a deleted node
// Returns nil if the node is not deleted
func (l TombstoneLog) GetDeletion(k []Key) *persistencespb.VersionedTransition {
	path := pathToString(k)
	for existingPath, vt := range l {
		if path == existingPath || strings.HasPrefix(path, existingPath+"/") {
			return vt
		}
	}
	return nil
}

// Helper functions for path handling
func pathToString(path []Key) string {
	if len(path) == 0 {
		return ""
	}
	var str string
	for i, key := range path {
		if i > 0 {
			str += "/"
		}
		str += fmt.Sprintf("%s:%s", key.Type, key.ID)
	}
	return str
}
