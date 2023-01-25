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

package tqname

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	// mangledTaskQueuePrefix is the prefix for all mangled task queue names.
	mangledTaskQueuePrefix = "/_sys/"

	suffixDelimiter     = "/"
	versionSetDelimiter = ":"
)

type (
	// Name encapsulates all the name mangling we do for task queues.
	//
	// Users work with "high-level task queues" and can give them arbitrary names (except for
	// our prefix).
	//
	// Each high-level task queue corresponds to one or more "low-level task queues", each of
	// which has a distinct task queue manager in memory in matching service, as well as a
	// distinct identity in persistence.
	//
	// There are two pieces of identifying information for low-level task queues: a partition
	// index, and a version set identifier. All low-level task queues have a partition index,
	// which may be 0. Partition 0 is called the "root". The version set identifier is
	// optional: task queues with it are called "versioned" and task queues without it are
	// called "unversioned".
	//
	// All versioned low-level task queues use mangled names. All unversioned low-level task
	// queues with non-zero partition also use mangled names.
	//
	// Mangled names look like this:
	//
	//   /_sys/<base name>/[<version set id>:]<partition id>
	//
	// The partition id is required, and the version set id is optional. If present, it is
	// separated from the partition id by a colon. This scheme lets users use anything they
	// like for a base name, except for strings starting with "/_sys/", without ambiguity.
	//
	// For backward compatibility, unversioned low-level task queues with partition 0 do not
	// use mangled names, they use the bare base name.
	Name struct {
		baseName   string // base name of the task queue as specified by user
		partition  int    // partition of task queue
		versionSet string // version set id
	}
)

var (
	ErrNoParent      = errors.New("root task queue partition has no parent")
	ErrInvalidDegree = errors.New("invalid task queue partition branching degree")
)

// Parse takes a mangled low-level task queue name and returns a Name. Returns an error if the
// given name is not a valid mangled name.
func Parse(name string) (Name, error) {
	baseName := name
	partition := 0
	versionSet := ""

	if strings.HasPrefix(name, mangledTaskQueuePrefix) {
		suffixOff := strings.LastIndex(name, suffixDelimiter)
		if suffixOff <= len(mangledTaskQueuePrefix) {
			return Name{}, fmt.Errorf("invalid task queue name %q", name)
		}
		baseName = name[len(mangledTaskQueuePrefix):suffixOff]

		suffix := name[suffixOff+1:]
		if partitionOff := strings.LastIndex(suffix, versionSetDelimiter); partitionOff == 0 {
			return Name{}, fmt.Errorf("invalid task queue name %q", name)
		} else if partitionOff > 0 {
			// pull out version set
			versionSet, suffix = suffix[:partitionOff], suffix[partitionOff+1:]
		}

		var err error
		partition, err = strconv.Atoi(suffix)
		if err != nil || partition < 0 || partition == 0 && len(versionSet) == 0 {
			return Name{}, fmt.Errorf("invalid task queue name %q", name)
		}
	}

	return Name{
		baseName:   baseName,
		partition:  partition,
		versionSet: versionSet,
	}, nil
}

// FromBaseName takes a base name and returns a Name. Returns an error if name looks like a
// mangled name.
func FromBaseName(name string) (Name, error) {
	if strings.HasPrefix(name, mangledTaskQueuePrefix) {
		return Name{}, fmt.Errorf("base name %q must not have prefix %q", name, mangledTaskQueuePrefix)
	}
	return Name{baseName: name}, nil
}

// IsRoot returns true if this task queue is a root partition.
func (tn Name) IsRoot() bool {
	return tn.partition == 0
}

// WithPartition returns a new Name with the same base and version set but a different partition.
func (tn Name) WithPartition(partition int) Name {
	nn := tn
	nn.partition = partition
	return nn
}

// Root is shorthand for WithPartition(0).
func (tn Name) Root() Name {
	return tn.WithPartition(0)
}

// WithVersionSet returns a new Name with the same base and partition but a different version set.
func (tn Name) WithVersionSet(versionSet string) Name {
	nn := tn
	nn.versionSet = versionSet
	return nn
}

// BaseNameString returns the base name for a task queue. This should be used when looking up
// settings in dynamic config, and pretty much nowhere else. To get the name of the root
// partition, use tn.Root().FullName().
func (tn Name) BaseNameString() string {
	return tn.baseName
}

// Partition returns the partition number for a task queue.
func (tn Name) Partition() int {
	return tn.partition
}

// VersionSet returns the version set for a task queue.
func (tn Name) VersionSet() string {
	return tn.versionSet
}

// Parent returns a Name for the parent partition, using the given branching degree.
func (tn Name) Parent(degree int) (Name, error) {
	if tn.IsRoot() {
		return Name{}, ErrNoParent
	} else if degree < 1 {
		return Name{}, ErrInvalidDegree
	}
	parent := (tn.partition+degree-1)/degree - 1
	return tn.WithPartition(parent), nil
}

// FullName returns the mangled name of the task queue, to be used in RPCs and persistence.
func (tn Name) FullName() string {
	if len(tn.versionSet) == 0 {
		if tn.partition == 0 {
			return tn.baseName
		}
		return fmt.Sprintf("%s%s%s%d", mangledTaskQueuePrefix, tn.baseName, suffixDelimiter, tn.partition)
	}
	// versioned always use prefix
	return fmt.Sprintf("%s%s%s%s%s%d", mangledTaskQueuePrefix, tn.baseName, suffixDelimiter, tn.versionSet, versionSetDelimiter, tn.partition)
}
