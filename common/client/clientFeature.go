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

package client

import (
	"strconv"
	"strings"
)

const (
	intBase    = 10
	intBitSize = 32

	versionDelim = "."
	versionLen   = 3
)

var defaultVersion = version{0, 0, 0}

type (
	// Feature provides information about client's capibility
	Feature interface {
		SupportStickyQuery() bool
	}

	// FeatureImpl is used for determining the client's capibility.
	// This can be useful when service support a feature, while
	// client does not, so we can use be backward comparible
	FeatureImpl struct {
		libVersion     version
		featureVersion version
		lang           string
	}

	version struct {
		major int64
		minor int64
		patch int64
	}
)

// NewFeatureImpl make a new NewFeatureImpl
// libVersion and featureVersion, will be both of format MAJOR.MINOR.PATCH
func NewFeatureImpl(libVersion string, featureVersion string, lang string) *FeatureImpl {
	impl := &FeatureImpl{
		libVersion:     parseVersion(libVersion),
		featureVersion: parseVersion(featureVersion),
		lang:           lang,
	}

	return impl
}

// SupportStickyQuery whether a client support sticky query
func (feature *FeatureImpl) SupportStickyQuery() bool {
	return feature.featureVersion.major > 0
}

func parseVersion(versionStr string) version {
	var major int64
	var minor int64
	var patch int64
	var err error
	versions := strings.Split(versionStr, versionDelim)

	if len(versions) != versionLen {
		// for any random input version, just assume default
		return defaultVersion
	}

	if major, err = strconv.ParseInt(versions[0], intBase, intBitSize); err != nil {
		return defaultVersion
	}

	if minor, err = strconv.ParseInt(versions[1], intBase, intBitSize); err != nil {
		return defaultVersion
	}

	if patch, err = strconv.ParseInt(versions[2], intBase, intBitSize); err != nil {
		return defaultVersion
	}

	return version{major, minor, patch}

}
