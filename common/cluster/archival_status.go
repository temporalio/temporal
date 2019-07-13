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

package cluster

import (
	"fmt"
	"strings"
)

type (
	// ArchivalStatus represents the archival status of the cluster
	ArchivalStatus int

	// DomainArchivalStatus represents the archival status of a domain
	DomainArchivalStatus int

	// ArchivalConfig is an immutable representation of the current archival configuration of the cluster
	ArchivalConfig struct {
		ClusterStatus          ArchivalStatus
		EnableReadFromArchival bool
		DomainDefaultStatus    DomainArchivalStatus
		DomainDefaultURI       string
	}
)

const (
	// ArchivalDisabled means this cluster is not configured to handle archival
	ArchivalDisabled ArchivalStatus = iota
	// ArchivalPaused means this cluster is configured to handle archival but is currently not archiving
	ArchivalPaused
	// ArchivalEnabled means this cluster is currently archiving
	ArchivalEnabled

	// DomainArchivalDisabled means a domain is not configured to handle archival
	DomainArchivalDisabled DomainArchivalStatus = iota
	// DomainArchivalEnabled means a domain is currently archiving
	DomainArchivalEnabled
)

// NewArchivalConfig constructs a new valid ArchivalConfig
func NewArchivalConfig(
	clusterStatus ArchivalStatus,
	enableReadFromArchival bool,
	domainDefaultStatus DomainArchivalStatus,
	domainDefaultURI string,
) *ArchivalConfig {
	ac := &ArchivalConfig{
		ClusterStatus:          clusterStatus,
		EnableReadFromArchival: enableReadFromArchival,
		DomainDefaultStatus:    domainDefaultStatus,
		DomainDefaultURI:       domainDefaultURI,
	}
	if !ac.isDomainDefaultValid() {
		return newDisabledArchivalConfig()
	}
	return ac
}

// ClusterConfiguredForArchival returns true if cluster is configured to handle archival, false otherwise.
func (a *ArchivalConfig) ClusterConfiguredForArchival() bool {
	return a.ClusterStatus == ArchivalEnabled
}

func (a *ArchivalConfig) isDomainDefaultValid() bool {
	URISet := len(a.DomainDefaultURI) != 0
	disabled := a.DomainDefaultStatus == DomainArchivalDisabled
	return (!URISet && disabled) || (URISet && !disabled)
}

func newDisabledArchivalConfig() *ArchivalConfig {
	return &ArchivalConfig{
		ClusterStatus:          ArchivalDisabled,
		EnableReadFromArchival: false,
		DomainDefaultStatus:    DomainArchivalDisabled,
		DomainDefaultURI:       "",
	}
}

func getClusterArchivalStatus(str string) (ArchivalStatus, error) {
	str = strings.TrimSpace(strings.ToLower(str))
	switch str {
	case "", "disabled":
		return ArchivalDisabled, nil
	case "paused":
		return ArchivalPaused, nil
	case "enabled":
		return ArchivalEnabled, nil
	}
	return ArchivalDisabled, fmt.Errorf("invalid archival status of %v for cluster, valid status are: {\"\", \"disabled\", \"paused\", \"enabled\"}", str)
}

func getDomainArchivalStatus(str string) (DomainArchivalStatus, error) {
	str = strings.TrimSpace(strings.ToLower(str))
	switch str {
	case "", "disabled":
		return DomainArchivalDisabled, nil
	case "enabled":
		return DomainArchivalEnabled, nil
	}
	return DomainArchivalDisabled, fmt.Errorf("invalid archival status of %v for domain, valid status are: {\"\", \"disabled\", \"enabled\"}", str)
}
