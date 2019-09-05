// Copyright (c) 2019 Uber Technologies, Inc.
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

package archiver

import (
	"fmt"
	"strings"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	// ArchivalMetadata provides cluster level archival information
	ArchivalMetadata interface {
		GetHistoryConfig() ArchivalConfig
		GetVisibilityConfig() ArchivalConfig
	}

	// ArchivalConfig is an immutable representation of the archival configuration of the cluster
	// This config is determined at cluster startup time
	ArchivalConfig interface {
		ClusterConfiguredForArchival() bool
		GetClusterStatus() ArchivalStatus
		ReadEnabled() bool
		GetDomainDefaultStatus() shared.ArchivalStatus
		GetDomainDefaultURI() string
	}

	archivalMetadata struct {
		historyConfig    ArchivalConfig
		visibilityConfig ArchivalConfig
	}

	archivalConfig struct {
		staticClusterStatus  ArchivalStatus
		dynamicClusterStatus dynamicconfig.StringPropertyFn
		enableRead           dynamicconfig.BoolPropertyFn
		domainDefaultStatus  shared.ArchivalStatus
		domainDefaultURI     string
	}

	// ArchivalStatus represents the archival status of the cluster
	ArchivalStatus int
)

const (
	// ArchivalDisabled means this cluster is not configured to handle archival
	ArchivalDisabled ArchivalStatus = iota
	// ArchivalPaused means this cluster is configured to handle archival but is currently not archiving
	// This state is not yet implemented, as of now ArchivalPaused is treated the same way as ArchivalDisabled
	ArchivalPaused
	// ArchivalEnabled means this cluster is currently archiving
	ArchivalEnabled
)

// NewArchivalMetadata constructs a new ArchivalMetadata
func NewArchivalMetadata(
	dc *dynamicconfig.Collection,
	historyStatus string,
	historyReadEnabled bool,
	visibilityStatus string,
	visibilityReadEnabled bool,
	domainDefaults *config.ArchivalDomainDefaults,
) ArchivalMetadata {
	historyConfig := NewArchivalConfig(
		historyStatus,
		dc.GetStringProperty(dynamicconfig.HistoryArchivalStatus, historyStatus),
		dc.GetBoolProperty(dynamicconfig.EnableReadFromHistoryArchival, historyReadEnabled),
		domainDefaults.History.Status,
		domainDefaults.History.URI,
	)

	visibilityConfig := NewArchivalConfig(
		visibilityStatus,
		dc.GetStringProperty(dynamicconfig.VisibilityArchivalStatus, visibilityStatus),
		dc.GetBoolProperty(dynamicconfig.EnableReadFromVisibilityArchival, visibilityReadEnabled),
		domainDefaults.Visibility.Status,
		domainDefaults.Visibility.URI,
	)

	return &archivalMetadata{
		historyConfig:    historyConfig,
		visibilityConfig: visibilityConfig,
	}
}

func (metadata *archivalMetadata) GetHistoryConfig() ArchivalConfig {
	return metadata.historyConfig
}

func (metadata *archivalMetadata) GetVisibilityConfig() ArchivalConfig {
	return metadata.visibilityConfig
}

// NewArchivalConfig constructs a new valid ArchivalConfig
func NewArchivalConfig(
	staticClusterStatusStr string,
	dynamicClusterStatus dynamicconfig.StringPropertyFn,
	enableRead dynamicconfig.BoolPropertyFn,
	domainDefaultStatusStr string,
	domainDefaultURI string,
) ArchivalConfig {
	staticClusterStatus, err := getClusterArchivalStatus(staticClusterStatusStr)
	if err != nil {
		panic(err)
	}
	domainDefaultStatus, err := getDomainArchivalStatus(domainDefaultStatusStr)
	if err != nil {
		panic(err)
	}

	return &archivalConfig{
		staticClusterStatus:  staticClusterStatus,
		dynamicClusterStatus: dynamicClusterStatus,
		enableRead:           enableRead,
		domainDefaultStatus:  domainDefaultStatus,
		domainDefaultURI:     domainDefaultURI,
	}
}

// NewDisabledArchvialConfig returns a disabled ArchivalConfig
func NewDisabledArchvialConfig() ArchivalConfig {
	return &archivalConfig{
		staticClusterStatus:  ArchivalDisabled,
		dynamicClusterStatus: nil,
		enableRead:           nil,
		domainDefaultStatus:  shared.ArchivalStatusDisabled,
		domainDefaultURI:     "",
	}
}

// ClusterConfiguredForArchival returns true if cluster is configured to handle archival, false otherwise
func (a *archivalConfig) ClusterConfiguredForArchival() bool {
	return a.GetClusterStatus() == ArchivalEnabled
}

func (a *archivalConfig) GetClusterStatus() ArchivalStatus {
	// Only check dynamic config when archival is enabled in static config.
	// If archival is disabled in static config, there will be no provider section in the static config
	// and the archiver provider can not create any archiver. Therefore, in that case,
	// even dynamic config says archival is enabled, we should ignore that.
	// Only when archival is enabled in static config, should we check if there's any difference between static config and dynamic config.
	if a.staticClusterStatus != ArchivalEnabled {
		return a.staticClusterStatus
	}

	dynamicStatusStr := a.dynamicClusterStatus()
	dynamicStatus, err := getClusterArchivalStatus(dynamicStatusStr)
	if err != nil {
		return ArchivalDisabled
	}
	return dynamicStatus
}

func (a *archivalConfig) ReadEnabled() bool {
	if !a.ClusterConfiguredForArchival() {
		return false
	}
	return a.enableRead()
}

func (a *archivalConfig) GetDomainDefaultStatus() shared.ArchivalStatus {
	return a.domainDefaultStatus
}

func (a *archivalConfig) GetDomainDefaultURI() string {
	return a.domainDefaultURI
}

func getClusterArchivalStatus(str string) (ArchivalStatus, error) {
	str = strings.TrimSpace(strings.ToLower(str))
	switch str {
	case "", common.ArchivalDisabled:
		return ArchivalDisabled, nil
	case common.ArchivalPaused:
		return ArchivalPaused, nil
	case common.ArchivalEnabled:
		return ArchivalEnabled, nil
	}
	return ArchivalDisabled, fmt.Errorf("invalid archival status of %v for cluster, valid status are: {\"\", \"disabled\", \"paused\", \"enabled\"}", str)
}

func getDomainArchivalStatus(str string) (shared.ArchivalStatus, error) {
	str = strings.TrimSpace(strings.ToLower(str))
	switch str {
	case "", common.ArchivalDisabled:
		return shared.ArchivalStatusDisabled, nil
	case common.ArchivalEnabled:
		return shared.ArchivalStatusEnabled, nil
	}
	return shared.ArchivalStatusDisabled, fmt.Errorf("invalid archival status of %v for domain, valid status are: {\"\", \"disabled\", \"enabled\"}", str)
}
