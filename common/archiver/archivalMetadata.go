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

package archiver

import (
	"fmt"
	"strings"

	enumspb "go.temporal.io/temporal-proto/enums/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/common/service/dynamicconfig"
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
		GetNamespaceDefaultStatus() enumspb.ArchivalStatus
		GetNamespaceDefaultURI() string
	}

	archivalMetadata struct {
		historyConfig    ArchivalConfig
		visibilityConfig ArchivalConfig
	}

	archivalConfig struct {
		staticClusterStatus    ArchivalStatus
		dynamicClusterStatus   dynamicconfig.StringPropertyFn
		enableRead             dynamicconfig.BoolPropertyFn
		namespaceDefaultStatus enumspb.ArchivalStatus
		namespaceDefaultURI    string
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
	namespaceDefaults *config.ArchivalNamespaceDefaults,
) ArchivalMetadata {
	historyConfig := NewArchivalConfig(
		historyStatus,
		dc.GetStringProperty(dynamicconfig.HistoryArchivalStatus, historyStatus),
		dc.GetBoolProperty(dynamicconfig.EnableReadFromHistoryArchival, historyReadEnabled),
		namespaceDefaults.History.Status,
		namespaceDefaults.History.URI,
	)

	visibilityConfig := NewArchivalConfig(
		visibilityStatus,
		dc.GetStringProperty(dynamicconfig.VisibilityArchivalStatus, visibilityStatus),
		dc.GetBoolProperty(dynamicconfig.EnableReadFromVisibilityArchival, visibilityReadEnabled),
		namespaceDefaults.Visibility.Status,
		namespaceDefaults.Visibility.URI,
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
	namespaceDefaultStatusStr string,
	namespaceDefaultURI string,
) ArchivalConfig {
	staticClusterStatus, err := getClusterArchivalStatus(staticClusterStatusStr)
	if err != nil {
		panic(err)
	}
	namespaceDefaultStatus, err := getNamespaceArchivalStatus(namespaceDefaultStatusStr)
	if err != nil {
		panic(err)
	}

	return &archivalConfig{
		staticClusterStatus:    staticClusterStatus,
		dynamicClusterStatus:   dynamicClusterStatus,
		enableRead:             enableRead,
		namespaceDefaultStatus: namespaceDefaultStatus,
		namespaceDefaultURI:    namespaceDefaultURI,
	}
}

// NewDisabledArchvialConfig returns a disabled ArchivalConfig
func NewDisabledArchvialConfig() ArchivalConfig {
	return &archivalConfig{
		staticClusterStatus:    ArchivalDisabled,
		dynamicClusterStatus:   nil,
		enableRead:             nil,
		namespaceDefaultStatus: enumspb.ARCHIVAL_STATUS_DISABLED,
		namespaceDefaultURI:    "",
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

func (a *archivalConfig) GetNamespaceDefaultStatus() enumspb.ArchivalStatus {
	return a.namespaceDefaultStatus
}

func (a *archivalConfig) GetNamespaceDefaultURI() string {
	return a.namespaceDefaultURI
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

func getNamespaceArchivalStatus(str string) (enumspb.ArchivalStatus, error) {
	str = strings.TrimSpace(strings.ToLower(str))
	switch str {
	case "", common.ArchivalDisabled:
		return enumspb.ARCHIVAL_STATUS_DISABLED, nil
	case common.ArchivalEnabled:
		return enumspb.ARCHIVAL_STATUS_ENABLED, nil
	}
	return enumspb.ARCHIVAL_STATUS_DISABLED, fmt.Errorf("invalid archival status of %v for namespace, valid status are: {\"\", \"disabled\", \"enabled\"}", str)
}
