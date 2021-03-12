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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination archivalMetadata_mock.go

package archiver

import (
	"fmt"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/config"

	"go.temporal.io/server/common/dynamicconfig"
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
		GetClusterState() ArchivalState
		ReadEnabled() bool
		GetNamespaceDefaultState() enumspb.ArchivalState
		GetNamespaceDefaultURI() string
	}

	archivalMetadata struct {
		historyConfig    ArchivalConfig
		visibilityConfig ArchivalConfig
	}

	archivalConfig struct {
		staticClusterState    ArchivalState
		dynamicClusterState   dynamicconfig.StringPropertyFn
		enableRead            dynamicconfig.BoolPropertyFn
		namespaceDefaultState enumspb.ArchivalState
		namespaceDefaultURI   string
	}

	// ArchivalState represents the archival state of the cluster
	ArchivalState int
)

const (
	// ArchivalDisabled means this cluster is not configured to handle archival
	ArchivalDisabled ArchivalState = iota
	// ArchivalPaused means this cluster is configured to handle archival but is currently not archiving
	// This state is not yet implemented, as of now ArchivalPaused is treated the same way as ArchivalDisabled
	ArchivalPaused
	// ArchivalEnabled means this cluster is currently archiving
	ArchivalEnabled
)

// NewArchivalMetadata constructs a new ArchivalMetadata
func NewArchivalMetadata(
	dc *dynamicconfig.Collection,
	historyState string,
	historyReadEnabled bool,
	visibilityState string,
	visibilityReadEnabled bool,
	namespaceDefaults *config.ArchivalNamespaceDefaults,
) ArchivalMetadata {
	historyConfig := NewArchivalConfig(
		historyState,
		dc.GetStringProperty(dynamicconfig.HistoryArchivalState, historyState),
		dc.GetBoolProperty(dynamicconfig.EnableReadFromHistoryArchival, historyReadEnabled),
		namespaceDefaults.History.State,
		namespaceDefaults.History.URI,
	)

	visibilityConfig := NewArchivalConfig(
		visibilityState,
		dc.GetStringProperty(dynamicconfig.VisibilityArchivalState, visibilityState),
		dc.GetBoolProperty(dynamicconfig.EnableReadFromVisibilityArchival, visibilityReadEnabled),
		namespaceDefaults.Visibility.State,
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
	staticClusterStateStr string,
	dynamicClusterState dynamicconfig.StringPropertyFn,
	enableRead dynamicconfig.BoolPropertyFn,
	namespaceDefaultStateStr string,
	namespaceDefaultURI string,
) ArchivalConfig {
	staticClusterState, err := getClusterArchivalState(staticClusterStateStr)
	if err != nil {
		panic(err)
	}
	namespaceDefaultState, err := getNamespaceArchivalState(namespaceDefaultStateStr)
	if err != nil {
		panic(err)
	}

	return &archivalConfig{
		staticClusterState:    staticClusterState,
		dynamicClusterState:   dynamicClusterState,
		enableRead:            enableRead,
		namespaceDefaultState: namespaceDefaultState,
		namespaceDefaultURI:   namespaceDefaultURI,
	}
}

// NewDisabledArchvialConfig returns a disabled ArchivalConfig
func NewDisabledArchvialConfig() ArchivalConfig {
	return &archivalConfig{
		staticClusterState:    ArchivalDisabled,
		dynamicClusterState:   nil,
		enableRead:            nil,
		namespaceDefaultState: enumspb.ARCHIVAL_STATE_DISABLED,
		namespaceDefaultURI:   "",
	}
}

// ClusterConfiguredForArchival returns true if cluster is configured to handle archival, false otherwise
func (a *archivalConfig) ClusterConfiguredForArchival() bool {
	return a.GetClusterState() == ArchivalEnabled
}

func (a *archivalConfig) GetClusterState() ArchivalState {
	// Only check dynamic config when archival is enabled in static config.
	// If archival is disabled in static config, there will be no provider section in the static config
	// and the archiver provider can not create any archiver. Therefore, in that case,
	// even dynamic config says archival is enabled, we should ignore that.
	// Only when archival is enabled in static config, should we check if there's any difference between static config and dynamic config.
	if a.staticClusterState != ArchivalEnabled {
		return a.staticClusterState
	}

	dynamicStateStr := a.dynamicClusterState()
	dynamicState, err := getClusterArchivalState(dynamicStateStr)
	if err != nil {
		return ArchivalDisabled
	}
	return dynamicState
}

func (a *archivalConfig) ReadEnabled() bool {
	if !a.ClusterConfiguredForArchival() {
		return false
	}
	return a.enableRead()
}

func (a *archivalConfig) GetNamespaceDefaultState() enumspb.ArchivalState {
	return a.namespaceDefaultState
}

func (a *archivalConfig) GetNamespaceDefaultURI() string {
	return a.namespaceDefaultURI
}

func getClusterArchivalState(str string) (ArchivalState, error) {
	str = strings.TrimSpace(strings.ToLower(str))
	switch str {
	case "", config.ArchivalDisabled:
		return ArchivalDisabled, nil
	case config.ArchivalPaused:
		return ArchivalPaused, nil
	case config.ArchivalEnabled:
		return ArchivalEnabled, nil
	}
	return ArchivalDisabled, fmt.Errorf("invalid archival state of %v for cluster, valid states are: {\"\", \"disabled\", \"paused\", \"enabled\"}", str)
}

func getNamespaceArchivalState(str string) (enumspb.ArchivalState, error) {
	str = strings.TrimSpace(strings.ToLower(str))
	switch str {
	case "", config.ArchivalDisabled:
		return enumspb.ARCHIVAL_STATE_DISABLED, nil
	case config.ArchivalEnabled:
		return enumspb.ARCHIVAL_STATE_ENABLED, nil
	}
	return enumspb.ARCHIVAL_STATE_DISABLED, fmt.Errorf("invalid archival state of %v for namespace, valid states are: {\"\", \"disabled\", \"enabled\"}", str)
}
