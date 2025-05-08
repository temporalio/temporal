package archiver

import (
	"go.uber.org/mock/gomock"
)

// MetadataMock is an implementation of ArchivalMetadata that can be used for testing.
// It can be used as a mock, but it also provides default values, which is something that can't be done with
// *MockArchivalMetadata. This cuts down on the amount of boilerplate code needed to write tests.
type MetadataMock interface {
	ArchivalMetadata
	// EXPECT returns a MetadataMockRecorder which can be used to set expectations on the mock.
	EXPECT() MetadataMockRecorder
	// SetHistoryEnabledByDefault sets the default history archival config to be enabled.
	SetHistoryEnabledByDefault()
	// SetVisibilityEnabledByDefault sets the default visibility archival config to be enabled.
	SetVisibilityEnabledByDefault()
}

// NewMetadataMock returns a new MetadataMock which uses the provided controller to create a MockArchivalMetadata
// instance.
func NewMetadataMock(controller *gomock.Controller) MetadataMock {
	m := &metadataMock{
		MockArchivalMetadata:    NewMockArchivalMetadata(controller),
		defaultHistoryConfig:    NewDisabledArchvialConfig(),
		defaultVisibilityConfig: NewDisabledArchvialConfig(),
	}
	return m
}

// MetadataMockRecorder is a wrapper around a ArchivalMetadata mock recorder.
// It is used to determine whether any calls to EXPECT().GetHistoryConfig() or EXPECT().GetVisibilityConfig() were made.
// A call to EXPECT().GetSomeConfig() causes that default config to no longer be used.
type MetadataMockRecorder interface {
	GetHistoryConfig() *gomock.Call
	GetVisibilityConfig() *gomock.Call
}

type metadataMock struct {
	*MockArchivalMetadata
	defaultHistoryConfig    ArchivalConfig
	defaultVisibilityConfig ArchivalConfig
	historyOverwritten      bool
	visibilityOverwritten   bool
}

func (m *metadataMock) SetHistoryEnabledByDefault() {
	m.defaultHistoryConfig = NewEnabledArchivalConfig()
}

func (m *metadataMock) SetVisibilityEnabledByDefault() {
	m.defaultVisibilityConfig = NewEnabledArchivalConfig()
}

func (m *metadataMock) GetHistoryConfig() ArchivalConfig {
	if !m.historyOverwritten {
		return m.defaultHistoryConfig
	}
	return m.MockArchivalMetadata.GetHistoryConfig()
}

func (m *metadataMock) GetVisibilityConfig() ArchivalConfig {
	if !m.visibilityOverwritten {
		return m.defaultVisibilityConfig
	}
	return m.MockArchivalMetadata.GetVisibilityConfig()
}

func (m *metadataMock) EXPECT() MetadataMockRecorder {
	return metadataMockRecorder{m}
}

type metadataMockRecorder struct {
	*metadataMock
}

func (r metadataMockRecorder) GetHistoryConfig() *gomock.Call {
	r.metadataMock.historyOverwritten = true
	return r.MockArchivalMetadata.EXPECT().GetHistoryConfig()
}

func (r metadataMockRecorder) GetVisibilityConfig() *gomock.Call {
	r.metadataMock.visibilityOverwritten = true
	return r.MockArchivalMetadata.EXPECT().GetVisibilityConfig()
}
