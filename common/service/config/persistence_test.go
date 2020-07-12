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

package config

import (
	"reflect"
	"testing"

	"github.com/gocql/gocql"
)

func TestCassandraStoreConsistency_GetConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     *CassandraStoreConsistency
		storeType StoreType
		want      gocql.Consistency
	}{
		{
			name:      "Nil Consistency Settings",
			input:     nil,
			storeType: HistoryStoreType,
			want:      gocql.LocalQuorum,
		},
		{
			name:      "Empty Consistency Settings",
			input:     &CassandraStoreConsistency{},
			storeType: ExecutionStoreType,
			want:      gocql.LocalQuorum,
		},
		{
			name: "Empty Default Settings",
			input: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{},
			},
			storeType: ShardStoreType,
			want:      gocql.LocalQuorum,
		},
		{
			name: "Default Override",
			input: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{
					Consistency: "All",
				},
			},
			storeType: TaskStoreType,
			want:      gocql.All,
		},
		{
			name: "Specific Store Override",
			input: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{
					Consistency: "All",
				},
				NamespaceMetadata: &CassandraConsistencySettings{
					Consistency: "LocaL_OnE",
				},
			},
			storeType: NamespaceMetadataStoreType,
			want:      gocql.LocalOne,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.input
			if got := c.GetConsistency(tt.storeType); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CassandraStoreConsistency.GetConsistency() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCassandraStoreConsistency_GetSerialConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     *CassandraStoreConsistency
		storeType StoreType
		want      gocql.SerialConsistency
	}{
		{
			name:      "Nil Consistency Settings",
			input:     nil,
			storeType: HistoryStoreType,
			want:      gocql.LocalSerial,
		},
		{
			name:      "Empty Consistency Settings",
			input:     &CassandraStoreConsistency{},
			storeType: ExecutionStoreType,
			want:      gocql.LocalSerial,
		},
		{
			name: "Empty Default Settings",
			input: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{},
			},
			storeType: ShardStoreType,
			want:      gocql.LocalSerial,
		},
		{
			name: "Default Override",
			input: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{
					SerialConsistency: "serial",
				},
			},
			storeType: TaskStoreType,
			want:      gocql.Serial,
		},
		{
			name: "Specific Store Override",
			input: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{
					SerialConsistency: "Serial",
				},
				NamespaceMetadata: &CassandraConsistencySettings{
					SerialConsistency: "LocaL_SeRiAl",
				},
			},
			storeType: NamespaceMetadataStoreType,
			want:      gocql.LocalSerial,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.input
			if got := c.GetSerialConsistency(tt.storeType); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CassandraStoreConsistency.GetSerialConsistency() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCassandraConsistencySettings_validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		settings *CassandraConsistencySettings
		wantErr  bool
	}{
		{
			name:     "nil settings",
			settings: nil,
			wantErr:  false,
		},
		{
			name: "empty fields",
			settings: &CassandraConsistencySettings{
				Consistency:       "",
				SerialConsistency: "",
			},
			wantErr: false,
		},
		{
			name: "happy path",
			settings: &CassandraConsistencySettings{
				Consistency:       "Local_Quorum",
				SerialConsistency: "lOcal_sErial",
			},
			wantErr: false,
		},
		{
			name: "bad consistency",
			settings: &CassandraConsistencySettings{
				Consistency:       "bad_value",
				SerialConsistency: "local_serial",
			},
			wantErr: true,
		},
		{
			name: "bad serial consistency",
			settings: &CassandraConsistencySettings{
				Consistency:       "local_quorum",
				SerialConsistency: "bad_value",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.settings
			if err := c.validate(); (err != nil) != tt.wantErr {
				t.Errorf("CassandraConsistencySettings.validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCassandraStoreConsistency_validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		settings *CassandraStoreConsistency
		wantErr  bool
	}{
		{
			name:     "nil settings",
			settings: nil,
			wantErr:  false,
		},
		{
			name:     "empty settings",
			settings: &CassandraStoreConsistency{},
			wantErr:  false,
		},
		{
			name: "empty default settings",
			settings: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{},
			},
			wantErr: false,
		},
		{
			name: "good default settings",
			settings: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{
					Consistency: "one",
				},
			},
			wantErr: false,
		},
		{
			name: "good override settings",
			settings: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{
					SerialConsistency: "serial",
				},
				Execution: &CassandraConsistencySettings{
					Consistency: "two",
				},
			},
			wantErr: false,
		},
		{
			name: "bad default settings",
			settings: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{
					Consistency: "fake_value",
				},
			},
			wantErr: true,
		},
		{
			name: "bad override settings",
			settings: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{
					Consistency: "all",
				},
				Visibility: &CassandraConsistencySettings{
					SerialConsistency: "bad_value",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.settings
			if err := c.validate(); (err != nil) != tt.wantErr {
				t.Errorf("CassandraStoreConsistency.validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCassandraStoreConsistency_getConsistencySettings(t *testing.T) {
	t.Parallel()

	overrideSettings := &CassandraConsistencySettings{
		Consistency:       "ALL",
		SerialConsistency: "SERIAL",
	}

	tests := []struct {
		name      string
		settings  *CassandraStoreConsistency
		storeType StoreType
		want      *CassandraConsistencySettings
	}{
		{
			name: "History Store override",
			settings: &CassandraStoreConsistency{
				History: overrideSettings,
			},
			storeType: HistoryStoreType,
			want:      overrideSettings,
		},
		{
			name: "Task Store override",
			settings: &CassandraStoreConsistency{
				Task: overrideSettings,
			},
			storeType: TaskStoreType,
			want:      overrideSettings,
		},
		{
			name: "Execution Store override",
			settings: &CassandraStoreConsistency{
				Execution: overrideSettings,
			},
			storeType: ExecutionStoreType,
			want:      overrideSettings,
		},
		{
			name: "Shard Store override",
			settings: &CassandraStoreConsistency{
				Shard: overrideSettings,
			},
			storeType: ShardStoreType,
			want:      overrideSettings,
		},
		{
			name: "Visibility Store override",
			settings: &CassandraStoreConsistency{
				Visibility: overrideSettings,
			},
			storeType: VisibilityStoreType,
			want:      overrideSettings,
		},
		{
			name: "Namespace Metadata Store override",
			settings: &CassandraStoreConsistency{
				NamespaceMetadata: overrideSettings,
			},
			storeType: NamespaceMetadataStoreType,
			want:      overrideSettings,
		},
		{
			name: "Cluster Metadata Store override",
			settings: &CassandraStoreConsistency{
				ClusterMetadata: overrideSettings,
			},
			storeType: ClusterMetadataStoreType,
			want:      overrideSettings,
		},
		{
			name: "Queue Store override",
			settings: &CassandraStoreConsistency{
				Queue: overrideSettings,
			},
			storeType: QueueStoreType,
			want:      overrideSettings,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.settings
			if got := c.getConsistencySettings(tt.storeType); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CassandraStoreConsistency.getConsistencySettings() = %v, want %v", got, tt.want)
			}
		})
	}
}
