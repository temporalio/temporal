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
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

func TestCassandraStoreConsistencyValidation(t *testing.T) {
	defaultSettings := &CassandraConsistencySettings{
		Consistency:       "LOCAL_QUORUM",
		SerialConsistency: "LOCAL_SERIAL",
	}

	overrideSettings := &CassandraConsistencySettings{
		Consistency:       "ALL",
		SerialConsistency: "SERIAL",
	}

	tests := []struct {
		name      string
		cassandra *Cassandra
		want      *CassandraStoreConsistency
		wantErr   bool
	}{
		{
			name:      "Consistency field completely unset",
			cassandra: &Cassandra{},
			want: &CassandraStoreConsistency{
				Default:           defaultSettings,
				Execution:         defaultSettings,
				History:           defaultSettings,
				Visibility:        defaultSettings,
				NamespaceMetadata: defaultSettings,
				ClusterMetadata:   defaultSettings,
				Shard:             defaultSettings,
				Task:              defaultSettings,
				Queue:             defaultSettings,
			},
			wantErr: false,
		},
		{
			name: "Consistency field partially set",
			cassandra: &Cassandra{
				Consistency: &CassandraStoreConsistency{
					Execution: &CassandraConsistencySettings{},
				},
			},
			want: &CassandraStoreConsistency{
				Default:           defaultSettings,
				Execution:         defaultSettings,
				History:           defaultSettings,
				Visibility:        defaultSettings,
				NamespaceMetadata: defaultSettings,
				ClusterMetadata:   defaultSettings,
				Shard:             defaultSettings,
				Task:              defaultSettings,
				Queue:             defaultSettings,
			},
			wantErr: false,
		},
		{
			name: "Override set on Queue store without explicit default",
			cassandra: &Cassandra{
				Consistency: &CassandraStoreConsistency{
					Queue: overrideSettings,
				},
			},
			want: &CassandraStoreConsistency{
				Default:           defaultSettings,
				Execution:         defaultSettings,
				History:           defaultSettings,
				Visibility:        defaultSettings,
				NamespaceMetadata: defaultSettings,
				ClusterMetadata:   defaultSettings,
				Shard:             defaultSettings,
				Task:              defaultSettings,
				Queue:             overrideSettings,
			},
			wantErr: false,
		},
		{
			name: "Override set on History store with explicit default",
			cassandra: &Cassandra{
				Consistency: &CassandraStoreConsistency{
					Default: defaultSettings,
					History: overrideSettings,
				},
			},
			want: &CassandraStoreConsistency{
				Default:           defaultSettings,
				Execution:         defaultSettings,
				History:           overrideSettings,
				Visibility:        defaultSettings,
				NamespaceMetadata: defaultSettings,
				ClusterMetadata:   defaultSettings,
				Shard:             defaultSettings,
				Task:              defaultSettings,
				Queue:             defaultSettings,
			},
			wantErr: false,
		},
		{
			name: "Override on default applies to all stores",
			cassandra: &Cassandra{
				Consistency: &CassandraStoreConsistency{
					Default: overrideSettings,
				},
			},
			want: &CassandraStoreConsistency{
				Default:           overrideSettings,
				Execution:         overrideSettings,
				History:           overrideSettings,
				Visibility:        overrideSettings,
				NamespaceMetadata: overrideSettings,
				ClusterMetadata:   overrideSettings,
				Shard:             overrideSettings,
				Task:              overrideSettings,
				Queue:             overrideSettings,
			},
			wantErr: false,
		},
		{
			name: "Bad default",
			cassandra: &Cassandra{
				Consistency: &CassandraStoreConsistency{
					Default: &CassandraConsistencySettings{
						Consistency:       "BAD_VALUE",
						SerialConsistency: "LOCAL_SERIAL",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Bad override",
			cassandra: &Cassandra{
				Consistency: &CassandraStoreConsistency{
					Queue: &CassandraConsistencySettings{
						Consistency:       "BAD_VALUE",
						SerialConsistency: "LOCAL_SERIAL",
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.cassandra
			if err := c.validate(); (err != nil) != tt.wantErr {
				t.Errorf("Cassandra.validate() error = %v, wantErr %v", err, tt.wantErr)
			} else if !tt.wantErr {
				assert.Equal(t, tt.want, c.Consistency)
			}
		})
	}
}

func TestCassandraConsistencySettingsValidation(t *testing.T) {
	tests := []struct {
		name     string
		settings *CassandraConsistencySettings
		wantErr  bool
	}{
		{
			name: "valid values",
			settings: &CassandraConsistencySettings{
				Consistency:       "local_quorum",
				SerialConsistency: "local_serial",
			},
			wantErr: false,
		},
		{
			name: "invalid consistency",
			settings: &CassandraConsistencySettings{
				Consistency:       "bad_value",
				SerialConsistency: "local_serial",
			},
			wantErr: true,
		},
		{
			name: "invalid serial consistency",
			settings: &CassandraConsistencySettings{
				Consistency:       "local_quorum",
				SerialConsistency: "bad_value",
			},
			wantErr: true,
		},
		{
			name: "both not set",
			settings: &CassandraConsistencySettings{
				Consistency:       "",
				SerialConsistency: "",
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

func TestCassandraConsistencySettingsGetters(t *testing.T) {
	tests := []struct {
		name                  string
		settings              *CassandraConsistencySettings
		wantConsistency       gocql.Consistency
		wantSerialConsistency gocql.SerialConsistency
	}{
		{
			name: "Tests getters for consistency settings",
			settings: &CassandraConsistencySettings{
				Consistency:       "LOCAL_ONE",
				SerialConsistency: "LOCAL_SERIAL",
			},
			wantConsistency:       gocql.LocalOne,
			wantSerialConsistency: gocql.LocalSerial,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantConsistency, tt.settings.GetConsistency())
			assert.Equal(t, tt.wantSerialConsistency, tt.settings.GetSerialConsistency())
		})
	}
}
