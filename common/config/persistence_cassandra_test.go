//go:build !temporal_minimal || temporal_with_cassandra

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataStoreValidate_Cassandra(t *testing.T) {
	ds := &DataStore{Cassandra: &Cassandra{Hosts: "127.0.0.1"}}
	require.NoError(t, ds.Validate())
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
			name: "bad default settings",
			settings: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{
					Consistency: "fake_value",
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
