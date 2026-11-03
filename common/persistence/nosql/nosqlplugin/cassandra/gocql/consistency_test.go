package gocql

import (
	"reflect"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
)

func TestGetConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input *config.CassandraStoreConsistency
		want  gocql.Consistency
	}{
		{
			name:  "Nil Consistency Settings",
			input: nil,
			want:  gocql.LocalQuorum,
		},
		{
			name:  "Empty Consistency Settings",
			input: &config.CassandraStoreConsistency{},
			want:  gocql.LocalQuorum,
		},
		{
			name: "Empty Default Settings",
			input: &config.CassandraStoreConsistency{
				Default: &config.CassandraConsistencySettings{},
			},
			want: gocql.LocalQuorum,
		},
		{
			name: "Default Override",
			input: &config.CassandraStoreConsistency{
				Default: &config.CassandraConsistencySettings{
					Consistency: "All",
				},
			},
			want: gocql.All,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.input
			if got := getConsistency(c); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getConsistency() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetSerialConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input *config.CassandraStoreConsistency
		want  gocql.SerialConsistency
	}{
		{
			name:  "Nil Consistency Settings",
			input: nil,
			want:  gocql.LocalSerial,
		},
		{
			name:  "Empty Consistency Settings",
			input: &config.CassandraStoreConsistency{},
			want:  gocql.LocalSerial,
		},
		{
			name: "Empty Default Settings",
			input: &config.CassandraStoreConsistency{
				Default: &config.CassandraConsistencySettings{},
			},
			want: gocql.LocalSerial,
		},
		{
			name: "Default Override",
			input: &config.CassandraStoreConsistency{
				Default: &config.CassandraConsistencySettings{
					SerialConsistency: "serial",
				},
			},
			want: gocql.Serial,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.input
			got, err := getSerialConsistency(c)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestConfigAcceptsDriverConsistencies guards config's driver-free consistency validation against drifting from the driver.
func TestConfigAcceptsDriverConsistencies(t *testing.T) {
	t.Parallel()

	for _, c := range []gocql.Consistency{
		gocql.Any, gocql.One, gocql.Two, gocql.Three, gocql.Quorum, gocql.All, gocql.LocalQuorum, gocql.EachQuorum, gocql.LocalOne,
	} {
		ds := config.DataStore{Cassandra: &config.Cassandra{Hosts: "127.0.0.1", Consistency: &config.CassandraStoreConsistency{
			Default: &config.CassandraConsistencySettings{Consistency: c.String()},
		}}}
		require.NoError(t, ds.Validate(), c.String())
	}
	for _, s := range []gocql.SerialConsistency{gocql.Serial, gocql.LocalSerial} {
		ds := config.DataStore{Cassandra: &config.Cassandra{Hosts: "127.0.0.1", Consistency: &config.CassandraStoreConsistency{
			Default: &config.CassandraConsistencySettings{SerialConsistency: s.String()},
		}}}
		require.NoError(t, ds.Validate(), s.String())
	}
}
