package config

import (
	"reflect"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/require"
)

func TestSQLValidate_MutualExclusivity(t *testing.T) {
	cfg := &SQL{
		Password: "static",
		PasswordCommand: &PasswordCommandConfig{
			Command: "echo",
			Args:    []string{"dynamic"},
		},
	}
	err := cfg.validate()
	require.ErrorContains(t, err, "mutually exclusive")
}

func TestSQLValidate_PasswordOnly(t *testing.T) {
	cfg := &SQL{Password: "static"}
	err := cfg.validate()
	require.NoError(t, err)
}

func TestSQLValidate_PasswordCommandOnly(t *testing.T) {
	cfg := &SQL{
		PasswordCommand: &PasswordCommandConfig{
			Command: "echo",
			Args:    []string{"dynamic"},
		},
	}
	err := cfg.validate()
	require.NoError(t, err)
}

func TestSQLValidate_PasswordCommandEmptyCommand(t *testing.T) {
	cfg := &SQL{
		PasswordCommand: &PasswordCommandConfig{},
	}
	err := cfg.validate()
	require.ErrorContains(t, err, "passwordCommand.command must not be empty")
}

func TestSQLResolvePassword_Static(t *testing.T) {
	cfg := &SQL{Password: "static-pass"}
	pw, err := cfg.ResolvePassword()
	require.NoError(t, err)
	require.Equal(t, "static-pass", pw)
}

func TestSQLResolvePassword_EmptyWhenNothingSet(t *testing.T) {
	cfg := &SQL{}
	pw, err := cfg.ResolvePassword()
	require.NoError(t, err)
	require.Empty(t, pw)
}

func TestSQLResolvePassword_Command(t *testing.T) {
	cfg := &SQL{
		PasswordCommand: &PasswordCommandConfig{
			Command: "echo",
			Args:    []string{"hello"},
		},
	}
	pw, err := cfg.ResolvePassword()
	require.NoError(t, err)
	require.Equal(t, "hello", pw)
}

func TestSQLResolvePassword_CommandTrimsTrailingNewline(t *testing.T) {
	cfg := &SQL{
		PasswordCommand: &PasswordCommandConfig{
			Command: "printf",
			Args:    []string{"hello\n\n"},
		},
	}
	pw, err := cfg.ResolvePassword()
	require.NoError(t, err)
	require.Equal(t, "hello", pw)
}

func TestSQLResolvePassword_CommandFailure(t *testing.T) {
	cfg := &SQL{
		PasswordCommand: &PasswordCommandConfig{
			Command: "false",
		},
	}
	_, err := cfg.ResolvePassword()
	require.ErrorContains(t, err, "passwordCommand")
}

func TestSQLResolvePassword_CommandTimeout(t *testing.T) {
	cfg := &SQL{
		PasswordCommand: &PasswordCommandConfig{
			Command: "sleep",
			Args:    []string{"10"},
			Timeout: 10 * time.Millisecond,
		},
	}
	start := time.Now()
	_, err := cfg.ResolvePassword()
	elapsed := time.Since(start)
	require.ErrorContains(t, err, "passwordCommand")
	require.Less(t, elapsed, 5*time.Second, "command should have been killed by timeout")
}

func TestCassandraStoreConsistency_GetConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input *CassandraStoreConsistency
		want  gocql.Consistency
	}{
		{
			name:  "Nil Consistency Settings",
			input: nil,
			want:  gocql.LocalQuorum,
		},
		{
			name:  "Empty Consistency Settings",
			input: &CassandraStoreConsistency{},
			want:  gocql.LocalQuorum,
		},
		{
			name: "Empty Default Settings",
			input: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{},
			},
			want: gocql.LocalQuorum,
		},
		{
			name: "Default Override",
			input: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{
					Consistency: "All",
				},
			},
			want: gocql.All,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.input
			if got := c.GetConsistency(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CassandraStoreConsistency.GetConsistency() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCassandraStoreConsistency_GetSerialConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input *CassandraStoreConsistency
		want  gocql.SerialConsistency
	}{
		{
			name:  "Nil Consistency Settings",
			input: nil,
			want:  gocql.LocalSerial,
		},
		{
			name:  "Empty Consistency Settings",
			input: &CassandraStoreConsistency{},
			want:  gocql.LocalSerial,
		},
		{
			name: "Empty Default Settings",
			input: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{},
			},
			want: gocql.LocalSerial,
		},
		{
			name: "Default Override",
			input: &CassandraStoreConsistency{
				Default: &CassandraConsistencySettings{
					SerialConsistency: "serial",
				},
			},
			want: gocql.Serial,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.input
			if got := c.GetSerialConsistency(); !reflect.DeepEqual(got, tt.want) {
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
