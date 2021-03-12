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
