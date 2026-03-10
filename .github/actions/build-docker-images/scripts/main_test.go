package main

import (
	"testing"
)

func TestParseTemporalVersion(t *testing.T) {
	tests := []struct {
		name    string
		output  string
		want    string
		wantErr bool
	}{
		{
			name:   "server version",
			output: "temporal version 1.29.0",
			want:   "1.29.0",
		},
		{
			name:   "cli release version",
			output: "temporal version 1.6.0 (Server 1.30.0, UI 2.45.0)",
			want:   "1.6.0",
		},
		{
			name:   "cli dev version",
			output: "temporal version 0.0.0-DEV (Server 1.30.1, UI 2.45.3)",
			want:   "0.0.0-DEV",
		},
		{
			name:   "with trailing newline",
			output: "temporal version 1.6.0 (Server 1.30.0, UI 2.45.0)\n",
			want:   "1.6.0",
		},
		{
			name:   "with leading and trailing whitespace",
			output: "  \n temporal version 1.6.0 (Server 1.30.0, UI 2.45.0) \n ",
			want:   "1.6.0",
		},
		{
			name:   "extra spaces between version and number",
			output: "temporal version              1.6.1",
			want:   "1.6.1",
		},
		{
			name:    "empty output",
			output:  "",
			wantErr: true,
		},
		{
			name:    "unexpected format",
			output:  "something else entirely",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTemporalVersion(tt.output)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got version %q", got)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
