package sql

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResolvePassword exercises the small helper introduced for #10028 that
// chooses between a literal --password and the contents of --password-file.
// readFile is injected so the test does not touch the real filesystem.
func TestResolvePassword(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		password   string
		file       string
		fileBytes  []byte
		fileErr    error
		want       string
		wantErrSub string // substring expected in error, "" if no error
	}{
		{
			name:     "neither flag set yields empty password",
			password: "",
			file:     "",
			want:     "",
		},
		{
			name:     "literal password passes through unchanged",
			password: "s3cret",
			file:     "",
			want:     "s3cret",
		},
		{
			name:      "file password trims one trailing newline",
			password:  "",
			file:      "/etc/secrets/sqlpw",
			fileBytes: []byte("s3cret\n"),
			want:      "s3cret",
		},
		{
			name:      "file password without trailing newline is unchanged",
			password:  "",
			file:      "/etc/secrets/sqlpw",
			fileBytes: []byte("s3cret"),
			want:      "s3cret",
		},
		{
			name:      "internal whitespace and other newlines are preserved (only one trailing \\n is trimmed)",
			password:  "",
			file:      "/etc/secrets/sqlpw",
			fileBytes: []byte("line1\nline2\n"),
			want:      "line1\nline2",
		},
		{
			name:       "both flags set is rejected to avoid silent precedence",
			password:   "literal",
			file:       "/etc/secrets/sqlpw",
			fileBytes:  []byte("ignored"),
			wantErrSub: "both --password and --password-file were provided",
		},
		{
			name:       "file read error is surfaced with path context",
			password:   "",
			file:       "/etc/secrets/missing",
			fileErr:    errors.New("open: no such file"),
			wantErrSub: "reading --password-file",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			read := func(path string) ([]byte, error) {
				assert.Equal(t, tc.file, path,
					"readFile should be called with the path passed to resolvePassword")
				if tc.fileErr != nil {
					return nil, tc.fileErr
				}
				return tc.fileBytes, nil
			}
			got, err := resolvePassword(tc.password, tc.file, read)
			if tc.wantErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSub)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
