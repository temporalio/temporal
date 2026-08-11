package junit

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRead(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    Testsuites
	}{
		{
			name: "testsuites root",
			content: `<testsuites tests="1">
    <testsuite name="suite" tests="1" failures="0" errors="0" id="0" time="">
        <testcase name="TestOne" classname="example.com/tests"></testcase>
    </testsuite>
</testsuites>`,
			want: Testsuites{
				XMLName: xml.Name{Local: "testsuites"},
				Tests:   1,
				Suites: []Testsuite{{
					Name:  "suite",
					Tests: 1,
					Testcases: []Testcase{{
						Name:      "TestOne",
						Classname: "example.com/tests",
					}},
				}},
			},
		},
		{
			name: "testsuite root with skipped testcase",
			content: `<testsuite name="suite" tests="1" failures="0" errors="0" skipped="1" id="0" time="">
	<testcase name="TestOne" classname="example.com/tests">
		<skipped></skipped>
	</testcase>
</testsuite>`,
			want: Testsuites{
				Tests:   1,
				Skipped: 1,
				Suites: []Testsuite{{
					Name:    "suite",
					Tests:   1,
					Skipped: 1,
					Testcases: []Testcase{{
						Name:      "TestOne",
						Classname: "example.com/tests",
						Skipped:   &Result{},
					}},
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "junit.xml")
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o644))

			report, err := Read(path)
			require.NoError(t, err)
			require.Equal(t, tt.want, *report)
		})
	}
}

func TestWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "junit.xml")
	report := &Testsuites{
		Tests: 1,
		Suites: []Testsuite{{
			Name:     "suite",
			Tests:    1,
			Failures: 0,
			Errors:   0,
			ID:       0,
			Time:     "",
			Testcases: []Testcase{{
				Name:      "TestOne",
				Classname: "example.com/tests",
			}},
		}},
	}

	require.NoError(t, Write(path, report))
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, `<testsuites tests="1">
    <testsuite name="suite" tests="1" failures="0" errors="0" id="0" time="">
        <testcase name="TestOne" classname="example.com/tests"></testcase>
    </testsuite>
</testsuites>`, string(content))
}
