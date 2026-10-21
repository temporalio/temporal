//go:build temporal_minimal && !temporal_with_elasticsearch

package elasticsearch

import (
	"errors"
	"fmt"
	"os"
)

// RunTool fails, since Elasticsearch support is excluded from this build.
func RunTool([]string) error {
	err := errors.New("elasticsearch support is not compiled into this tool; rebuild with the temporal_with_elasticsearch build tag")
	_, _ = fmt.Fprintln(os.Stderr, err)
	return err
}
