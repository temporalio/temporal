package tests

import (
	"os"
	"path/filepath"
	"strings"
)

func GetTemporalPackageDir() (string, error) {
	var err error
	temporalPackageDir := os.Getenv("TEMPORAL_ROOT")
	if temporalPackageDir == "" {
		temporalPackageDir, err = os.Getwd()
		if err != nil {
			panic(err)
		}
		temporalPackageDir = filepath.ToSlash(temporalPackageDir)
		temporalIndex := strings.LastIndex(temporalPackageDir, "/temporal/")
		if temporalIndex == -1 {
			panic("Unable to find repo path. Use env var TEMPORAL_ROOT or clone the repo into folder named 'temporal'")
		}
		temporalPackageDir = temporalPackageDir[:temporalIndex+len("/temporal/")]
		if err != nil {
			panic(err)
		}
	}
	return temporalPackageDir, err
}
