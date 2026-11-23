package main

import (
	"encoding/json"
	"log"
	"os"
	"strings"

	"go.temporal.io/server/tools/draftapi"
	"golang.org/x/tools/go/analysis/unitchecker"
)

func main() {
	last := len(os.Args) - 1
	if last > 0 && strings.HasSuffix(os.Args[last], ".cfg") {
		config, err := enableAPIValidation(os.Args[last])
		if err != nil {
			log.Fatal(err)
		}
		os.Args[last] = config
	}
	unitchecker.Main(draftapi.Analyzer)
}

func enableAPIValidation(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var cfg unitchecker.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return "", err
	}
	if !cfg.VetxOnly || !strings.HasPrefix(cfg.ImportPath, "go.temporal.io/api/") {
		return path, nil
	}

	// Dependency-only vet runs discard diagnostics and Run errors.
	// Enable diagnostics for API dependencies so go vet reports validation at the
	// source while still receiving the dependency facts through its JSON protocol.
	cfg.VetxOnly = false
	data, err = json.Marshal(&cfg)
	if err != nil {
		return "", err
	}
	path = strings.TrimSuffix(path, ".cfg") + ".draftapi.cfg"
	return path, os.WriteFile(path, data, 0600)
}
