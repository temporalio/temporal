package schema

import (
	"embed"
)

//go:embed *
var assets embed.FS

func Assets() *embed.FS {
	return &assets
}
