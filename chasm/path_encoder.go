package chasm

import "strings"

var _ NodePathEncoder = (*defaultPathEncoder)(nil)

var DefaultPathEncoder NodePathEncoder = &defaultPathEncoder{}

type defaultPathEncoder struct{}

// TODO: Have a real implementation for DefaultPathEncoder
// that handles special characters in the path and support
// getting all immedidate children of a map node.

func (e *defaultPathEncoder) Encode(
	_ *Node,
	path []string,
) (string, error) {
	return strings.Join(path, "/"), nil
}

func (e *defaultPathEncoder) Decode(
	encodedPath string,
) ([]string, error) {
	if encodedPath == "" {
		return []string{}, nil
	}
	return strings.Split(encodedPath, "/"), nil
}
