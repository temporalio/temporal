//go:build !experimental

package services

func get(string) (variant, bool) {
	return variant{}, false
}

func names() []string {
	return nil
}

func registryError() error {
	return nil
}
