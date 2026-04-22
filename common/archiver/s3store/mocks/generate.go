//go:generate mockgen -package "$GOPACKAGE" -destination s3api.go -source ../s3iface.go S3API

package mocks
