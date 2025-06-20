//go:generate mockgen -package "$GOPACKAGE" -destination s3api.go github.com/aws/aws-sdk-go/service/s3/s3iface S3API

package mocks
