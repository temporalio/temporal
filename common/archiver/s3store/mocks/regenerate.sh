mockgen -copyright_file ../../../../LICENSE -package mocks -source $(go env GOPATH)/pkg/mod/github.com/aws/aws-sdk-go@v1.35.23/service/s3/s3iface/interface.go -destination S3API.go