#!/bin/sh

set -e

aws_sdk=$(go list -f '{{.Dir}}' github.com/aws/aws-sdk-go)
if [ -z "$aws_sdk" ]; then
  echo "Can't locate aws-sdk-go source code"
  exit 1
fi

mockgen -copyright_file ../../../../LICENSE -package "$GOPACKAGE" -source "${aws_sdk}/service/s3/s3iface/interface.go" | grep -v "^// Source: .*" > S3API.go
