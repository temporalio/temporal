# Amazon S3 blobstore

## Using localstack for local development
1. Install awscli from [here][https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html]
2. Install localstack from [here][https://github.com/localstack/localstack#installing]
3. Launch localstack with `SERVICES=s3 localstack start`
4. Create a bucket using `aws --endpoint-url=http://localhost:4572 s3 mb s3://cadence-development` 
5. Configure archival with the following configuration
```
archival:
  status: "enabled"
  enableReadFromArchival: true
  defaultBucket: "cadence-development"
  s3store:
    region: "us-east-1"
    endpoint: "http://127.0.0.1:4572"
    s3ForcePathStyle: true
```




