//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_mock.go

package connector

import (
	"context"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	// Precondition is a function that allow you to filter a query result.
	// If subject match params conditions then return true, else return false.
	Precondition func(subject interface{}) bool

	// Client is a wrapper around Azure Blob Storage client library.
	Client interface {
		Upload(ctx context.Context, URI archiver.URI, fileName string, file []byte) error
		Get(ctx context.Context, URI archiver.URI, file string) ([]byte, error)
		Query(ctx context.Context, URI archiver.URI, fileNamePrefix string) ([]string, error)
		QueryWithFilters(ctx context.Context, URI archiver.URI, fileNamePrefix string, pageSize, offset int, filters []Precondition) ([]string, bool, int, error)
		Exist(ctx context.Context, URI archiver.URI, fileName string) (bool, error)
	}

	azblobClient struct {
		client *azblob.Client
		logger log.Logger
	}
)

// NewClient returns an Azblob Client based on config.
func NewClient(cfg *config.AzblobArchiver, logger log.Logger) (Client, error) {
	var client *azblob.Client
	var err error

	if cfg.UseSharedKey {
		cred, err := azblob.NewSharedKeyCredential(cfg.AccountName, cfg.AccountKey)
		if err != nil {
			logger.Error("Failed to create Azure Shared Key Credential", tag.Any("AccountName", cfg.AccountName), tag.Error(err))
			return nil, err
		}
		client, err = azblob.NewClientWithSharedKeyCredential(cfg.Endpoint, cred, nil)
	} else {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			logger.Error("Failed to create Azure Default Credential", tag.Error(err))
			return nil, err
		}
		client, err = azblob.NewClient(cfg.Endpoint, cred, nil)
	}

	if err != nil {
		logger.Error("Failed to create Azure Blob Storage client", tag.Error(err))
		return nil, err
	}

	logger.Info("Successfully created Azure Blob Storage client", tag.Any("AccountName", cfg.AccountName))
	return &azblobClient{
		client: client,
		logger: logger,
	}, nil
}

func (a *azblobClient) Upload(ctx context.Context, URI archiver.URI, fileName string, file []byte) error {
	containerName := URI.Hostname()
	blobName := formatBlobPath(URI.Path(), fileName)
	_, err := a.client.UploadBuffer(ctx, containerName, blobName, file, nil)
	return err
}

func (a *azblobClient) Get(ctx context.Context, URI archiver.URI, fileName string) ([]byte, error) {
	containerName := URI.Hostname()
	blobName := formatBlobPath(URI.Path(), fileName)

	resp, err := a.client.DownloadStream(ctx, containerName, blobName, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (a *azblobClient) Query(ctx context.Context, URI archiver.URI, fileNamePrefix string) ([]string, error) {
	containerName := URI.Hostname()
	prefix := formatBlobPath(URI.Path(), fileNamePrefix)

	var blobs []string
	pager := a.client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, blob := range resp.Segment.BlobItems {
			blobs = append(blobs, *blob.Name)
		}
	}
	return blobs, nil
}

// QueryWithFilters, retrieves filenames that match filter parameters. PageSize is optional, 0 means all records.
func (a *azblobClient) QueryWithFilters(ctx context.Context, URI archiver.URI, fileNamePrefix string, pageSize, offset int, filters []Precondition) ([]string, bool, int, error) {
	containerName := URI.Hostname()
	prefix := formatBlobPath(URI.Path(), fileNamePrefix)

	currentPos := offset
	resultSet := make([]string, 0)
	pager := a.client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, false, currentPos, err
		}
		for _, blob := range resp.Segment.BlobItems {
			if isPageCompleted(pageSize, len(resultSet)) {
				return resultSet, false, currentPos, nil
			}

			valid := true
			for _, f := range filters {
				if valid = f(*blob.Name); !valid {
					break
				}
			}

			if valid {
				if offset > 0 {
					offset--
					continue
				}
				resultSet = append(resultSet, *blob.Name)
				currentPos++
			}
		}
	}
	return resultSet, true, currentPos, nil
}

func isPageCompleted(pageSize, currentPosition int) bool {
	return pageSize != 0 && currentPosition > 0 && pageSize <= currentPosition
}

func (a *azblobClient) Exist(ctx context.Context, URI archiver.URI, fileName string) (bool, error) {
	containerName := URI.Hostname()
	if fileName == "" {
		// Check if container exists
		_, err := a.client.ServiceClient().NewContainerClient(containerName).GetProperties(ctx, nil)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	blobName := formatBlobPath(URI.Path(), fileName)
	_, err := a.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName).GetProperties(ctx, nil)
	if err != nil {
		return false, err
	}
	return true, nil
}

func formatBlobPath(path string, fileName string) string {
	if path == "" || path == "/" {
		return fileName
	}
	// Remove leading slash from path
	p := path
	if p[0] == '/' {
		p = p[1:]
	}
	if p[len(p)-1] == '/' {
		return p + fileName
	}
	return p + "/" + fileName
}
