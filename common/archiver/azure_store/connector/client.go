//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_mock.go

package connector

import (
	"context"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
)

type (
	// Client is a wrapper around Azure Blob Storage client library.
	Client interface {
		Upload(ctx context.Context, URI archiver.URI, fileName string, file []byte) error
		Get(ctx context.Context, URI archiver.URI, file string) ([]byte, error)
		Query(ctx context.Context, URI archiver.URI, fileNamePrefix string) ([]string, error)
		Exist(ctx context.Context, URI archiver.URI, fileName string) (bool, error)
	}

	azblobClient struct {
		client *azblob.Client
	}
)

// NewClient returns an Azblob Client based on config.
func NewClient(cfg *config.AzblobArchiver) (Client, error) {
	var client *azblob.Client
	var err error

	if cfg.UseSharedKey {
		cred, err := azblob.NewSharedKeyCredential(cfg.AccountName, cfg.AccountKey)
		if err != nil {
			return nil, err
		}
		client, err = azblob.NewClientWithSharedKeyCredential(cfg.Endpoint, cred, nil)
	} else {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, err
		}
		client, err = azblob.NewClient(cfg.Endpoint, cred, nil)
	}

	if err != nil {
		return nil, err
	}

	return &azblobClient{
		client: client,
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
