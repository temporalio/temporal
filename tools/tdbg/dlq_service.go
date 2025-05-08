package tdbg

import (
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/tasks"
)

const (
	defaultPageSize = 1000
)

type (
	DLQService interface {
		ReadMessages(c *cli.Context) error
		PurgeMessages(c *cli.Context) error
		MergeMessages(c *cli.Context) error
		ListQueues(c *cli.Context) error
	}
	DLQServiceProvider struct {
		clientFactory        ClientFactory
		taskBlobEncoder      TaskBlobEncoder
		taskCategoryRegistry tasks.TaskCategoryRegistry
		writer               io.Writer
		prompterFactory      PrompterFactory
	}
	// noCloseWriter adapts an [io.Writer] with no cleanup logic to an [io.WriteCloser].
	noCloseWriter struct {
		io.Writer
	}
)

func NewDLQServiceProvider(
	clientFactory ClientFactory,
	taskBlobEncoder TaskBlobEncoder,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
	writer io.Writer,
	prompterFactory PrompterFactory,
) *DLQServiceProvider {
	return &DLQServiceProvider{
		clientFactory:        clientFactory,
		taskBlobEncoder:      taskBlobEncoder,
		taskCategoryRegistry: taskCategoryRegistry,
		writer:               writer,
		prompterFactory:      prompterFactory,
	}
}

// GetDLQService returns a DLQService based on FlagDLQVersion.
func (p *DLQServiceProvider) GetDLQService(
	c *cli.Context,
) (DLQService, error) {
	prompter := p.prompterFactory(c)
	version := c.String(FlagDLQVersion)
	if version == "v1" {
		return NewDLQV1Service(p.clientFactory, prompter, p.writer), nil
	}
	if version == "v2" {
		return getDLQV2Service(
			c,
			p.clientFactory,
			p.taskCategoryRegistry,
			p.writer,
			prompter,
			p.taskBlobEncoder,
		)
	}
	return nil, fmt.Errorf("unknown DLQ version: %v", version)
}

func getDLQV2Service(
	c *cli.Context,
	clientFactory ClientFactory,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
	writer io.Writer,
	prompter *Prompter,
	taskBlobEncoder TaskBlobEncoder,
) (DLQService, error) {
	dlqType := c.String(FlagDLQType)
	category, ok, err := getCategoryByID(c, taskCategoryRegistry, dlqType)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("unknown dlq category %v", dlqType)
	}
	targetCluster, service, err := getTargetCluster(c, clientFactory)
	if err != nil {
		return service, err
	}
	sourceCluster := c.String(FlagCluster)
	if len(sourceCluster) == 0 {
		if category == tasks.CategoryReplication {
			return nil, fmt.Errorf(
				"must provide source cluster, --%s, when managing the replication dlq", FlagCluster,
			)
		}
		sourceCluster = targetCluster
	}
	return NewDLQV2Service(
		category,
		sourceCluster,
		targetCluster,
		clientFactory,
		writer,
		prompter,
		taskBlobEncoder,
	), nil
}

func getTargetCluster(c *cli.Context, clientFactory ClientFactory) (string, DLQService, error) {
	targetCluster := c.String(FlagTargetCluster)
	if len(targetCluster) == 0 {
		client := clientFactory.AdminClient(c)
		cluster, err := client.DescribeCluster(c.Context, &adminservice.DescribeClusterRequest{})
		if err != nil {
			return "", nil, fmt.Errorf(
				"can't figure out current cluster name to set default value of --%s because DescribeCluster failed: %v",
				FlagTargetCluster,
				err,
			)
		}
		targetCluster = cluster.ClusterName
	}
	return targetCluster, nil, nil
}

func toQueueType(dlqType string) (enumsspb.DeadLetterQueueType, error) {
	switch dlqType {
	case "namespace":
		return enumsspb.DEAD_LETTER_QUEUE_TYPE_NAMESPACE, nil
	case "history":
		return enumsspb.DEAD_LETTER_QUEUE_TYPE_REPLICATION, nil
	default:
		return enumsspb.DEAD_LETTER_QUEUE_TYPE_UNSPECIFIED, fmt.Errorf("unsupported queue type %v", dlqType)
	}
}

func getOutputFile(outputFile string, writer io.Writer) (io.WriteCloser, error) {
	if len(outputFile) == 0 {
		return noCloseWriter{writer}, nil
	}
	f, err := os.Create(outputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %s", err)
	}
	return f, nil
}

func (n noCloseWriter) Close() error {
	return nil
}

// GetDLQJobService returns a DLQJobService.
func (p *DLQServiceProvider) GetDLQJobService() DLQJobService {
	return DLQJobService{
		clientFactory: p.clientFactory,
		writer:        p.writer,
	}
}
