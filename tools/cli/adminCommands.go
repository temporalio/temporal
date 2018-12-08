// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cli

import (
	"encoding/json"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/admin"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	cassp "github.com/uber/cadence/common/persistence/cassandra"
	"github.com/uber/cadence/tools/cassandra"
	"github.com/urfave/cli"
)

const maxEventID = 9999

// AdminShowWorkflow shows history
func AdminShowWorkflow(c *cli.Context) {
	domainID := c.String(FlagDomainID)
	wid := c.String(FlagWorkflowID)
	rid := c.String(FlagRunID)
	tid := c.String(FlagTreeID)
	bid := c.String(FlagBranchID)

	session := connectToCassandra(c)
	serializer := persistence.NewHistorySerializer()
	var history []*persistence.DataBlob
	if len(wid) != 0 {
		histV1 := cassp.NewHistoryPersistenceFromSession(session, bark.NewNopLogger())
		resp, err := histV1.GetWorkflowExecutionHistory(&persistence.InternalGetWorkflowExecutionHistoryRequest{
			LastEventBatchVersion: common.EmptyVersion,
			DomainID:              domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(wid),
				RunId:      common.StringPtr(rid),
			},
			FirstEventID: 1,
			NextEventID:  maxEventID,
			PageSize:     maxEventID,
		})
		if err != nil {
			ErrorAndExit("GetWorkflowExecutionHistory err", err)
		}

		history = resp.History

	} else if len(tid) != 0 {
		histV2 := cassp.NewHistoryV2PersistenceFromSession(session, bark.NewNopLogger())

		resp, err := histV2.ReadHistoryBranch(&persistence.InternalReadHistoryBranchRequest{
			TreeID:    tid,
			BranchID:  bid,
			MinNodeID: 1,
			MaxNodeID: maxEventID,
			PageSize:  maxEventID,
		})
		if err != nil {
			ErrorAndExit("ReadHistoryBranch err", err)
		}

		history = resp.History
	} else {
		ErrorAndExit("need to specify either WorkflowId/RunID for v1, or TreeID/BranchID for v2", nil)
	}

	if len(history) == 0 {
		ErrorAndExit("no events", nil)
	}
	totalSize := 0
	for idx, b := range history {
		totalSize += len(b.Data)
		fmt.Printf("======== batch %v, blob len: %v ======\n", idx+1, len(b.Data))
		historyBatch, err := serializer.DeserializeBatchEvents(b)
		if err != nil {
			ErrorAndExit("DeserializeBatchEvents err", err)
		}
		for _, e := range historyBatch {
			jsonstr, err := json.Marshal(e)
			if err != nil {
				ErrorAndExit("json.Marshal err", err)
			}
			fmt.Println(string(jsonstr))
		}
	}
	fmt.Printf("======== total batches %v, total blob len: %v ======\n", len(history), totalSize)
}

// AdminDescribeWorkflow describe a new workflow execution for admin
func AdminDescribeWorkflow(c *cli.Context) {
	adminClient := cFactory.ServerAdminClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	ctx, cancel := newContext()
	defer cancel()

	resp, err := adminClient.DescribeWorkflowExecution(ctx, &admin.DescribeWorkflowExecutionRequest{
		Domain: common.StringPtr(domain),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(rid),
		},
	})
	if err != nil {
		ErrorAndExit("Describe workflow execution failed", err)
	}

	prettyPrintJSONObject(resp)
}

// AdminDeleteWorkflow describe a new workflow execution for admin
func AdminDeleteWorkflow(c *cli.Context) {
	domainID := getRequiredOption(c, FlagDomainID)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := getRequiredOption(c, FlagRunID)
	if !c.IsSet(FlagShardID) {
		ErrorAndExit("shardID is required", nil)
	}
	shardID := c.Int(FlagShardID)

	session := connectToCassandra(c)

	var err error
	permanentRunID := "30000000-0000-f000-f000-000000000001"
	selectTmpl := "select execution from executions where shard_id = ? and type = 1 and domain_id = ? and workflow_id = ? and run_id = ? "
	deleteTmpl := "delete from executions where shard_id = ? and type = 1 and domain_id = ? and workflow_id = ? and run_id = ? "

	query := session.Query(selectTmpl, shardID, domainID, wid, permanentRunID)
	_, err = readOneRow(query)
	if err != nil {
		fmt.Printf("readOneRow for permanentRunID, %v, skip \n", err)
	} else {

		query := session.Query(deleteTmpl, shardID, domainID, wid, permanentRunID)
		err := query.Exec()
		if err != nil {
			ErrorAndExit("delete row failed", err)
		}
		fmt.Println("delete row successfully")
	}

	query = session.Query(selectTmpl, shardID, domainID, wid, rid)
	_, err = readOneRow(query)
	if err != nil {
		fmt.Printf("readOneRow for rid %v, %v, skip \n", rid, err)
	} else {

		query := session.Query(deleteTmpl, shardID, domainID, wid, rid)
		err := query.Exec()
		if err != nil {
			ErrorAndExit("delete row failed", err)
		}
		fmt.Println("delete row successfully")
	}
}

func readOneRow(query *gocql.Query) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	err := query.MapScan(result)
	return result, err
}

func connectToCassandra(c *cli.Context) *gocql.Session {
	host := getRequiredOption(c, FlagAddress)
	if !c.IsSet(FlagPort) {
		ErrorAndExit("port is required", nil)
	}
	port := c.Int(FlagPort)
	user := c.String(FlagUsername)
	pw := c.String(FlagPassword)
	ksp := getRequiredOption(c, FlagKeyspace)

	clusterCfg, err := cassandra.NewCassandraCluster(host, port, user, pw, ksp, 10)
	clusterCfg.SerialConsistency = gocql.LocalSerial
	clusterCfg.NumConns = 20
	if err != nil {
		ErrorAndExit("connect to Cassandra failed", err)
	}
	session, err := clusterCfg.CreateSession()
	if err != nil {
		ErrorAndExit("connect to Cassandra failed", err)
	}
	return session
}

// AdminGetDomainIDOrName map domain
func AdminGetDomainIDOrName(c *cli.Context) {
	domainID := c.String(FlagDomainID)
	domainName := c.String(FlagDomain)
	if len(domainID) == 0 && len(domainName) == 0 {
		ErrorAndExit("Need either domainName or domainID", nil)
	}

	session := connectToCassandra(c)

	if len(domainID) > 0 {
		tmpl := "select domain from domains where id = ? "
		query := session.Query(tmpl, domainID)
		res, err := readOneRow(query)
		if err != nil {
			ErrorAndExit("readOneRow", err)
		}
		domain := res["domain"].(map[string]interface{})
		domainName := domain["name"].(string)
		fmt.Printf("domainName for domainID %v is %v \n", domainID, domainName)
	} else {
		tmpl := "select domain from domains_by_name where name = ?"
		tmplV2 := "select domain from domains_by_name_v2 where domains_partition=0 and name = ?"

		query := session.Query(tmpl, domainName)
		res, err := readOneRow(query)
		if err != nil {
			fmt.Printf("v1 return error: %v , trying v2...\n", err)

			query := session.Query(tmplV2, domainName)
			res, err := readOneRow(query)
			if err != nil {
				ErrorAndExit("readOneRow for v2", err)
			}
			domain := res["domain"].(map[string]interface{})
			domainID := domain["id"].(gocql.UUID).String()
			fmt.Printf("domainID for domainName %v is %v \n", domainName, domainID)
		} else {
			domain := res["domain"].(map[string]interface{})
			domainID := domain["id"].(gocql.UUID).String()
			fmt.Printf("domainID for domainName %v is %v \n", domainName, domainID)
		}
	}
}

// AdminGetShardID get shardID
func AdminGetShardID(c *cli.Context) {
	wid := getRequiredOption(c, FlagWorkflowID)
	numberOfShards := c.Int(FlagNumberOfShards)

	if numberOfShards <= 0 {
		ErrorAndExit("numberOfShards is required", nil)
		return
	}
	shardID := common.WorkflowIDToHistoryShard(wid, numberOfShards)
	fmt.Printf("ShardID for workflowID: %v is %v \n", wid, shardID)
}

// AdminDescribeHistoryHost describes history host
func AdminDescribeHistoryHost(c *cli.Context) {
	adminClient := cFactory.ServerAdminClient(c)

	wid := c.String(FlagWorkflowID)
	sid := c.Int(FlagShardID)
	addr := c.String(FlagHistoryAddress)
	printFully := c.Bool(FlagPrintFullyDetail)

	if len(wid) <= 0 && !c.IsSet(FlagShardID) && len(addr) <= 0 {
		ErrorAndExit("at least one of them is required to provide to lookup host: workflowID, shardID and host address", nil)
		return
	}

	ctx, cancel := newContext()
	defer cancel()

	req := &shared.DescribeHistoryHostRequest{}
	if len(wid) > 0 {
		req.ExecutionForHost = &shared.WorkflowExecution{WorkflowId: common.StringPtr(wid)}
	}
	if c.IsSet(FlagShardID) {
		req.ShardIdForHost = common.Int32Ptr(int32(sid))
	}
	if len(addr) > 0 {
		req.HostAddress = common.StringPtr(addr)
	}

	resp, err := adminClient.DescribeHistoryHost(ctx, req)
	if err != nil {
		ErrorAndExit("Describe history host failed", err)
	}

	if !printFully {
		resp.ShardIDs = nil
	}
	prettyPrintJSONObject(resp)
}
