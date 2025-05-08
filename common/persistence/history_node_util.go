package persistence

import (
	"fmt"
	"sort"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
)

type (
	historyNodeMetadata struct {
		branchInfo *persistencespb.HistoryBranch

		nodeID            int64
		transactionID     int64
		prevTransactionID int64
	}
)

func validateNodeChainAndTrim(
	tailNodeID int64,
	tailTransactionID int64,
	transactionIDToNode map[int64]historyNodeMetadata,
) ([]historyNodeMetadata, error) {

	nodeIDToNodes := indexNodeIDToNode(transactionIDToNode)

	nodeChain, err := reverselyLinkNode(
		tailNodeID,
		tailTransactionID,
		transactionIDToNode,
	)
	if err != nil {
		return nil, err
	}

	nodesToTrim := trimNodes(nodeIDToNodes, nodeChain)
	return nodesToTrim, nil
}

func indexNodeIDToNode(
	transactionIDToNode map[int64]historyNodeMetadata,
) map[int64][]historyNodeMetadata {
	// indexing node ID -> sorted nodeMetadata by transaction ID from high to low
	nodeIDToNodes := make(map[int64][]historyNodeMetadata)
	for _, node := range transactionIDToNode {
		nodeMetadata := nodeIDToNodes[node.nodeID]
		nodeMetadata = append(nodeMetadata, node)
		nodeIDToNodes[node.nodeID] = nodeMetadata
	}
	for nodeID := range nodeIDToNodes {
		nodes := nodeIDToNodes[nodeID]
		// reverse sort by transaction ID
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].transactionID > nodes[j].transactionID
		})
		nodeIDToNodes[nodeID] = nodes
	}
	return nodeIDToNodes
}

func reverselyLinkNode(
	tailNodeID int64,
	tailTransactionID int64,
	transactionIDToNode map[int64]historyNodeMetadata,
) ([]historyNodeMetadata, error) {
	// from tail node, trace back
	transactionID := tailTransactionID
	node, ok := transactionIDToNode[transactionID]
	// sanity check node ID <-> transaction ID being unique
	if !ok || node.nodeID != tailNodeID {
		return nil, serviceerror.NewInternal(
			fmt.Sprintf("unable to find or verify the tail history node, node ID: %v, transaction ID: %v",
				tailNodeID,
				tailTransactionID,
			),
		)
	}

	var nodes []historyNodeMetadata
	nodes = append(nodes, node)
	for node.nodeID > common.FirstEventID {
		if prevNode, ok := transactionIDToNode[node.prevTransactionID]; !ok {
			return nil, serviceerror.NewInternal(
				fmt.Sprintf("unable to back trace history node, node ID: %v, transaction ID: %v, prev transaction ID: %v",
					node.nodeID,
					node.transactionID,
					node.prevTransactionID,
				),
			)
		} else {
			node = prevNode
			nodes = append(nodes, node)
		}
	}

	// now node should be the first node
	// node.nodeID == common.FirstEventID
	// node.prevTransactionID == 0
	if node.nodeID != common.FirstEventID || node.prevTransactionID != 0 {
		return nil, serviceerror.NewInternal(
			fmt.Sprintf("unable to back trace history node, node ID: %v, transaction ID: %v, prev transaction ID: %v",
				node.nodeID,
				node.transactionID,
				node.prevTransactionID,
			),
		)
	}

	return nodes, nil
}

func trimNodes(
	nodeIDToNodes map[int64][]historyNodeMetadata,
	nodeChain []historyNodeMetadata,
) []historyNodeMetadata {
	var nodesToTrim []historyNodeMetadata
	// for each node on the chain, validate that the transaction ID being the largest
	for _, validNode := range nodeChain {
		for _, node := range nodeIDToNodes[validNode.nodeID] {
			if node.transactionID <= validNode.transactionID {
				break
			}
			nodesToTrim = append(nodesToTrim, node)
		}
	}
	return nodesToTrim
}
