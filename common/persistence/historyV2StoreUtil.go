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

package persistence

import (
	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/shared"
	"time"
)

/*

DeleteWorkflowExecutionHistoryV2 is used to delete workflow execution history from historyV2.
Histories in historyV2 are represented as a tree rather than a linear list of history events.
The tree based history structure introduces complexities for deletions. The following concepts
must be understood in order to understand how histories get deleted:
- Branch token is used to identify a single path from root to leaf on the history tree
- Creation of child branches from parent branches are not atomic, their creation can be in progress during this method
- When a child branch has been forked from the parent branch it will call CompleteForkBranch
- Due to failure cases it is possible the child branch started to fork but never called CompleteForkBranch
- When deleting a branch, only the section of the branch which is not relied upon by other branches can be safely deleted

Given the above understanding this method can now be explained as the following protocol
1. Attempt to delete branch identified by branch token. The persistence APIs which get invoked are smart enough
to only delete the section of branch identified by branch token which are not depended on by other branches.
2. If the delete was successful then return nil.
3. If delete failed then check if it failed due to a child branch fork creation being in progress (error type for
child branch fork in progress is ConditionFailedError).
4. If error was not caused by child branch fork in progress then return error.
5. Otherwise history will attempt to be deleted while a child branch fork is ongoing. In order to do this
it is checked if the child branch fork was created less than one minute ago. If it was created less than one minute ago
return retryable error type indicating that branch cannot be deleted currently but client can retry deletion. If child
branch fork has been in progress for longer than one minute then it is assumed the child branch fork will be successful
and only the section of history which is not common to the child being created will get deleted.

This protocol is safe in that it will never delete history which is relied upon. But it is not complete in the sense
that zombie history segments can remain under some rare failure cases. Consider the following sequence of events
1. Child branch fork started and is in progress.
2. Forking has been in progress for longer than one minute, therefore forking is assumed to be successful.
3. History section of parent branch is deleted. But section of parent which is common to child is not deleted.
4. Our assumption of child branch fork being successful is actually wrong and the child never successfully forked.
Under this rare case the section of parent history which was assumed to be common to child will be a zombie history section.

*/
func DeleteWorkflowExecutionHistoryV2(historyV2Mgr HistoryV2Manager, branchToken []byte, logger bark.Logger) error {
	err := historyV2Mgr.DeleteHistoryBranch(&DeleteHistoryBranchRequest{
		BranchToken: branchToken,
	})
	if err == nil {
		return nil
	}

	_, ok := err.(*ConditionFailedError)
	if !ok {
		return err
	}

	// we believe this is very rare case to see: DeleteHistoryBranch returns ConditionFailedError means there are some incomplete branches

	resp, err := historyV2Mgr.GetHistoryTree(&GetHistoryTreeRequest{
		BranchToken: branchToken,
	})
	if err != nil {
		return err
	}

	if len(resp.ForkingInProgressBranches) > 0 {
		logInfo := ""
		defer func() {
			logger.Warnf("seeing incomplete forking branches when deleting branch, details: %v", logInfo)
		}()
		for _, br := range resp.ForkingInProgressBranches {
			if time.Now().After(br.ForkTime.Add(time.Minute)) {
				logInfo += ";" + br.Info
				// this can be case of goroutine crash the API call doesn't call CompleteForkBranch() to clean up the new branch
				bt, err := NewHistoryBranchTokenFromAnother(br.BranchID, branchToken)
				if err != nil {
					return err
				}
				err = historyV2Mgr.CompleteForkBranch(&CompleteForkBranchRequest{
					// actually we don't know it is success or fail. but use true for safety
					// the worst case is we may leak some data that will never deleted
					Success:     true,
					BranchToken: bt,
				})
				if err != nil {
					return err
				}
			} else {
				// in case of the forking is in progress within a short time period
				return &shared.ServiceBusyError{
					Message: "waiting for forking to complete",
				}
			}
		}
	}
	err = historyV2Mgr.DeleteHistoryBranch(&DeleteHistoryBranchRequest{
		BranchToken: branchToken,
	})
	return err
}
