package tasks

// DeleteWorkflowExecutionStage used by ContextImpl.DeleteWorkflowExecution to indicate progress stage.
type DeleteWorkflowExecutionStage byte

const (
	DeleteWorkflowExecutionStageNone DeleteWorkflowExecutionStage = 0
)

const (
	DeleteWorkflowExecutionStageVisibility DeleteWorkflowExecutionStage = 1 << iota
	DeleteWorkflowExecutionStageCurrent
	DeleteWorkflowExecutionStageMutableState
	DeleteWorkflowExecutionStageHistory
)

func (s *DeleteWorkflowExecutionStage) MarkProcessed(stage DeleteWorkflowExecutionStage) {
	if s == nil {
		return
	}
	*s |= stage
}
func (s *DeleteWorkflowExecutionStage) IsProcessed(stage DeleteWorkflowExecutionStage) bool {
	return s != nil && *s&stage == stage
}
