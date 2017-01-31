package util

type (
	// TaskTokenSerializer serializes task tokens
	TaskTokenSerializer interface {
		Serialize(token *TaskToken) ([]byte, error)
		Deserialize(data []byte) (*TaskToken, error)
	}

	// TaskToken identifies a task
	TaskToken struct {
		WorkflowID string `json:"workflowId"`
		RunID      string `json:"runId"`
		ScheduleID int64  `json:"scheduleId"`
	}
)
