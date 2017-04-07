package common

type (
	// TaskTokenSerializer serializes task tokens
	TaskTokenSerializer interface {
		Serialize(token *TaskToken) ([]byte, error)
		Deserialize(data []byte) (*TaskToken, error)
	}

	// TaskToken identifies a task
	TaskToken struct {
		DomainID   string `json:"domainId"`
		WorkflowID string `json:"workflowId"`
		RunID      string `json:"runId"`
		ScheduleID int64  `json:"scheduleId"`
	}
)
