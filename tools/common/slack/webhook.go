package slack

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

var webhookHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
}

// Send posts the message to a Slack webhook.
func (m *Message) Send(webhookURL string) (retErr error) {
	if webhookURL == "" {
		return errors.New("webhook URL is empty")
	}
	if m == nil {
		return errors.New("slack message is nil")
	}

	payload, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal Slack message: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, webhookURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create Slack request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := webhookHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send Slack request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("failed to close Slack response: %w", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected Slack response status: %s", resp.Status)
	}
	return nil
}
